"""
Schema validation functions for Dataverse sync.

Validates schema against Dataverse $metadata before syncing.
"""

from typing import Optional

from igh_data_sync.type_mapping import TableSchema
from igh_data_sync.validation.database_schema import DatabaseSchemaQuery
from igh_data_sync.validation.dataverse_schema import DataverseSchemaFetcher
from igh_data_sync.validation.schema_comparer import SchemaComparer

# System columns added by the sync tool (should be excluded from validation)
SYSTEM_COLUMNS = {"row_id", "json_response", "sync_time", "valid_from", "valid_to"}


def _filter_system_columns(
    schema: TableSchema, expected_pk: Optional[str] = None, singular_entity_name: Optional[str] = None
) -> TableSchema:
    """
    Filter out system columns from schema before comparison.

    Args:
        schema: TableSchema to filter
        expected_pk: Expected primary key from Dataverse (used when DB has row_id)
        singular_entity_name: Singular entity name (e.g., "systemuser" not "systemusers")

    Returns:
        TableSchema with system columns removed
    """
    filtered_columns = [col for col in schema.columns if col.name not in SYSTEM_COLUMNS]

    # If primary key is row_id (SCD2 surrogate key), use the expected business key
    # if it exists as a column. This allows comparison to succeed when SCD2 is enabled.
    if schema.primary_key in SYSTEM_COLUMNS:
        # Check if expected_pk exists in filtered columns
        if expected_pk and any(col.name == expected_pk for col in filtered_columns):
            filtered_pk = expected_pk  # Use business key for comparison
        # Dataverse metadata quirk: sometimes PK doesn't exist as a column property
        # (e.g., systemuser has PK=ownerid but only systemuserid exists as column)
        # In this case, look for the standard {entity}id pattern
        elif singular_entity_name:
            entity_id_col = f"{singular_entity_name}id"
            col_names = [col.name for col in filtered_columns]
            filtered_pk = entity_id_col if entity_id_col in col_names else None
        else:
            filtered_pk = None  # Can't determine business key
    else:
        filtered_pk = schema.primary_key

    return TableSchema(
        entity_name=schema.entity_name,
        columns=filtered_columns,
        primary_key=filtered_pk,
        foreign_keys=schema.foreign_keys,
    )


def _validate_entity_schema(entity, dv_schemas, db_schemas, comparer):
    """Validate schema for a single entity."""
    singular_name = entity.name
    plural_name = entity.api_name
    result = {"valid": False, "create": False, "differences": []}

    # Check if in Dataverse
    if singular_name not in dv_schemas:
        result["differences"].append({
            "entity": plural_name,
            "severity": "warning",
            "description": f"Entity '{singular_name}' in config but not in $metadata - skipping",
        })
        return result

    dv_schema = dv_schemas[singular_name]

    # Check if table exists
    if plural_name not in db_schemas:
        result["differences"].append({
            "entity": plural_name,
            "severity": "info",
            "description": "New entity - table will be created",
        })
        result["valid"] = True
        result["create"] = True
        return result

    # Compare schemas
    db_schema_filtered = _filter_system_columns(
        db_schemas[plural_name], expected_pk=dv_schema.primary_key, singular_entity_name=singular_name
    )

    # Handle Dataverse metadata quirk: phantom PK adjustment
    dv_schema_adjusted = _adjust_phantom_pk(dv_schema, db_schema_filtered, singular_name)

    entity_diffs = comparer.compare_all(
        {singular_name: dv_schema_adjusted},
        {singular_name: db_schema_filtered},
    )
    result["differences"].extend(
        {
            "entity": diff.entity,
            "severity": diff.severity,
            "description": diff.description,
            "details": diff.details,
        }
        for diff in entity_diffs
    )
    result["valid"] = True
    return result


def _adjust_phantom_pk(dv_schema, db_schema_filtered, singular_name):
    """Adjust DV schema when PK doesn't exist as column (phantom PK)."""
    if dv_schema.primary_key and not any(col.name == dv_schema.primary_key for col in dv_schema.columns):
        entity_id_col = f"{singular_name}id"
        if db_schema_filtered.primary_key == entity_id_col:
            return TableSchema(
                entity_name=dv_schema.entity_name,
                columns=dv_schema.columns,
                primary_key=entity_id_col,
                foreign_keys=dv_schema.foreign_keys,
                indexes=dv_schema.indexes,
            )
    return dv_schema


async def validate_schema_before_sync(config, entities, client, _db_manager, logger=None):
    """
    Validate schema against Dataverse $metadata.

    Args:
        config: Configuration object
        entities: List of EntityConfig objects to validate
        client: DataverseClient instance
        _db_manager: DatabaseManager instance (unused, kept for API compatibility)
        logger: Optional logger for output

    Returns: (valid_entities, entities_to_create, differences, validation_passed)
    """

    def _log(message):
        if logger:
            logger.info(message)
        else:
            print(message)

    _log("  Fetching schemas from Dataverse $metadata...")

    fetcher = DataverseSchemaFetcher(client, target_db="sqlite")
    dv_schemas = await fetcher.fetch_schemas_from_metadata([e.name for e in entities])

    db_query = DatabaseSchemaQuery(config, db_type="sqlite")
    db_schemas = db_query.query_all_schemas([e.api_name for e in entities])

    comparer = SchemaComparer(target_db="sqlite")
    differences = []
    valid_entities = []
    entities_to_create = []

    for entity in entities:
        result = _validate_entity_schema(entity, dv_schemas, db_schemas, comparer)
        differences.extend(result["differences"])
        if result["valid"]:
            valid_entities.append(entity)
        if result["create"]:
            entities_to_create.append(entity)

    validation_passed = _report_validation_results(differences, logger)
    return valid_entities, entities_to_create, differences, validation_passed


def _report_validation_results(differences, logger=None):
    """
    Print validation results.

    Args:
        differences: List of schema differences
        logger: Optional logger for output (if None, uses print)

    Returns:
        bool: True if validation passed (no errors), False if errors found
    """

    def _log(message):
        if logger:
            logger.info(message)
        else:
            print(message)

    errors = [d for d in differences if d["severity"] == "error"]
    warnings = [d for d in differences if d["severity"] == "warning"]
    infos = [d for d in differences if d["severity"] == "info"]

    if differences:
        _log("\n  Schema Validation Results:")
        _log(f"    Errors: {len(errors)}, Warnings: {len(warnings)}, Info: {len(infos)}\n")

        for diff in errors:
            _log(f"    ❌ ERROR [{diff['entity']}]: {diff['description']}")
        for diff in warnings:
            _log(f"    ⚠️  WARNING [{diff['entity']}]: {diff['description']}")
        for diff in infos:
            _log(f"    ℹ️  INFO [{diff['entity']}]: {diff['description']}")  # noqa: RUF001 - info emoji for user-facing output

    # Return False if errors (don't exit)
    if errors:
        _log(f"\n❌ SYNC ABORTED: {len(errors)} breaking schema change(s)")
        return False

    if warnings or infos:
        _log(f"\n  ✓ Validation passed with {len(warnings)} warning(s), {len(infos)} info")
    else:
        _log("\n  ✓ Validation passed (no changes)")

    return True
