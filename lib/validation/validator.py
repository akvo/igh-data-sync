"""
Schema validation functions for Dataverse sync.

Validates schema against Dataverse $metadata before syncing.
"""

import sys

from lib.validation.database_schema import DatabaseSchemaQuery
from lib.validation.dataverse_schema import DataverseSchemaFetcher
from lib.validation.schema_comparer import SchemaComparer


async def validate_schema_before_sync(config, entities, client, _db_manager):
    """
    Validate schema against Dataverse $metadata.

    Args:
        config: Configuration object
        entities: List of EntityConfig objects to validate
        client: DataverseClient instance
        _db_manager: DatabaseManager instance (unused, kept for API compatibility)

    Returns: (valid_entities, entities_to_create, differences)
    """
    print("  Fetching schemas from Dataverse $metadata...")

    fetcher = DataverseSchemaFetcher(client, target_db="sqlite")
    dv_schemas = await fetcher.fetch_schemas_from_metadata([e.name for e in entities])

    # Query database schemas
    db_query = DatabaseSchemaQuery(config, db_type="sqlite")
    db_schemas = db_query.query_all_schemas([e.api_name for e in entities])

    # Compare schemas
    comparer = SchemaComparer(target_db="sqlite")
    differences = []
    valid_entities = []
    entities_to_create = []

    for entity in entities:
        singular_name = entity.name
        plural_name = entity.api_name

        # Check if in Dataverse
        if singular_name not in dv_schemas:
            differences.append(
                {
                    "entity": plural_name,
                    "severity": "warning",
                    "description": (
                        f"Entity '{singular_name}' in config but not in $metadata - skipping"
                    ),
                },
            )
            continue

        dv_schema = dv_schemas[singular_name]

        # Check if table exists
        if plural_name not in db_schemas:
            differences.append(
                {
                    "entity": plural_name,
                    "severity": "info",
                    "description": "New entity - table will be created",
                },
            )
            entities_to_create.append(entity)
            valid_entities.append(entity)
        else:
            # Compare schemas
            db_schema = db_schemas[plural_name]
            entity_diffs = comparer.compare_all(
                {singular_name: dv_schema},
                {plural_name: db_schema},
            )
            for diff in entity_diffs:
                differences.append(
                    {
                        "entity": diff.entity,
                        "severity": diff.severity,
                        "description": diff.description,
                        "details": diff.details,
                    },
                )
            valid_entities.append(entity)

    # Analyze and report
    _report_validation_results(differences)

    return valid_entities, entities_to_create, differences


def _report_validation_results(differences):
    """Print validation results and exit if errors found."""
    errors = [d for d in differences if d["severity"] == "error"]
    warnings = [d for d in differences if d["severity"] == "warning"]
    infos = [d for d in differences if d["severity"] == "info"]

    if differences:
        print("\n  Schema Validation Results:")
        print(f"    Errors: {len(errors)}, Warnings: {len(warnings)}, Info: {len(infos)}\n")

        for diff in errors:
            print(f"    ❌ ERROR [{diff['entity']}]: {diff['description']}")
        for diff in warnings:
            print(f"    ⚠️  WARNING [{diff['entity']}]: {diff['description']}")
        for diff in infos:
            print(f"    ℹ️  INFO [{diff['entity']}]: {diff['description']}")

    # Exit if errors
    if errors:
        print(f"\n❌ SYNC ABORTED: {len(errors)} breaking schema change(s)")
        sys.exit(1)

    if warnings or infos:
        print(f"\n  ✓ Validation passed with {len(warnings)} warning(s), {len(infos)} info")
    else:
        print("\n  ✓ Validation passed (no changes)")
