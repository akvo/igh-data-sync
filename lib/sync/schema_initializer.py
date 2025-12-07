"""Schema initialization using authoritative $metadata XML."""

import json
from pathlib import Path
from typing import Optional

from ..config import EntityConfig
from ..type_mapping import TableSchema
from ..validation.dataverse_schema import DataverseSchemaFetcher


def generate_create_table_sql(
    table_name: str,
    schema: TableSchema,
    special_columns: Optional[list[str]] = None,
) -> str:
    """
    Generate CREATE TABLE SQL from TableSchema.

    Args:
        table_name: Name for the table (plural, e.g., 'vin_candidates')
        schema: TableSchema from $metadata (singular entity name)
        special_columns: Additional columns to add (e.g., ['json_response', 'sync_time'])

    Returns:
        SQL CREATE TABLE statement
    """
    lines = [f"CREATE TABLE IF NOT EXISTS {table_name} ("]

    # Add columns from schema
    column_defs = []

    # Add surrogate primary key as FIRST column (for SCD2)
    column_defs.append("  row_id INTEGER PRIMARY KEY AUTOINCREMENT")

    for col in schema.columns:
        col_def = f"  {col.name} {col.db_type}"

        # Add NOT NULL if not nullable
        if not col.nullable:
            col_def += " NOT NULL"

        # NOTE: Primary key constraint removed for SCD2
        # Business key is now a regular indexed column

        column_defs.append(col_def)

    # Add special sync columns
    if special_columns:
        if "json_response" in special_columns:
            column_defs.append("  json_response TEXT NOT NULL")
        if "sync_time" in special_columns:
            column_defs.append("  sync_time TEXT NOT NULL")
        if "valid_from" in special_columns:
            column_defs.append("  valid_from TEXT")
        if "valid_to" in special_columns:
            column_defs.append("  valid_to TEXT")

    lines.append(",\n".join(column_defs))
    lines.append(");")

    return "\n".join(lines)


async def initialize_tables(_config, entities: list[EntityConfig], client, db_manager):
    """
    Create tables using authoritative $metadata schemas.

    Args:
        _config: Configuration object (unused, kept for API compatibility)
        entities: List of EntityConfig objects to initialize
        client: DataverseClient instance
        db_manager: DatabaseManager instance

    Raises:
        RuntimeError: If schema fetch or table creation fails
    """

    # STEP 1: Load option set config if exists
    config_path = Path("config/optionsets.json")
    option_set_fields_by_entity = {}

    if config_path.exists():
        print(f"Loading option set configuration from {config_path}...")
        with open(config_path, encoding="utf-8") as f:
            option_set_fields_by_entity = json.load(f)
        total_fields = sum(len(fields) for fields in option_set_fields_by_entity.values())
        num_entities = len(option_set_fields_by_entity)
        print(f"  ✓ Loaded config for {num_entities} entities, {total_fields} option set fields")
    else:
        print("⚠️  No option set config found - tables will use TEXT for option sets")
        print(f"   To fix: Run sync, then: python generate_optionset_config.py > {config_path}")

    # STEP 2: Fetch schemas from $metadata
    fetcher = DataverseSchemaFetcher(client, target_db="sqlite")

    # Get singular names for $metadata lookup
    singular_names = [e.name for e in entities]

    print(f"Fetching schemas for {len(entities)} entities from $metadata...")
    schemas = await fetcher.fetch_schemas_from_metadata(
        singular_names,
        option_set_fields_by_entity=option_set_fields_by_entity or None,
    )

    # Create tables
    for entity in entities:
        singular_name = entity.name  # vin_candidate
        plural_name = entity.api_name  # vin_candidates

        if singular_name not in schemas:
            print(f"⚠️  Skipping '{singular_name}' - not found in $metadata")
            continue

        schema = schemas[singular_name]

        # Check if table exists
        if db_manager.table_exists(plural_name):
            print(f"  Table '{plural_name}' already exists, skipping")
            continue

        print(f"Creating table '{plural_name}' with {len(schema.columns)} columns...")

        # Generate CREATE TABLE SQL
        create_sql = generate_create_table_sql(
            table_name=plural_name,
            schema=schema,
            special_columns=["json_response", "sync_time", "valid_from", "valid_to"],
        )

        # Execute CREATE TABLE
        db_manager.execute(create_sql)

        # Create indexes for timestamp columns
        if any(c.name == "modifiedon" for c in schema.columns):
            db_manager.create_index(plural_name, "modifiedon")

        if any(c.name == "createdon" for c in schema.columns):
            db_manager.create_index(plural_name, "createdon")

        # Create SCD2 indexes
        # Check if primary key actually exists in columns (some entities have mismatched pk names)
        column_names = [c.name for c in schema.columns]
        if schema.primary_key and schema.primary_key in column_names:
            # Index on business key for lookups
            db_manager.create_index(plural_name, schema.primary_key)

            # Composite index (business_key, valid_to) for efficient active record queries
            index_name = f"idx_{plural_name}_{schema.primary_key}_valid_to"
            # S608: SQL safe - table/column names from schema (not user input)
            sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {plural_name}({schema.primary_key}, valid_to)"
            db_manager.execute(sql)

        # Index on valid_to for time-travel queries
        db_manager.create_index(plural_name, "valid_to")

        print(f"✓ Table '{plural_name}' created successfully")

    print("✓ Schema initialization complete")
