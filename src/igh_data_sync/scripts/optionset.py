#!/usr/bin/env python3
"""Generate option set configuration from synced database.

Usage:
    python -m igh_data_sync.scripts.optionset [--db PATH] [--entities-config PATH] [--env-file PATH]

Arguments:
    --db PATH                 Path to SQLite database (default: dataverse_complete.db)
    --entities-config PATH    Path to entities config file (default: package data/entities_config.json)
    --env-file PATH          Path to .env file (for consistency)

This script analyzes the SQLite database to find all option set lookup tables
(_optionset_*) and maps them back to entity fields, generating a configuration
file for future syncs.

Examples:
    # Use default paths
    python -m igh_data_sync.scripts.optionset > config/optionsets.json

    # Use custom database path
    python -m igh_data_sync.scripts.optionset --db /path/to/my.db > config/optionsets.json

    # Use custom config path
    python -m igh_data_sync.scripts.optionset --entities-config /path/to/entities_config.json > config/optionsets.json
"""

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Optional

from igh_data_sync.config import get_default_config_path


def extract_option_sets(db_path: str, entities_config_path: Optional[str] = None) -> dict[str, list[str]]:
    """
    Extract option set fields from database.

    Args:
        db_path: Path to SQLite database
        entities_config_path: Optional path to entities configuration file.
                              If None, uses package default

    Returns:
        Dict mapping entity name to list of option set field names
    """
    # Load entity config to get correct singular names
    if entities_config_path is None:
        entities_config_path = get_default_config_path("entities_config.json")

    config_path = Path(entities_config_path)
    if not config_path.exists():
        print(f"❌ entities_config.json not found at {entities_config_path}", file=sys.stderr)
        sys.exit(1)

    with Path(config_path).open(encoding="utf-8") as f:
        entities_config = json.load(f)

    # Build mapping: plural table name → singular entity name
    table_to_entity = {e["api_name"]: e["name"] for e in entities_config["entities"]}

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Find all option set lookup tables
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name LIKE '_optionset_%'
        ORDER BY name
    """)

    optionset_tables = [row[0] for row in cursor.fetchall()]

    if not optionset_tables:
        print("⚠️  No option set tables found in database", file=sys.stderr)
        return {}

    print(f"Found {len(optionset_tables)} option set tables", file=sys.stderr)

    # Map option set fields to entities
    option_sets_by_entity = {}

    for table in optionset_tables:
        # Extract field name from table name (_optionset_<field_name>)
        field_name = table.replace("_optionset_", "")

        # Find which entity tables have this field
        # Filter out internal tables (_junction_, _optionset_, _sync_state) and SQLite tables
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table'
              AND substr(name, 1, 1) != '_'  -- Exclude tables starting with underscore
              AND name NOT LIKE 'sqlite_%'  -- Exclude SQLite tables
        """)

        entity_tables = [row[0] for row in cursor.fetchall()]

        for entity_table in entity_tables:
            # Check if this entity has the field
            cursor.execute(f"PRAGMA table_info({entity_table})")
            columns = {col[1]: col[2] for col in cursor.fetchall()}  # name -> type

            if field_name in columns:
                # IMPORTANT: Only include INTEGER fields (single-select option sets)
                # TEXT fields are multi-select option sets and must stay as TEXT
                column_type = columns[field_name]
                if column_type != "INTEGER":
                    print(f"  ⊘ {entity_table}.{field_name} (skipped: {column_type}, not INTEGER)", file=sys.stderr)
                    continue

                # Look up the correct singular entity name from config
                entity_name = table_to_entity.get(entity_table)

                if entity_name is None:
                    # Table not in config - skip it
                    continue

                if entity_name not in option_sets_by_entity:
                    option_sets_by_entity[entity_name] = []

                option_sets_by_entity[entity_name].append(field_name)
                print(f"  ✓ {entity_name}.{field_name}", file=sys.stderr)

    conn.close()

    # Sort fields for consistency
    for fields in option_sets_by_entity.values():
        fields.sort()

    return option_sets_by_entity


def main():
    parser = argparse.ArgumentParser(
        description="Generate option set configuration from synced database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use default paths
  python -m igh_data_sync.scripts.optionset > config/optionsets.json

  # Use custom database path
  python -m igh_data_sync.scripts.optionset --db /path/to/my.db > config/optionsets.json

  # Use custom config path
  python -m igh_data_sync.scripts.optionset --entities-config /path/to/entities_config.json > config/optionsets.json
        """,
    )
    parser.add_argument(
        "--db",
        default="dataverse_complete.db",
        help="Path to SQLite database (default: dataverse_complete.db)",
    )
    parser.add_argument(
        "--entities-config",
        default=None,
        help="Path to entities configuration file (default: package data/entities_config.json)",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Path to .env file (for consistency, though not used in this script)",
    )
    args = parser.parse_args()

    db_path = args.db

    if not Path(db_path).exists():
        print(f"❌ Database not found: {db_path}", file=sys.stderr)
        print("   Run sync_dataverse.py first to create the database", file=sys.stderr)
        sys.exit(1)

    print(f"Analyzing database: {db_path}", file=sys.stderr)
    option_sets = extract_option_sets(db_path, entities_config_path=args.entities_config)

    print(f"\n✓ Generated config for {len(option_sets)} entities", file=sys.stderr)
    total_fields = sum(len(fields) for fields in option_sets.values())
    print(f"  Total option set fields: {total_fields}", file=sys.stderr)
    print("\nSave output to config/optionsets.json, then re-sync from scratch:", file=sys.stderr)
    print("  mkdir -p config", file=sys.stderr)
    if db_path != "dataverse_complete.db":
        print(f"  python -m igh_data_sync.scripts.optionset --db {db_path} > config/optionsets.json", file=sys.stderr)
        print(f"  rm {db_path}", file=sys.stderr)
    else:
        print("  python -m igh_data_sync.scripts.optionset > config/optionsets.json", file=sys.stderr)
        print("  rm dataverse_complete.db", file=sys.stderr)
    print("  python -m igh_data_sync.scripts.sync", file=sys.stderr)
    print("", file=sys.stderr)

    # Output JSON to stdout
    print(json.dumps(option_sets, indent=2))


if __name__ == "__main__":
    main()
