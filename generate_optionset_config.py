#!/usr/bin/env python3
"""Generate option set configuration from synced database.

Usage:
    python generate_optionset_config.py > config/optionsets.json

This script analyzes the SQLite database to find all option set lookup tables
(_optionset_*) and maps them back to entity fields, generating a configuration
file for future syncs.
"""

import json
import sqlite3
import sys
from pathlib import Path


def extract_option_sets(db_path: str) -> dict[str, list[str]]:
    """
    Extract option set fields from database.

    Args:
        db_path: Path to SQLite database

    Returns:
        Dict mapping entity name to list of option set field names
    """
    # Load entity config to get correct singular names
    config_path = Path("entities_config.json")
    if not config_path.exists():
        print("❌ entities_config.json not found", file=sys.stderr)
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
            columns = [col[1] for col in cursor.fetchall()]

            if field_name in columns:
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
    db_path = "dataverse_complete.db"

    if not Path(db_path).exists():
        print(f"❌ Database not found: {db_path}", file=sys.stderr)
        print("   Run sync_dataverse.py first to create the database", file=sys.stderr)
        sys.exit(1)

    print("Analyzing database...", file=sys.stderr)
    option_sets = extract_option_sets(db_path)

    print(f"\n✓ Generated config for {len(option_sets)} entities", file=sys.stderr)
    total_fields = sum(len(fields) for fields in option_sets.values())
    print(f"  Total option set fields: {total_fields}", file=sys.stderr)
    print("\nSave output to config/optionsets.json, then re-sync from scratch:", file=sys.stderr)
    print("  mkdir -p config", file=sys.stderr)
    print("  python generate_optionset_config.py > config/optionsets.json", file=sys.stderr)
    print("  rm dataverse_complete.db", file=sys.stderr)
    print("  python sync_dataverse.py", file=sys.stderr)
    print("", file=sys.stderr)

    # Output JSON to stdout
    print(json.dumps(option_sets, indent=2))


if __name__ == "__main__":
    main()
