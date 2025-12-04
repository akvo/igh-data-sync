"""SQLite database operations for sync."""

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Optional

from ..type_mapping import TableSchema


class DatabaseManager:
    """Manages SQLite database operations for sync."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None

    def connect(self):
        """Establish database connection."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def execute(self, sql: str, params: Optional[tuple] = None) -> sqlite3.Cursor:
        """Execute SQL statement."""
        if not self.conn:
            self.connect()
        cursor = self.conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        self.conn.commit()
        return cursor

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists."""
        if not self.conn:
            self.connect()
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        return cursor.fetchone() is not None

    def create_index(self, table_name: str, column_name: str):
        """Create index on column if not exists."""
        index_name = f"idx_{table_name}_{column_name}"
        sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({column_name})"
        self.execute(sql)

    def init_sync_tables(self):
        """Create sync metadata tables."""
        # Sync state table
        self.execute("""
            CREATE TABLE IF NOT EXISTS _sync_state (
                entity_name TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                last_sync_time TEXT,
                last_timestamp TEXT,
                records_count INTEGER DEFAULT 0
            )
        """)

        # Sync log table
        self.execute("""
            CREATE TABLE IF NOT EXISTS _sync_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_name TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT,
                records_added INTEGER DEFAULT 0,
                records_updated INTEGER DEFAULT 0,
                status TEXT NOT NULL,
                error_message TEXT
            )
        """)

    def ensure_optionset_table(self, field_name: str) -> None:
        """
        Create option set lookup table if it doesn't exist.

        Args:
            field_name: Name of the option set field
        """
        table_name = f"_optionset_{field_name}"

        if self.table_exists(table_name):
            return

        # Create lookup table
        self.execute(f"""
            CREATE TABLE {table_name} (
                code INTEGER PRIMARY KEY,
                label TEXT NOT NULL,
                first_seen TEXT NOT NULL
            )
        """)

        print(f"  ✓ Created option set lookup table '{table_name}'")

    def ensure_junction_table(
        self, entity_name: str, field_name: str, entity_pk: str
    ) -> None:
        """
        Create junction table for multi-select option set if it doesn't exist.

        Args:
            entity_name: Name of the entity table
            field_name: Name of the multi-select field
            entity_pk: Primary key column of entity table
        """
        table_name = f"_junction_{entity_name}_{field_name}"

        if self.table_exists(table_name):
            return

        lookup_table = f"_optionset_{field_name}"

        # Create junction table with foreign keys
        self.execute(f"""
            CREATE TABLE {table_name} (
                entity_id TEXT NOT NULL,
                option_code INTEGER NOT NULL,
                PRIMARY KEY (entity_id, option_code),
                FOREIGN KEY (entity_id) REFERENCES {entity_name}({entity_pk}),
                FOREIGN KEY (option_code) REFERENCES {lookup_table}(code)
            )
        """)

        print(f"  ✓ Created junction table '{table_name}'")

    def upsert_option_set_value(self, field_name: str, code: int, label: str) -> None:
        """
        Insert or update an option set value in the lookup table.

        Args:
            field_name: Name of the option set field
            code: Option code (integer value)
            label: Display label for the option
        """
        if not self.conn:
            self.connect()

        # Ensure table exists
        self.ensure_optionset_table(field_name)

        table_name = f"_optionset_{field_name}"
        cursor = self.conn.cursor()

        # Check if code exists
        # S608: SQL safe - table_name internally generated from field names (not user input), values parameterized
        cursor.execute(f"SELECT label FROM {table_name} WHERE code = ?", (code,))  # noqa: S608
        existing = cursor.fetchone()

        if existing:
            # Update label if changed (keep original first_seen)
            if existing[0] != label:
                # S608: SQL safe - table_name internally generated from field names (not user input), values parameterized
                cursor.execute(
                    f"UPDATE {table_name} SET label = ? WHERE code = ?",  # noqa: S608
                    (label, code),
                )
        else:
            # Insert new option
            first_seen = datetime.now(timezone.utc).isoformat()
            # S608: SQL safe - table_name internally generated from field names (not user input), values parameterized
            cursor.execute(
                f"INSERT INTO {table_name} (code, label, first_seen) VALUES (?, ?, ?)",  # noqa: S608
                (code, label, first_seen),
            )

        self.conn.commit()

    def upsert_junction_record(
        self, entity_name: str, field_name: str, entity_id: str, option_code: int
    ) -> None:
        """
        Insert junction record for multi-select option set.

        Args:
            entity_name: Name of the entity table
            field_name: Name of the multi-select field
            entity_id: ID of the entity record
            option_code: Option code to link
        """
        if not self.conn:
            self.connect()

        table_name = f"_junction_{entity_name}_{field_name}"
        cursor = self.conn.cursor()

        # INSERT OR IGNORE (duplicate prevention)
        # S608: SQL safe - table_name internally generated from entity/field names (not user input), values parameterized
        cursor.execute(
            f"INSERT OR IGNORE INTO {table_name} (entity_id, option_code) VALUES (?, ?)",  # noqa: S608
            (entity_id, option_code),
        )

        self.conn.commit()

    def clear_junction_records(
        self, entity_name: str, field_name: str, entity_id: str
    ) -> None:
        """
        Clear all junction records for an entity before re-inserting.

        Args:
            entity_name: Name of the entity table
            field_name: Name of the multi-select field
            entity_id: ID of the entity record
        """
        if not self.conn:
            self.connect()

        table_name = f"_junction_{entity_name}_{field_name}"

        # Check if table exists first
        if not self.table_exists(table_name):
            return

        cursor = self.conn.cursor()
        # S608: SQL safe - table_name internally generated from entity/field names (not user input), values parameterized
        cursor.execute(f"DELETE FROM {table_name} WHERE entity_id = ?", (entity_id,))  # noqa: S608
        self.conn.commit()

    def populate_detected_option_sets(
        self,
        detected: dict,  # Dict[str, DetectedOptionSet]
        entity_name: str,
        entity_id: str,
        entity_pk: str,
    ) -> None:
        """
        Populate option set data from detected option sets.

        Args:
            detected: Dict of detected option sets from OptionSetDetector
            entity_name: Name of the entity
            entity_id: Primary key value of the entity
            entity_pk: Primary key column name
        """
        for field_name, option_set in detected.items():
            if option_set.is_multi_select:
                # Multi-select: Populate lookup and junction tables
                self.ensure_optionset_table(field_name)
                self.ensure_junction_table(entity_name, field_name, entity_pk)

                # Clear old junction records
                self.clear_junction_records(entity_name, field_name, entity_id)

                # Populate
                for code, label in option_set.codes_and_labels.items():
                    self.upsert_option_set_value(field_name, code, label)
                    self.upsert_junction_record(
                        entity_name, field_name, entity_id, code
                    )

            else:
                # Single-select: Just populate lookup table
                self.ensure_optionset_table(field_name)

                for code, label in option_set.codes_and_labels.items():
                    self.upsert_option_set_value(field_name, code, label)

    def upsert(self, table_name: str, primary_key: str, record: dict[str, Any]) -> bool:
        """
        Insert or replace record.

        Args:
            table_name: Table name
            primary_key: Primary key column name
            record: Dict of column values

        Returns:
            True if new record (inserted), False if updated
        """
        if not self.conn:
            self.connect()

        # Check if exists
        pk_value = record.get(primary_key)
        cursor = self.conn.cursor()
        # S608: SQL safe - table/column names from EntityConfig/TableSchema (not user input), values parameterized
        cursor.execute(f"SELECT 1 FROM {table_name} WHERE {primary_key} = ?", (pk_value,))  # noqa: S608
        is_new = cursor.fetchone() is None

        # Build INSERT OR REPLACE
        columns = list(record.keys())
        placeholders = ",".join(["?" for _ in columns])
        column_list = ",".join(columns)

        # S608: SQL safe - table/column names from EntityConfig/TableSchema (not user input), values parameterized
        sql = f"INSERT OR REPLACE INTO {table_name} ({column_list}) VALUES ({placeholders})"  # noqa: S608
        values = tuple(record[col] for col in columns)

        cursor.execute(sql, values)
        self.conn.commit()

        return is_new

    def upsert_batch(
        self,
        table_name: str,
        primary_key: str,
        schema: TableSchema,
        api_records: list[dict],
    ) -> tuple[int, int]:
        """
        Batch upsert records with option set detection and json_response storage.

        Args:
            table_name: Table name
            primary_key: Primary key column name
            schema: TableSchema for column mapping
            api_records: List of API response records

        Returns:
            Tuple of (records_added, records_updated)
        """
        # Import detector
        from .optionset_detector import OptionSetDetector

        detector = OptionSetDetector()
        added = 0
        updated = 0

        for api_record in api_records:
            # Get entity_id from api_record (primary key value)
            entity_id = api_record.get(primary_key)

            if not entity_id:
                continue

            # STEP 1: Detect option sets from this record
            detected_option_sets = detector.detect_from_record(api_record)

            # STEP 2: Populate option set data (lookup and junction tables)
            if detected_option_sets:
                self.populate_detected_option_sets(
                    detected_option_sets,
                    table_name,
                    entity_id,
                    primary_key,
                )

            # STEP 3: Map columns from schema for entity table
            record = {}
            for col in schema.columns:
                if col.name in api_record:
                    # Store raw value (INTEGER for option sets)
                    value = api_record[col.name]

                    # Handle multi-select: don't store in entity table
                    # (will be in junction table instead)
                    field_name = col.name
                    if field_name in detected_option_sets:
                        option_set = detected_option_sets[field_name]
                        if option_set.is_multi_select:
                            # Skip - multi-select stored in junction table only
                            continue

                    record[col.name] = value

            # Add special columns
            record["json_response"] = json.dumps(api_record)
            record["sync_time"] = datetime.now(timezone.utc).isoformat()
            record["valid_from"] = api_record.get("modifiedon")

            # STEP 4: Upsert entity record
            is_new = self.upsert(table_name, primary_key, record)
            if is_new:
                added += 1
            else:
                updated += 1

        return added, updated

    def get_last_sync_timestamp(self, entity_name: str) -> Optional[str]:
        """Get last sync timestamp for entity."""
        if not self.conn:
            self.connect()
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT last_timestamp FROM _sync_state WHERE entity_name = ?",
            (entity_name,),
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def update_sync_timestamp(self, entity_name: str, timestamp: str, count: int):
        """Update last sync timestamp."""
        self.execute(
            """
            INSERT OR REPLACE INTO _sync_state
            (entity_name, state, last_sync_time, last_timestamp, records_count)
            VALUES (?, 'completed', ?, ?, ?)
        """,
            (entity_name, datetime.now(timezone.utc).isoformat(), timestamp, count),
        )

    def query_distinct_values(self, table_name: str, column_name: str) -> set:
        """
        Query distinct non-null values from a column.

        Used for extracting foreign key values during filtered entity sync.

        Args:
            table_name: Table to query
            column_name: Column to extract values from

        Returns:
            Set of distinct values (empty set if table doesn't exist)
        """
        if not self.conn:
            self.connect()

        cursor = self.conn.cursor()

        # Check if table exists
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        if not cursor.fetchone():
            return set()

        # Query distinct values
        # S608: SQL safe - table/column names from EntityConfig/TableSchema (not user input), values parameterized
        cursor.execute(
            f"SELECT DISTINCT {column_name} FROM {table_name} WHERE {column_name} IS NOT NULL",  # noqa: S608
        )
        return {row[0] for row in cursor.fetchall()}
