"""SQLite database operations for sync."""

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from ..type_mapping import TableSchema
from .optionset_detector import OptionSetDetector


@dataclass
class SCD2Result:
    """Result of SCD2 upsert operation."""

    is_new_entity: bool  # True if entity never existed before
    version_created: bool  # True if new version was created (including first version)
    valid_from: str  # The valid_from timestamp of the current version
    business_key_value: str  # The business key value (e.g., accountid)


class DatabaseManager:  # noqa: PLR0904 - Complex data manager with many methods for different operations
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

    def __enter__(self):
        """Context manager entry - establish connection."""
        if not self.conn:
            self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close connection."""
        self.close()
        return False

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

    def ensure_junction_table(self, entity_name: str, field_name: str, entity_pk: str) -> None:
        """
        Create junction table for multi-select option set with temporal tracking.

        Args:
            entity_name: Name of the entity table
            field_name: Name of the multi-select field
            entity_pk: Primary key column of entity table
        """
        table_name = f"_junction_{entity_name}_{field_name}"

        if self.table_exists(table_name):
            return

        lookup_table = f"_optionset_{field_name}"

        # Create junction table with temporal tracking (SCD2)
        self.execute(f"""
            CREATE TABLE {table_name} (
                junction_id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_id TEXT NOT NULL,
                option_code INTEGER NOT NULL,
                valid_from TEXT NOT NULL,
                valid_to TEXT,
                FOREIGN KEY (entity_id) REFERENCES {entity_name}({entity_pk}),
                FOREIGN KEY (option_code) REFERENCES {lookup_table}(code)
            )
        """)

        # Create SCD2 indexes for efficient temporal queries
        self.create_index(table_name, "entity_id")

        # Composite index (entity_id, valid_to) for active record queries
        index_name = f"idx_{table_name}_entity_id_valid_to"
        # S608: SQL safe - table_name internally generated from entity/field names (not user input)
        sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}(entity_id, valid_to)"
        self.execute(sql)

        # Index on valid_to for time-travel queries
        self.create_index(table_name, "valid_to")

        print(f"  ✓ Created junction table '{table_name}' with temporal tracking")

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
        cursor.execute(f"SELECT label FROM {table_name} WHERE code = ?", (code,))  # noqa: S608 - table name from schema, values parameterized
        existing = cursor.fetchone()

        if existing:
            # Update label if changed (keep original first_seen)
            if existing[0] != label:
                cursor.execute(
                    f"UPDATE {table_name} SET label = ? WHERE code = ?",  # noqa: S608 - table name from schema, values parameterized
                    (label, code),
                )
        else:
            # Insert new option
            first_seen = datetime.now(timezone.utc).isoformat()
            cursor.execute(
                f"INSERT INTO {table_name} (code, label, first_seen) VALUES (?, ?, ?)",  # noqa: S608 - table name from schema, values parameterized
                (code, label, first_seen),
            )

        self.conn.commit()

    def upsert_junction_record(self, entity_name: str, field_name: str, entity_id: str, option_code: int) -> None:
        """
        Insert junction record for multi-select option set (backward compatibility).

        Note: This method is for backward compatibility when scd2_result is not available.
        New code should use snapshot_junction_relationships() via populate_detected_option_sets()
        with scd2_result parameter.

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

        # Use current timestamp for temporal columns (backward compatibility)
        current_time = datetime.now(timezone.utc).isoformat()

        # INSERT OR IGNORE (duplicate prevention)
        # S608: SQL safe - table_name internally generated

        # from entity/field names (not user input), values parameterized
        cursor.execute(
            f"INSERT OR IGNORE INTO {table_name} (entity_id, option_code, valid_from, valid_to) VALUES (?, ?, ?, NULL)",  # noqa: S608 - table/column names from schema, values parameterized
            (entity_id, option_code, current_time),
        )

        self.conn.commit()

    def clear_junction_records(self, entity_name: str, field_name: str, entity_id: str) -> None:
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
        # S608: SQL safe - table_name internally generated

        # from entity/field names (not user input), values parameterized
        cursor.execute(f"DELETE FROM {table_name} WHERE entity_id = ?", (entity_id,))  # noqa: S608 - table/column names from schema, values parameterized
        self.conn.commit()

    def snapshot_junction_relationships(
        self,
        table_name: str,
        entity_id: str,
        option_codes: list[int],
        valid_from: str,
    ) -> None:
        """
        Create a temporal snapshot of junction relationships for SCD2 tracking.

        This method creates a new version of junction relationships tied to the parent
        entity's version. Old relationships are closed (valid_to set) and new ones
        are inserted with valid_to = NULL.

        Args:
            table_name: Junction table name (e.g., '_junction_accounts_categories')
            entity_id: Entity ID value (e.g., account GUID)
            option_codes: List of option codes for current relationships
            valid_from: Timestamp for this snapshot (from parent entity's valid_from)
        """
        if not self.conn:
            self.connect()

        cursor = self.conn.cursor()

        # STEP 1: Close active junction records (set valid_to)
        # S608: SQL safe - table_name internally generated

        # from entity/field names (not user input), values parameterized
        cursor.execute(
            f"UPDATE {table_name} SET valid_to = ? WHERE entity_id = ? AND valid_to IS NULL",  # noqa: S608 - table/column names from schema, values parameterized
            (valid_from, entity_id),
        )

        # STEP 2: Insert new snapshot with valid_to = NULL
        if option_codes:
            for code in option_codes:
                # S608: SQL safe - table_name internally generated

                # (not user input), values parameterized
                cursor.execute(
                    f"INSERT INTO {table_name} (entity_id, option_code, valid_from, valid_to) VALUES (?, ?, ?, NULL)",  # noqa: S608 - table/column names from schema, values parameterized
                    (entity_id, code, valid_from),
                )

        self.conn.commit()

    def populate_detected_option_sets(
        self,
        detected: dict,  # Dict[str, DetectedOptionSet]
        entity_name: str,
        entity_id: str,
        entity_pk: str,
        scd2_result: Optional[SCD2Result] = None,
    ) -> None:
        """
        Populate option set data from detected option sets.

        Args:
            detected: Dict of detected option sets from OptionSetDetector
            entity_name: Name of the entity
            entity_id: Primary key value of the entity
            entity_pk: Primary key column name
            scd2_result: Optional SCD2Result from parent entity upsert (for temporal tracking)
        """
        for field_name, option_set in detected.items():
            if option_set.is_multi_select:
                # Multi-select: Populate lookup and junction tables
                self.ensure_optionset_table(field_name)
                self.ensure_junction_table(entity_name, field_name, entity_pk)

                # Populate option set lookup table
                for code, label in option_set.codes_and_labels.items():
                    self.upsert_option_set_value(field_name, code, label)

                # Handle junction records based on SCD2 mode
                if scd2_result is None:
                    # OLD APPROACH (backward compatibility): Clear and re-insert
                    self.clear_junction_records(entity_name, field_name, entity_id)
                    for code in option_set.codes_and_labels:
                        self.upsert_junction_record(entity_name, field_name, entity_id, code)
                # NEW APPROACH (SCD2): Snapshot only when parent version changes
                elif scd2_result.version_created:
                    table_name = f"_junction_{entity_name}_{field_name}"
                    option_codes = list(option_set.codes_and_labels.keys())
                    self.snapshot_junction_relationships(
                        table_name=table_name,
                        entity_id=entity_id,
                        option_codes=option_codes,
                        valid_from=scd2_result.valid_from,
                    )
                    # If version_created is False, skip junction update (no change in parent)

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
        # S608: SQL safe - table/column names from EntityConfig/TableSchema

        # (not user input), values parameterized
        cursor.execute(f"SELECT 1 FROM {table_name} WHERE {primary_key} = ?", (pk_value,))  # noqa: S608 - table/column names from schema, values parameterized
        is_new = cursor.fetchone() is None

        # Build INSERT OR REPLACE
        columns = list(record.keys())
        placeholders = ",".join(["?" for _ in columns])
        column_list = ",".join(columns)

        # S608: SQL safe - table/column names from EntityConfig/TableSchema

        # (not user input), values parameterized
        sql = f"INSERT OR REPLACE INTO {table_name} ({column_list}) VALUES ({placeholders})"  # noqa: S608 - table/column names from schema, values parameterized
        values = tuple(record[col] for col in columns)

        cursor.execute(sql, values)
        self.conn.commit()

        return is_new

    def upsert_scd2(self, table_name: str, business_key: str, record: dict[str, Any]) -> SCD2Result:
        """
        Insert or update record using SCD2 (Slowly Changing Dimension Type 2) logic.

        SCD2 Logic:
        1. Check if active record exists (business_key match AND valid_to IS NULL)
        2. If exists AND data changed:
            a. Close old record: UPDATE SET valid_to = new_record.valid_from
            b. Insert new record: INSERT with valid_to = NULL
            c. Return SCD2Result(False, True, ...) - version created
        3. If exists AND data NOT changed:
            a. Update sync_time only
            b. Return SCD2Result(False, False, ...) - no version
        4. If not exists:
            a. Insert with valid_to = NULL
            b. Return SCD2Result(True, True, ...) - new entity

        Args:
            table_name: Table name
            business_key: Business key column name (e.g., 'accountid')
            record: Dict of column values (including valid_from)

        Returns:
            SCD2Result with entity status and version information
        """
        if not self.conn:
            self.connect()

        cursor = self.conn.cursor()
        business_key_value = record.get(business_key)
        new_valid_from = record.get("valid_from")
        new_json_response = record.get("json_response")

        # STEP 1: Find active record (valid_to IS NULL)
        # S608: SQL safe - table/column names from EntityConfig/TableSchema

        # (not user input), values parameterized
        cursor.execute(
            f"SELECT row_id, json_response FROM {table_name} WHERE {business_key} = ? AND valid_to IS NULL",  # noqa: S608 - table/column names from schema, values parameterized
            (business_key_value,),
        )
        active_record = cursor.fetchone()

        # STEP 2: If no active record exists, INSERT new
        if active_record is None:
            columns = list(record.keys())
            # Ensure valid_to is NULL for new records
            columns.append("valid_to")
            values = list(record.values())
            values.append(None)

            placeholders = ",".join(["?" for _ in columns])
            column_list = ",".join(columns)

            # S608: SQL safe - table/column names from EntityConfig/TableSchema

            # (not user input), values parameterized
            sql = f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"  # noqa: S608 - table/column names from schema, values parameterized
            cursor.execute(sql, tuple(values))
            self.conn.commit()

            return SCD2Result(
                is_new_entity=True,
                version_created=True,
                valid_from=new_valid_from,
                business_key_value=business_key_value,
            )

        # STEP 3: Active record exists - check if data changed
        row_id = active_record[0]
        old_json_response = active_record[1]

        # Compare json_response to detect changes
        if old_json_response == new_json_response:
            # No change detected - optionally update sync_time
            cursor.execute(
                f"UPDATE {table_name} SET sync_time = ? WHERE row_id = ?",  # noqa: S608 - table/column names from schema, values parameterized
                (record.get("sync_time"), row_id),
            )
            self.conn.commit()
            return SCD2Result(
                is_new_entity=False,
                version_created=False,
                valid_from=new_valid_from,
                business_key_value=business_key_value,
            )

        # STEP 4: Data changed - close old record and insert new
        # Close old record by setting valid_to
        cursor.execute(
            f"UPDATE {table_name} SET valid_to = ? WHERE row_id = ?",  # noqa: S608 - table/column names from schema, values parameterized
            (new_valid_from, row_id),
        )

        # Insert new record with valid_to = NULL
        columns = list(record.keys())
        columns.append("valid_to")
        values = list(record.values())
        values.append(None)

        placeholders = ",".join(["?" for _ in columns])
        column_list = ",".join(columns)

        # S608: SQL safe - table/column names from EntityConfig/TableSchema

        # (not user input), values parameterized
        sql = f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"  # noqa: S608 - table/column names from schema, values parameterized
        cursor.execute(sql, tuple(values))
        self.conn.commit()

        return SCD2Result(
            is_new_entity=False,
            version_created=True,
            valid_from=new_valid_from,
            business_key_value=business_key_value,
        )

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

            # STEP 2: Map columns from schema for entity table
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
            # Remove OData metadata fields that change on every fetch (not actual data changes)
            api_record_clean = {k: v for k, v in api_record.items() if not k.startswith("@odata.")}
            record["json_response"] = json.dumps(api_record_clean, sort_keys=True)
            record["sync_time"] = datetime.now(timezone.utc).isoformat()
            record["valid_from"] = api_record.get("modifiedon") or datetime.now(timezone.utc).isoformat()

            # STEP 3: Upsert entity record using SCD2 logic
            scd2_result = self.upsert_scd2(table_name, primary_key, record)
            if scd2_result.is_new_entity:
                added += 1
            elif scd2_result.version_created:
                updated += 1
            # else: no change detected, sync_time updated only

            # STEP 4: Populate option set data (lookup and junction tables)
            # Pass scd2_result to enable temporal tracking of junction relationships
            if detected_option_sets:
                self.populate_detected_option_sets(
                    detected_option_sets,
                    table_name,
                    entity_id,
                    primary_key,
                    scd2_result=scd2_result,
                )

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
        # S608: SQL safe - table/column names from EntityConfig/TableSchema

        # (not user input), values parameterized
        cursor.execute(
            f"SELECT DISTINCT {column_name} FROM {table_name} WHERE {column_name} IS NOT NULL",  # noqa: S608 - table/column names from schema, values parameterized
        )
        return {row[0] for row in cursor.fetchall()}
