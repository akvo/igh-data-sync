"""Option set storage operations for SQLite database."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import sqlite3

    from .manager import DatabaseManager, SCD2Result


class OptionSetStorage:
    """Handles option set and junction table operations."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    @property
    def conn(self) -> sqlite3.Connection | None:
        return self.db_manager.conn

    def ensure_optionset_table(self, field_name: str) -> None:
        """
        Create option set lookup table if it doesn't exist.

        Args:
            field_name: Name of the option set field
        """
        table_name = f"_optionset_{field_name}"

        if self.db_manager.table_exists(table_name):
            return

        # Create lookup table
        self.db_manager.execute(f"""
            CREATE TABLE {table_name} (
                code INTEGER PRIMARY KEY,
                label TEXT NOT NULL,
                first_seen TEXT NOT NULL
            )
        """)

        print(f"  \u2713 Created option set lookup table '{table_name}'")

    def ensure_junction_table(self, entity_name: str, field_name: str, entity_pk: str) -> None:
        """
        Create junction table for multi-select option set with temporal tracking.

        Args:
            entity_name: Name of the entity table
            field_name: Name of the multi-select field
            entity_pk: Primary key column of entity table
        """
        table_name = f"_junction_{entity_name}_{field_name}"

        if self.db_manager.table_exists(table_name):
            return

        lookup_table = f"_optionset_{field_name}"

        # Create junction table with temporal tracking (SCD2)
        self.db_manager.execute(f"""
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
        self.db_manager.create_index(table_name, "entity_id")

        # Composite index (entity_id, valid_to) for active record queries
        index_name = f"idx_{table_name}_entity_id_valid_to"
        # S608: SQL safe - table_name internally generated from entity/field names (not user input)
        sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}(entity_id, valid_to)"
        self.db_manager.execute(sql)

        # Index on valid_to for time-travel queries
        self.db_manager.create_index(table_name, "valid_to")

        print(f"  \u2713 Created junction table '{table_name}' with temporal tracking")

    def upsert_option_set_value(self, field_name: str, code: int, label: str) -> None:
        """
        Insert or update an option set value in the lookup table.

        Args:
            field_name: Name of the option set field
            code: Option code (integer value)
            label: Display label for the option
        """
        if not self.conn:
            self.db_manager.connect()

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
            self.db_manager.connect()

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
            self.db_manager.connect()

        table_name = f"_junction_{entity_name}_{field_name}"

        # Check if table exists first
        if not self.db_manager.table_exists(table_name):
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
            self.db_manager.connect()

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
        scd2_result: SCD2Result | None = None,
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
