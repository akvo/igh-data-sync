"""SCD2 upsert operations for SQLite database."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from ..optionset_detector import OptionSetDetector

if TYPE_CHECKING:
    import sqlite3

    from ...type_mapping import TableSchema
    from .manager import DatabaseManager, SCD2Result
    from .optionset_storage import OptionSetStorage


class SCD2Upserter:
    """Handles SCD2 upsert and batch operations."""

    def __init__(self, db_manager: DatabaseManager, optionset_storage: OptionSetStorage):
        self.db_manager = db_manager
        self.optionset_storage = optionset_storage

    @property
    def conn(self) -> sqlite3.Connection | None:
        return self.db_manager.conn

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
            self.db_manager.connect()

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
        # Import here to avoid circular import
        from .manager import SCD2Result  # noqa: PLC0415

        if not self.conn:
            self.db_manager.connect()

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
                self.optionset_storage.populate_detected_option_sets(
                    detected_option_sets,
                    table_name,
                    entity_id,
                    primary_key,
                    scd2_result=scd2_result,
                )

        return added, updated
