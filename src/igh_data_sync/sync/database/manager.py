"""SQLite database manager for sync operations."""

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from ...type_mapping import TableSchema
from .optionset_storage import OptionSetStorage
from .scd2_upsert import SCD2Upserter


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
        self._optionset_storage: Optional[OptionSetStorage] = None
        self._scd2_upserter: Optional[SCD2Upserter] = None

    @property
    def optionset(self) -> OptionSetStorage:
        """Get optionset storage helper (lazy initialization)."""
        if self._optionset_storage is None:
            self._optionset_storage = OptionSetStorage(self)
        return self._optionset_storage

    @property
    def scd2(self) -> SCD2Upserter:
        """Get SCD2 upserter helper (lazy initialization)."""
        if self._scd2_upserter is None:
            self._scd2_upserter = SCD2Upserter(self, self.optionset)
        return self._scd2_upserter

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

    # Delegation methods for backward compatibility

    def ensure_optionset_table(self, field_name: str) -> None:
        """Create option set lookup table if it doesn't exist."""
        self.optionset.ensure_optionset_table(field_name)

    def ensure_junction_table(self, entity_name: str, field_name: str, entity_pk: str) -> None:
        """Create junction table for multi-select option set with temporal tracking."""
        self.optionset.ensure_junction_table(entity_name, field_name, entity_pk)

    def upsert_option_set_value(self, field_name: str, code: int, label: str) -> None:
        """Insert or update an option set value in the lookup table."""
        self.optionset.upsert_option_set_value(field_name, code, label)

    def upsert_junction_record(self, entity_name: str, field_name: str, entity_id: str, option_code: int) -> None:
        """Insert junction record for multi-select option set."""
        self.optionset.upsert_junction_record(entity_name, field_name, entity_id, option_code)

    def clear_junction_records(self, entity_name: str, field_name: str, entity_id: str) -> None:
        """Clear all junction records for an entity before re-inserting."""
        self.optionset.clear_junction_records(entity_name, field_name, entity_id)

    def snapshot_junction_relationships(
        self,
        table_name: str,
        entity_id: str,
        option_codes: list[int],
        valid_from: str,
    ) -> None:
        """Create a temporal snapshot of junction relationships for SCD2 tracking."""
        self.optionset.snapshot_junction_relationships(table_name, entity_id, option_codes, valid_from)

    def populate_detected_option_sets(
        self,
        detected: dict,
        entity_name: str,
        entity_id: str,
        entity_pk: str,
        scd2_result: Optional[SCD2Result] = None,
    ) -> None:
        """Populate option set data from detected option sets."""
        self.optionset.populate_detected_option_sets(detected, entity_name, entity_id, entity_pk, scd2_result)

    def upsert(self, table_name: str, primary_key: str, record: dict[str, Any]) -> bool:
        """Insert or replace record."""
        return self.scd2.upsert(table_name, primary_key, record)

    def upsert_scd2(self, table_name: str, business_key: str, record: dict[str, Any]) -> SCD2Result:
        """Insert or update record using SCD2 logic."""
        return self.scd2.upsert_scd2(table_name, business_key, record)

    def upsert_batch(
        self,
        table_name: str,
        primary_key: str,
        schema: TableSchema,
        api_records: list[dict],
    ) -> tuple[int, int]:
        """Batch upsert records with option set detection and json_response storage."""
        return self.scd2.upsert_batch(table_name, primary_key, schema, api_records)
