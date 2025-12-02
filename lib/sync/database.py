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
        cursor.execute(f"SELECT 1 FROM {table_name} WHERE {primary_key} = ?", (pk_value,))
        is_new = cursor.fetchone() is None

        # Build INSERT OR REPLACE
        columns = list(record.keys())
        placeholders = ",".join(["?" for _ in columns])
        column_list = ",".join(columns)

        sql = f"INSERT OR REPLACE INTO {table_name} ({column_list}) VALUES ({placeholders})"
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
        Batch upsert records with json_response storage.

        Args:
            table_name: Table name
            primary_key: Primary key column name
            schema: TableSchema for column mapping
            api_records: List of API response records

        Returns:
            Tuple of (records_added, records_updated)
        """
        added = 0
        updated = 0

        for api_record in api_records:
            # Map columns from schema
            record = {}
            for col in schema.columns:
                if col.name in api_record:
                    record[col.name] = api_record[col.name]

            # Add special columns
            record["json_response"] = json.dumps(api_record)
            record["sync_time"] = datetime.now(timezone.utc).isoformat()
            record["valid_from"] = api_record.get("modifiedon")

            # Upsert
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
        cursor.execute(
            f"SELECT DISTINCT {column_name} FROM {table_name} WHERE {column_name} IS NOT NULL",
        )
        return {row[0] for row in cursor.fetchall()}
