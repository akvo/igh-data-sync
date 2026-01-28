"""Sync state tracking for Dataverse synchronization."""

from datetime import datetime, timezone
from typing import Optional

from .database import DatabaseManager


class SyncStateManager:
    """Manages sync state and logging."""

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    def set_state(self, entity_name: str, state: str):
        """
        Set sync state for entity.

        States: pending, in_progress, completed, failed
        """
        # Use INSERT OR IGNORE to create row if it doesn't exist, then UPDATE
        # This preserves last_timestamp and records_count when updating state
        self.db.execute(
            """
            INSERT OR IGNORE INTO _sync_state
            (entity_name, state, last_sync_time)
            VALUES (?, ?, ?)
        """,
            (entity_name, state, datetime.now(timezone.utc).isoformat()),
        )

        self.db.execute(
            """
            UPDATE _sync_state
            SET state = ?, last_sync_time = ?
            WHERE entity_name = ?
        """,
            (state, datetime.now(timezone.utc).isoformat(), entity_name),
        )

    def get_state(self, entity_name: str) -> Optional[str]:
        """Get current state for entity."""
        if not self.db.conn:
            self.db.connect()
        cursor = self.db.conn.cursor()
        cursor.execute("SELECT state FROM _sync_state WHERE entity_name = ?", (entity_name,))
        row = cursor.fetchone()
        return row[0] if row else None

    def start_sync(self, entity_name: str) -> int:
        """
        Start sync for entity. Returns log ID.
        """
        self.set_state(entity_name, "in_progress")
        cursor = self.db.execute(
            """
            INSERT INTO _sync_log
            (entity_name, start_time, status)
            VALUES (?, ?, 'in_progress')
        """,
            (entity_name, datetime.now(timezone.utc).isoformat()),
        )
        return cursor.lastrowid

    def complete_sync(self, log_id: int, entity_name: str, added: int, updated: int):
        """Complete sync successfully."""
        self.set_state(entity_name, "completed")
        self.db.execute(
            """
            UPDATE _sync_log
            SET end_time = ?,
                records_added = ?,
                records_updated = ?,
                status = 'completed'
            WHERE id = ?
        """,
            (datetime.now(timezone.utc).isoformat(), added, updated, log_id),
        )

    def fail_sync(self, log_id: int, entity_name: str, error: str):
        """Mark sync as failed."""
        self.set_state(entity_name, "failed")
        self.db.execute(
            """
            UPDATE _sync_log
            SET end_time = ?,
                status = 'failed',
                error_message = ?
            WHERE id = ?
        """,
            (datetime.now(timezone.utc).isoformat(), error, log_id),
        )
