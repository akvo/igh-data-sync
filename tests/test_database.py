"""Tests for database operations."""

import pytest

from lib.sync.database import DatabaseManager


class TestDatabaseManager:
    """Test database manager operations."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        """Create temp database."""
        db_path = tmp_path / "test.db"
        self.db = DatabaseManager(str(db_path))
        yield
        self.db.close()

    def test_init_sync_tables(self):
        """Test sync tables creation."""
        self.db.init_sync_tables()
        assert self.db.table_exists("_sync_state") is True
        assert self.db.table_exists("_sync_log") is True

    def test_table_exists(self):
        """Test table existence check."""
        assert self.db.table_exists("nonexistent") is False

        self.db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY)")
        assert self.db.table_exists("test_table") is True

    def test_upsert_insert(self):
        """Test upsert creates new record."""
        self.db.execute("CREATE TABLE test (id TEXT PRIMARY KEY, name TEXT)")

        is_new = self.db.upsert("test", "id", {"id": "1", "name": "Alice"})
        assert is_new is True

        # Verify inserted
        cursor = self.db.conn.cursor()
        cursor.execute("SELECT name FROM test WHERE id = '1'")
        assert cursor.fetchone()[0] == "Alice"

    def test_upsert_update(self):
        """Test upsert updates existing record."""
        self.db.execute("CREATE TABLE test (id TEXT PRIMARY KEY, name TEXT)")
        self.db.execute("INSERT INTO test VALUES ('1', 'Alice')")

        is_new = self.db.upsert("test", "id", {"id": "1", "name": "Bob"})
        assert is_new is False

        # Verify updated
        cursor = self.db.conn.cursor()
        cursor.execute("SELECT name FROM test WHERE id = '1'")
        assert cursor.fetchone()[0] == "Bob"

    def test_get_last_sync_timestamp(self):
        """Test retrieving last sync timestamp."""
        self.db.init_sync_tables()

        # No timestamp yet
        assert self.db.get_last_sync_timestamp("test_entity") is None

        # Add timestamp
        self.db.update_sync_timestamp("test_entity", "2024-01-01T00:00:00", 10)

        timestamp = self.db.get_last_sync_timestamp("test_entity")
        assert timestamp == "2024-01-01T00:00:00"
