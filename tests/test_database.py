"""Tests for database operations."""
import unittest
import tempfile
import os
from lib.sync.database import DatabaseManager
from lib.type_mapping import TableSchema, ColumnMetadata


class TestDatabaseManager(unittest.TestCase):
    """Test database manager operations."""

    def setUp(self):
        """Create temp database."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db = DatabaseManager(self.temp_db.name)

    def tearDown(self):
        """Clean up temp database."""
        self.db.close()
        os.unlink(self.temp_db.name)

    def test_init_sync_tables(self):
        """Test sync tables creation."""
        self.db.init_sync_tables()
        self.assertTrue(self.db.table_exists('_sync_state'))
        self.assertTrue(self.db.table_exists('_sync_log'))

    def test_table_exists(self):
        """Test table existence check."""
        self.assertFalse(self.db.table_exists('nonexistent'))

        self.db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY)")
        self.assertTrue(self.db.table_exists('test_table'))

    def test_upsert_insert(self):
        """Test upsert creates new record."""
        self.db.execute("CREATE TABLE test (id TEXT PRIMARY KEY, name TEXT)")

        is_new = self.db.upsert('test', 'id', {'id': '1', 'name': 'Alice'})
        self.assertTrue(is_new)

        # Verify inserted
        cursor = self.db.conn.cursor()
        cursor.execute("SELECT name FROM test WHERE id = '1'")
        self.assertEqual(cursor.fetchone()[0], 'Alice')

    def test_upsert_update(self):
        """Test upsert updates existing record."""
        self.db.execute("CREATE TABLE test (id TEXT PRIMARY KEY, name TEXT)")
        self.db.execute("INSERT INTO test VALUES ('1', 'Alice')")

        is_new = self.db.upsert('test', 'id', {'id': '1', 'name': 'Bob'})
        self.assertFalse(is_new)

        # Verify updated
        cursor = self.db.conn.cursor()
        cursor.execute("SELECT name FROM test WHERE id = '1'")
        self.assertEqual(cursor.fetchone()[0], 'Bob')

    def test_get_last_sync_timestamp(self):
        """Test retrieving last sync timestamp."""
        self.db.init_sync_tables()

        # No timestamp yet
        self.assertIsNone(self.db.get_last_sync_timestamp('test_entity'))

        # Add timestamp
        self.db.update_sync_timestamp('test_entity', '2024-01-01T00:00:00', 10)

        timestamp = self.db.get_last_sync_timestamp('test_entity')
        self.assertEqual(timestamp, '2024-01-01T00:00:00')


if __name__ == '__main__':
    unittest.main()
