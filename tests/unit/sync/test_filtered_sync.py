"""Tests for filtered sync functionality."""

import pytest

from igh_data_sync.sync.database import DatabaseManager
from igh_data_sync.sync.filtered_sync import FilteredSyncManager
from igh_data_sync.sync.sync_state import SyncStateManager


class TestFilteredSyncManager:
    """Test FilteredSyncManager methods."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        """Create temp database and manager."""
        db_path = tmp_path / "test.db"
        self.db_manager = DatabaseManager(str(db_path))
        self.state_manager = SyncStateManager(self.db_manager)

        # Create a mock FilteredSyncManager (without client since we're testing DB operations)
        self.manager = FilteredSyncManager(
            client=None,  # Not needed for _separate_new_and_existing_ids
            db_manager=self.db_manager,
            state_manager=self.state_manager,
        )

        yield
        self.db_manager.close()

    def test_separate_new_and_existing_ids_no_timestamp(self):
        """Test that when last_timestamp is None, all IDs are considered new."""
        ids = {"id1", "id2", "id3"}

        new_ids, existing_ids = self.manager._separate_new_and_existing_ids(
            ids=ids,
            entity_api_name="test_entity",
            primary_key="test_id",
            last_timestamp=None,
        )

        assert new_ids == ids
        assert existing_ids == set()

    def test_separate_new_and_existing_ids_all_new(self):
        """Test when no IDs exist in database."""
        # Create empty table
        self.db_manager.execute("CREATE TABLE test_entity (test_id TEXT PRIMARY KEY, name TEXT)")

        ids = {"id1", "id2", "id3"}

        new_ids, existing_ids = self.manager._separate_new_and_existing_ids(
            ids=ids,
            entity_api_name="test_entity",
            primary_key="test_id",
            last_timestamp="2024-01-01T00:00:00Z",
        )

        assert new_ids == ids
        assert existing_ids == set()

    def test_separate_new_and_existing_ids_all_existing(self):
        """Test when all IDs exist in database."""
        # Create table with records
        self.db_manager.execute("CREATE TABLE test_entity (test_id TEXT PRIMARY KEY, name TEXT)")
        self.db_manager.execute("INSERT INTO test_entity VALUES ('id1', 'Name1')")
        self.db_manager.execute("INSERT INTO test_entity VALUES ('id2', 'Name2')")
        self.db_manager.execute("INSERT INTO test_entity VALUES ('id3', 'Name3')")

        ids = {"id1", "id2", "id3"}

        new_ids, existing_ids = self.manager._separate_new_and_existing_ids(
            ids=ids,
            entity_api_name="test_entity",
            primary_key="test_id",
            last_timestamp="2024-01-01T00:00:00Z",
        )

        assert new_ids == set()
        assert existing_ids == ids

    def test_separate_new_and_existing_ids_mixed(self):
        """Test when some IDs exist and some are new."""
        # Create table with some records
        self.db_manager.execute("CREATE TABLE test_entity (test_id TEXT PRIMARY KEY, name TEXT)")
        self.db_manager.execute("INSERT INTO test_entity VALUES ('id1', 'Name1')")
        self.db_manager.execute("INSERT INTO test_entity VALUES ('id3', 'Name3')")

        ids = {"id1", "id2", "id3", "id4"}

        new_ids, existing_ids = self.manager._separate_new_and_existing_ids(
            ids=ids,
            entity_api_name="test_entity",
            primary_key="test_id",
            last_timestamp="2024-01-01T00:00:00Z",
        )

        assert new_ids == {"id2", "id4"}
        assert existing_ids == {"id1", "id3"}

    def test_separate_new_and_existing_ids_empty_set(self):
        """Test with empty ID set."""
        self.db_manager.execute("CREATE TABLE test_entity (test_id TEXT PRIMARY KEY, name TEXT)")

        new_ids, existing_ids = self.manager._separate_new_and_existing_ids(
            ids=set(),
            entity_api_name="test_entity",
            primary_key="test_id",
            last_timestamp="2024-01-01T00:00:00Z",
        )

        assert new_ids == set()
        assert existing_ids == set()

    def test_separate_new_and_existing_ids_large_set(self):
        """Test with large ID set (>999 to verify batching works)."""
        # Create table
        self.db_manager.execute("CREATE TABLE test_entity (test_id TEXT PRIMARY KEY, name TEXT)")

        # Insert 1500 existing records
        cursor = self.db_manager.conn.cursor()
        for i in range(1500):
            cursor.execute("INSERT INTO test_entity VALUES (?, ?)", (f"id{i}", f"Name{i}"))
        self.db_manager.conn.commit()

        # Test with 2000 IDs: 1500 existing + 500 new
        all_ids = {f"id{i}" for i in range(2000)}

        new_ids, existing_ids = self.manager._separate_new_and_existing_ids(
            ids=all_ids,
            entity_api_name="test_entity",
            primary_key="test_id",
            last_timestamp="2024-01-01T00:00:00Z",
        )

        assert len(new_ids) == 500
        assert len(existing_ids) == 1500
        assert new_ids == {f"id{i}" for i in range(1500, 2000)}
        assert existing_ids == {f"id{i}" for i in range(1500)}

    def test_separate_new_and_existing_ids_with_integer_pk(self):
        """Test with integer primary key instead of text."""
        # Create table with integer PK
        self.db_manager.execute("CREATE TABLE test_entity (test_id INTEGER PRIMARY KEY, name TEXT)")
        self.db_manager.execute("INSERT INTO test_entity VALUES (1, 'Name1')")
        self.db_manager.execute("INSERT INTO test_entity VALUES (3, 'Name3')")

        ids = {"1", "2", "3", "4"}  # IDs from API are typically strings

        new_ids, existing_ids = self.manager._separate_new_and_existing_ids(
            ids=ids,
            entity_api_name="test_entity",
            primary_key="test_id",
            last_timestamp="2024-01-01T00:00:00Z",
        )

        assert new_ids == {"2", "4"}
        assert existing_ids == {"1", "3"}
