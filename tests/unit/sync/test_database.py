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


class TestSCD2Operations:
    """Test SCD2-specific database operations."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        """Create temp database with SCD2 schema."""
        db_path = tmp_path / "test.db"
        self.db = DatabaseManager(str(db_path))

        # Create table with SCD2 schema (row_id, valid_to)
        self.db.execute("""
            CREATE TABLE accounts (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                accountid TEXT NOT NULL,
                name TEXT,
                json_response TEXT NOT NULL,
                sync_time TEXT NOT NULL,
                valid_from TEXT,
                valid_to TEXT
            )
        """)
        yield
        self.db.close()

    def test_scd2_insert_new_record(self):
        """Test SCD2 inserts new record with valid_to = NULL."""
        record = {
            "accountid": "a1",
            "name": "Acme Corp",
            "json_response": '{"accountid": "a1", "name": "Acme Corp"}',
            "sync_time": "2024-01-01T10:00:00Z",
            "valid_from": "2024-01-01T09:00:00Z",
        }

        result = self.db.upsert_scd2("accounts", "accountid", record)
        assert result.is_new_entity is True
        assert result.version_created is True

        # Verify record inserted with valid_to = NULL
        cursor = self.db.conn.cursor()
        cursor.execute(
            "SELECT accountid, name, valid_from, valid_to FROM accounts WHERE accountid = 'a1'"
        )
        row = cursor.fetchone()
        assert row[0] == "a1"
        assert row[1] == "Acme Corp"
        assert row[2] == "2024-01-01T09:00:00Z"
        assert row[3] is None  # valid_to should be NULL

    def test_scd2_update_closes_old_and_inserts_new(self):
        """Test SCD2 closes old record and inserts new on update."""
        # Insert initial record
        record1 = {
            "accountid": "a1",
            "name": "Acme Corp",
            "json_response": '{"accountid": "a1", "name": "Acme Corp"}',
            "sync_time": "2024-01-01T10:00:00Z",
            "valid_from": "2024-01-01T09:00:00Z",
        }
        self.db.upsert_scd2("accounts", "accountid", record1)

        # Update record
        record2 = {
            "accountid": "a1",
            "name": "Acme Corporation",  # Changed
            "json_response": '{"accountid": "a1", "name": "Acme Corporation"}',
            "sync_time": "2024-02-01T10:00:00Z",
            "valid_from": "2024-02-01T09:00:00Z",
        }
        result = self.db.upsert_scd2("accounts", "accountid", record2)
        assert result.is_new_entity is False  # Update, not new
        assert result.version_created is True  # But new version created

        # Verify TWO records exist
        cursor = self.db.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM accounts WHERE accountid = 'a1'")
        assert cursor.fetchone()[0] == 2

        # Verify old record is closed
        cursor.execute(
            "SELECT name, valid_to FROM accounts WHERE accountid = 'a1' "
            "ORDER BY row_id LIMIT 1"
        )
        row = cursor.fetchone()
        assert row[0] == "Acme Corp"
        assert row[1] == "2024-02-01T09:00:00Z"  # Closed with new valid_from

        # Verify new record is active
        cursor.execute(
            "SELECT name, valid_from, valid_to FROM accounts WHERE accountid = 'a1' "
            "AND valid_to IS NULL"
        )
        row = cursor.fetchone()
        assert row[0] == "Acme Corporation"
        assert row[1] == "2024-02-01T09:00:00Z"
        assert row[2] is None  # Active

    def test_scd2_no_change_no_new_version(self):
        """Test SCD2 doesn't create new version if data unchanged."""
        record1 = {
            "accountid": "a1",
            "name": "Acme Corp",
            "json_response": '{"accountid": "a1", "name": "Acme Corp"}',
            "sync_time": "2024-01-01T10:00:00Z",
            "valid_from": "2024-01-01T09:00:00Z",
        }
        self.db.upsert_scd2("accounts", "accountid", record1)

        # Re-sync same data (sync_time different but json_response same)
        record2 = {
            "accountid": "a1",
            "name": "Acme Corp",
            "json_response": '{"accountid": "a1", "name": "Acme Corp"}',  # Same
            "sync_time": "2024-01-02T10:00:00Z",  # Different
            "valid_from": "2024-01-01T09:00:00Z",  # Same
        }
        result = self.db.upsert_scd2("accounts", "accountid", record2)
        assert result.is_new_entity is False
        assert result.version_created is False  # No change, no new version

        # Verify only ONE record exists
        cursor = self.db.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM accounts WHERE accountid = 'a1'")
        assert cursor.fetchone()[0] == 1

        # Verify sync_time was updated
        cursor.execute("SELECT sync_time FROM accounts WHERE accountid = 'a1'")
        assert cursor.fetchone()[0] == "2024-01-02T10:00:00Z"

    def test_scd2_query_active_records(self):
        """Test querying active records using valid_to IS NULL."""
        # Insert multiple versions
        for name, timestamp in [
            ("Acme Corp", "2024-01-01T09:00:00Z"),
            ("Acme Corporation", "2024-02-01T09:00:00Z"),
            ("Acme Industries", "2024-03-01T09:00:00Z"),
        ]:
            record = {
                "accountid": "a1",
                "name": name,
                "json_response": f'{{"accountid": "a1", "name": "{name}"}}',
                "sync_time": timestamp,
                "valid_from": timestamp,
            }
            self.db.upsert_scd2("accounts", "accountid", record)

        # Query active records
        cursor = self.db.conn.cursor()
        cursor.execute("SELECT name FROM accounts WHERE valid_to IS NULL")
        active = [row[0] for row in cursor.fetchall()]

        assert len(active) == 1
        assert active[0] == "Acme Industries"

    def test_scd2_multiple_records(self):
        """Test SCD2 with multiple different records."""
        # Insert two different accounts
        for accountid, name in [("a1", "Acme Corp"), ("a2", "Beta Inc")]:
            record = {
                "accountid": accountid,
                "name": name,
                "json_response": f'{{"accountid": "{accountid}", "name": "{name}"}}',
                "sync_time": "2024-01-01T10:00:00Z",
                "valid_from": "2024-01-01T09:00:00Z",
            }
            self.db.upsert_scd2("accounts", "accountid", record)

        # Update first account
        record_update = {
            "accountid": "a1",
            "name": "Acme Corporation",
            "json_response": '{"accountid": "a1", "name": "Acme Corporation"}',
            "sync_time": "2024-02-01T10:00:00Z",
            "valid_from": "2024-02-01T09:00:00Z",
        }
        self.db.upsert_scd2("accounts", "accountid", record_update)

        # Verify total records (2 for a1, 1 for a2)
        cursor = self.db.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM accounts")
        assert cursor.fetchone()[0] == 3

        # Verify active records (1 for each)
        cursor.execute("SELECT COUNT(*) FROM accounts WHERE valid_to IS NULL")
        assert cursor.fetchone()[0] == 2

        # Verify a2 is still "Beta Inc"
        cursor.execute(
            "SELECT name FROM accounts WHERE accountid = 'a2' AND valid_to IS NULL"
        )
        assert cursor.fetchone()[0] == "Beta Inc"


class TestJunctionTableSCD2:
    """Test SCD2 temporal tracking for junction tables."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        """Create temp database with junction table SCD2 schema."""
        db_path = tmp_path / "test.db"
        self.db = DatabaseManager(str(db_path))

        # Create entity table with SCD2 schema
        self.db.execute("""
            CREATE TABLE accounts (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                accountid TEXT NOT NULL,
                name TEXT,
                json_response TEXT NOT NULL,
                sync_time TEXT NOT NULL,
                valid_from TEXT,
                valid_to TEXT
            )
        """)

        # Create junction table with temporal tracking
        self.db.ensure_junction_table("accounts", "categories", "accountid")

        yield
        self.db.close()

    def test_junction_snapshot_on_new_entity(self):
        """Test junction records created with valid_to = NULL for new entity."""
        from lib.sync.database import SCD2Result

        # Snapshot for new entity
        scd2_result = SCD2Result(
            is_new_entity=True,
            version_created=True,
            valid_from="2024-01-01T09:00:00Z",
            business_key_value="a1",
        )

        self.db.snapshot_junction_relationships(
            table_name="_junction_accounts_categories",
            entity_id="a1",
            option_codes=[1, 2, 3],
            valid_from=scd2_result.valid_from,
        )

        # Verify 3 junction records created
        cursor = self.db.conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM _junction_accounts_categories WHERE entity_id = 'a1'"
        )
        assert cursor.fetchone()[0] == 3

        # Verify all have valid_to = NULL (active)
        cursor.execute(
            "SELECT COUNT(*) FROM _junction_accounts_categories "
            "WHERE entity_id = 'a1' AND valid_to IS NULL"
        )
        assert cursor.fetchone()[0] == 3

        # Verify valid_from matches parent
        cursor.execute(
            "SELECT DISTINCT valid_from FROM _junction_accounts_categories "
            "WHERE entity_id = 'a1'"
        )
        assert cursor.fetchone()[0] == "2024-01-01T09:00:00Z"

    def test_junction_snapshot_on_entity_update(self):
        """Test junction snapshot closes old records and creates new ones."""
        from lib.sync.database import SCD2Result

        # Initial snapshot
        self.db.snapshot_junction_relationships(
            table_name="_junction_accounts_categories",
            entity_id="a1",
            option_codes=[1, 2],
            valid_from="2024-01-01T09:00:00Z",
        )

        # Update: different categories
        self.db.snapshot_junction_relationships(
            table_name="_junction_accounts_categories",
            entity_id="a1",
            option_codes=[2, 3, 4],
            valid_from="2024-02-01T09:00:00Z",
        )

        cursor = self.db.conn.cursor()

        # Verify total records: 2 old + 3 new = 5
        cursor.execute(
            "SELECT COUNT(*) FROM _junction_accounts_categories WHERE entity_id = 'a1'"
        )
        assert cursor.fetchone()[0] == 5

        # Verify only 3 active records (new snapshot)
        cursor.execute(
            "SELECT COUNT(*) FROM _junction_accounts_categories "
            "WHERE entity_id = 'a1' AND valid_to IS NULL"
        )
        assert cursor.fetchone()[0] == 3

        # Verify old records closed with correct valid_to
        cursor.execute(
            "SELECT COUNT(*) FROM _junction_accounts_categories "
            "WHERE entity_id = 'a1' AND valid_to = '2024-02-01T09:00:00Z'"
        )
        assert cursor.fetchone()[0] == 2

        # Verify new snapshot has correct option codes
        cursor.execute(
            "SELECT option_code FROM _junction_accounts_categories "
            "WHERE entity_id = 'a1' AND valid_to IS NULL ORDER BY option_code"
        )
        codes = [row[0] for row in cursor.fetchall()]
        assert codes == [2, 3, 4]

    def test_junction_no_snapshot_when_entity_unchanged(self):
        """Test no junction snapshot when parent entity version_created = False."""
        from lib.sync.database import SCD2Result

        # Initial snapshot
        scd2_result1 = SCD2Result(
            is_new_entity=True,
            version_created=True,
            valid_from="2024-01-01T09:00:00Z",
            business_key_value="a1",
        )

        # Simulate populate_detected_option_sets with version_created=True
        from lib.sync.optionset_detector import DetectedOptionSet

        detected = {
            "categories": DetectedOptionSet(
                field_name="categories",
                is_multi_select=True,
                codes_and_labels={1: "Cat1", 2: "Cat2"},
            )
        }

        self.db.populate_detected_option_sets(
            detected=detected,
            entity_name="accounts",
            entity_id="a1",
            entity_pk="accountid",
            scd2_result=scd2_result1,
        )

        # Verify 2 junction records created
        cursor = self.db.conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM _junction_accounts_categories WHERE entity_id = 'a1'"
        )
        assert cursor.fetchone()[0] == 2

        # Second sync: no change in entity (version_created=False)
        scd2_result2 = SCD2Result(
            is_new_entity=False,
            version_created=False,
            valid_from="2024-01-01T09:00:00Z",
            business_key_value="a1",
        )

        self.db.populate_detected_option_sets(
            detected=detected,
            entity_name="accounts",
            entity_id="a1",
            entity_pk="accountid",
            scd2_result=scd2_result2,
        )

        # Verify still only 2 junction records (no new snapshot)
        cursor.execute(
            "SELECT COUNT(*) FROM _junction_accounts_categories WHERE entity_id = 'a1'"
        )
        assert cursor.fetchone()[0] == 2

    def test_junction_query_active_relationships(self):
        """Test querying active relationships with WHERE valid_to IS NULL."""
        # Create 3 versions of same entity
        self.db.snapshot_junction_relationships(
            table_name="_junction_accounts_categories",
            entity_id="a1",
            option_codes=[1, 2],
            valid_from="2024-01-01T09:00:00Z",
        )

        self.db.snapshot_junction_relationships(
            table_name="_junction_accounts_categories",
            entity_id="a1",
            option_codes=[2, 3],
            valid_from="2024-02-01T09:00:00Z",
        )

        self.db.snapshot_junction_relationships(
            table_name="_junction_accounts_categories",
            entity_id="a1",
            option_codes=[3, 4, 5],
            valid_from="2024-03-01T09:00:00Z",
        )

        cursor = self.db.conn.cursor()

        # Verify total records: 2 + 2 + 3 = 7
        cursor.execute(
            "SELECT COUNT(*) FROM _junction_accounts_categories WHERE entity_id = 'a1'"
        )
        assert cursor.fetchone()[0] == 7

        # Query active relationships
        cursor.execute(
            "SELECT option_code FROM _junction_accounts_categories "
            "WHERE entity_id = 'a1' AND valid_to IS NULL ORDER BY option_code"
        )
        active_codes = [row[0] for row in cursor.fetchall()]
        assert active_codes == [3, 4, 5]  # Only latest version

    def test_junction_point_in_time_query(self):
        """Test querying relationships as of specific date."""
        # Create 3 versions
        self.db.snapshot_junction_relationships(
            table_name="_junction_accounts_categories",
            entity_id="a1",
            option_codes=[1, 2],
            valid_from="2024-01-01T09:00:00Z",
        )

        self.db.snapshot_junction_relationships(
            table_name="_junction_accounts_categories",
            entity_id="a1",
            option_codes=[2, 3],
            valid_from="2024-02-01T09:00:00Z",
        )

        self.db.snapshot_junction_relationships(
            table_name="_junction_accounts_categories",
            entity_id="a1",
            option_codes=[3, 4, 5],
            valid_from="2024-03-01T09:00:00Z",
        )

        cursor = self.db.conn.cursor()

        # Query as of 2024-02-15 (should return version 2)
        cursor.execute(
            "SELECT option_code FROM _junction_accounts_categories "
            "WHERE entity_id = 'a1' "
            "AND valid_from <= '2024-02-15T00:00:00Z' "
            "AND (valid_to IS NULL OR valid_to > '2024-02-15T00:00:00Z') "
            "ORDER BY option_code"
        )
        codes = [row[0] for row in cursor.fetchall()]
        assert codes == [2, 3]  # Version 2 relationships
