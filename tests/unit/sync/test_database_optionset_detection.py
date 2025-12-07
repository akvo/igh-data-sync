"""Tests for database option set detection integration."""

import pytest

from lib.sync.database import DatabaseManager
from lib.type_mapping import TableSchema


class TestDatabaseOptionSetDetection:
    """Test option set detection in database operations."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        """Create temp database."""
        db_path = tmp_path / "test.db"
        self.db = DatabaseManager(str(db_path))
        self.db.connect()
        yield
        self.db.close()

    def test_ensure_optionset_table(self):
        """Test creating option set lookup table."""
        self.db.ensure_optionset_table("statuscode")

        assert self.db.table_exists("_optionset_statuscode") is True

        # Calling again should be idempotent
        self.db.ensure_optionset_table("statuscode")
        assert self.db.table_exists("_optionset_statuscode") is True

    def test_ensure_junction_table(self):
        """Test creating junction table."""
        # First create entity and lookup tables
        self.db.execute("CREATE TABLE accounts (accountid TEXT PRIMARY KEY)")
        self.db.ensure_optionset_table("categories")

        # Create junction table
        self.db.ensure_junction_table("accounts", "categories", "accountid")

        assert self.db.table_exists("_junction_accounts_categories") is True

    def test_upsert_option_set_value_creates_table(self):
        """Test that upsert creates table if needed."""
        self.db.upsert_option_set_value("statuscode", 1, "Active")

        # Table should exist
        assert self.db.table_exists("_optionset_statuscode") is True

        # Value should be inserted
        cursor = self.db.conn.cursor()
        cursor.execute("SELECT code, label FROM _optionset_statuscode WHERE code = 1")
        row = cursor.fetchone()

        assert row is not None
        assert row[0] == 1
        assert row[1] == "Active"

    def test_populate_detected_option_sets_single_select(self):
        """Test populating single-select option set."""
        from lib.sync.optionset_detector import DetectedOptionSet

        # Create entity table
        self.db.execute(
            "CREATE TABLE accounts (accountid TEXT PRIMARY KEY, statuscode INTEGER)"
        )

        detected = {
            "statuscode": DetectedOptionSet(
                field_name="statuscode",
                is_multi_select=False,
                codes_and_labels={1: "Active", 2: "Inactive"},
            )
        }

        api_record = {
            "accountid": "acc123",
            "statuscode": 1,
        }

        self.db.populate_detected_option_sets(
            detected, "accounts", "acc123", "accountid"
        )

        # Check lookup table populated
        cursor = self.db.conn.cursor()
        cursor.execute("SELECT code, label FROM _optionset_statuscode ORDER BY code")
        rows = cursor.fetchall()

        assert len(rows) == 2
        assert rows[0][0] == 1
        assert rows[0][1] == "Active"
        assert rows[1][0] == 2
        assert rows[1][1] == "Inactive"

    def test_populate_detected_option_sets_multi_select(self):
        """Test populating multi-select option set."""
        from lib.sync.optionset_detector import DetectedOptionSet

        # Create entity table
        self.db.execute("CREATE TABLE accounts (accountid TEXT PRIMARY KEY)")

        detected = {
            "categories": DetectedOptionSet(
                field_name="categories",
                is_multi_select=True,
                codes_and_labels={1: "Category A", 2: "Category B"},
            )
        }

        api_record = {
            "accountid": "acc123",
            "categories": "1,2",
        }

        self.db.populate_detected_option_sets(
            detected, "accounts", "acc123", "accountid"
        )

        # Check lookup table populated
        cursor = self.db.conn.cursor()
        cursor.execute("SELECT code, label FROM _optionset_categories ORDER BY code")
        rows = cursor.fetchall()

        assert len(rows) == 2
        assert rows[0][0] == 1
        assert rows[0][1] == "Category A"

        # Check junction table populated (active records only)
        cursor.execute(
            "SELECT entity_id, option_code FROM _junction_accounts_categories "
            "WHERE valid_to IS NULL ORDER BY option_code"
        )
        junction_rows = cursor.fetchall()

        assert len(junction_rows) == 2
        assert junction_rows[0][0] == "acc123"
        assert junction_rows[0][1] == 1
