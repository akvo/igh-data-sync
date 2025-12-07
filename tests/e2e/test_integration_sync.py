"""True end-to-end integration tests that call main workflow with mocked APIs."""

import sqlite3
from unittest.mock import patch

import pytest

from lib.config import EntityConfig
from lib.sync.database import DatabaseManager
from sync_dataverse import run_sync_workflow
from tests.helpers.fake_dataverse_client import FakeDataverseClient


class TestE2ESync:
    """True end-to-end tests calling run_sync_workflow() with fake API client."""

    @pytest.mark.asyncio
    async def test_complete_sync_workflow(
        self,
        test_config,
        temp_db,
        mock_metadata_xml,
    ):
        """Test complete sync workflow calling run_sync_workflow() with fake client."""

        # Setup entities
        test_entities = [
            EntityConfig(
                name="account",
                api_name="accounts",
                filtered=False,
                description="Test accounts",
            ),
            EntityConfig(
                name="contact",
                api_name="contacts",
                filtered=False,
                description="Test contacts",
            ),
        ]

        # Create fake client with canned responses (including option sets)
        fake_client = FakeDataverseClient(test_config, "fake-token")
        fake_client.set_metadata_response(mock_metadata_xml)
        fake_client.set_entity_response("accounts", [
            {
                "accountid": "00000000-0000-0000-0000-000000000001",
                "name": "Acme Corporation",
                "statuscode": 1,
                "statuscode@OData.Community.Display.V1.FormattedValue": "Active",
                "statecode": 0,
                "statecode@OData.Community.Display.V1.FormattedValue": "Active",
                "modifiedon": "2024-01-15T10:30:00Z",
                "createdon": "2024-01-01T09:00:00Z",
            },
            {
                "accountid": "00000000-0000-0000-0000-000000000002",
                "name": "Global Industries",
                "statuscode": 2,
                "statuscode@OData.Community.Display.V1.FormattedValue": "Inactive",
                "statecode": 1,
                "statecode@OData.Community.Display.V1.FormattedValue": "Inactive",
                "modifiedon": "2024-01-20T14:45:00Z",
                "createdon": "2024-01-05T11:30:00Z",
            },
        ])
        fake_client.set_entity_response("contacts", [
            {
                "contactid": "00000000-0000-0000-0000-000000000003",
                "fullname": "John Doe",
                "emailaddress1": "john.doe@example.com",
                "_parentcustomerid_value": "00000000-0000-0000-0000-000000000001",
                "preferredcontactmethodcode": 1,
                "preferredcontactmethodcode@OData.Community.Display.V1.FormattedValue": "Email",
                "modifiedon": "2024-01-18T12:00:00Z",
                "createdon": "2024-01-10T10:00:00Z",
            },
        ])

        # Suppress print statements for cleaner test output
        with patch("builtins.print"):
            # Call REAL sync workflow (this is the key difference!)
            db_manager = DatabaseManager(temp_db)
            await run_sync_workflow(
                fake_client,
                test_config,
                test_entities,
                db_manager,
                verify_references=False,
            )

        # Verify REAL business logic ran:
        # - Tables created via schema_initializer
        # - Records inserted via sync_entity() -> upsert_batch()
        # - Sync state tracked via SyncStateManager

        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()

        # Verify tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}
        assert "accounts" in tables
        assert "contacts" in tables
        assert "_sync_state" in tables

        # Verify data was synced via REAL sync_entity() logic
        cursor.execute("SELECT COUNT(*) FROM accounts")
        assert cursor.fetchone()[0] == 2

        cursor.execute("SELECT name FROM accounts ORDER BY name")
        names = [row[0] for row in cursor.fetchall()]
        assert names == ["Acme Corporation", "Global Industries"]

        cursor.execute("SELECT COUNT(*) FROM contacts")
        assert cursor.fetchone()[0] == 1

        # Verify sync state was tracked (proves SyncStateManager ran)
        cursor.execute("SELECT entity_name, state FROM _sync_state ORDER BY entity_name")
        states = cursor.fetchall()
        assert len(states) == 2
        assert states[0] == ("accounts", "completed")
        assert states[1] == ("contacts", "completed")

        # NEW: Verify option set tables were created
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '_optionset%' ORDER BY name"
        )
        optionset_tables = [row[0] for row in cursor.fetchall()]
        assert "_optionset_statuscode" in optionset_tables
        assert "_optionset_statecode" in optionset_tables
        assert "_optionset_preferredcontactmethodcode" in optionset_tables

        # NEW: Verify option set values were populated
        cursor.execute("SELECT code, label FROM _optionset_statuscode ORDER BY code")
        statuscode_values = cursor.fetchall()
        assert (1, "Active") in statuscode_values
        assert (2, "Inactive") in statuscode_values

        cursor.execute("SELECT code, label FROM _optionset_statecode ORDER BY code")
        statecode_values = cursor.fetchall()
        assert (0, "Active") in statecode_values
        assert (1, "Inactive") in statecode_values

        # NEW: Verify JOINs work correctly
        cursor.execute("""
            SELECT a.name, a.statuscode, s.label
            FROM accounts a
            LEFT JOIN _optionset_statuscode s ON a.statuscode = s.code
            ORDER BY a.name
        """)
        joined_data = cursor.fetchall()
        assert joined_data[0] == ("Acme Corporation", 1, "Active")
        assert joined_data[1] == ("Global Industries", 2, "Inactive")

        conn.close()

    @pytest.mark.asyncio
    async def test_incremental_sync(
        self,
        test_config,
        temp_db,
        mock_metadata_xml,
    ):
        """Test incremental sync uses modifiedon timestamp filtering."""

        test_entities = [
            EntityConfig(name="account", api_name="accounts", filtered=False, description=""),
        ]

        # Initial sync (with option sets)
        fake_client = FakeDataverseClient(test_config, "fake-token")
        fake_client.set_metadata_response(mock_metadata_xml)
        fake_client.set_entity_response("accounts", [
            {
                "accountid": "00000000-0000-0000-0000-000000000001",
                "name": "Acme Corp",
                "statuscode": 1,
                "statuscode@OData.Community.Display.V1.FormattedValue": "Active",
                "modifiedon": "2024-01-01T10:00:00Z",
                "createdon": "2024-01-01T09:00:00Z",
            },
        ])

        with patch("builtins.print"):
            db_manager = DatabaseManager(temp_db)
            await run_sync_workflow(fake_client, test_config, test_entities, db_manager)

        # Verify initial state
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM accounts")
        assert cursor.fetchone()[0] == "Acme Corp"
        conn.close()

        # Incremental sync with updated record (including NEW option set value)
        fake_client2 = FakeDataverseClient(test_config, "fake-token")
        fake_client2.set_metadata_response(mock_metadata_xml)
        fake_client2.set_entity_response("accounts", [
            {
                "accountid": "00000000-0000-0000-0000-000000000001",
                "name": "Acme Corporation (Updated)",  # Changed!
                "statuscode": 3,  # NEW option set value!
                "statuscode@OData.Community.Display.V1.FormattedValue": "Pending",
                "modifiedon": "2024-02-01T10:00:00Z",  # Newer timestamp
                "createdon": "2024-01-01T09:00:00Z",
            },
        ])

        with patch("builtins.print"):
            db_manager2 = DatabaseManager(temp_db)
            await run_sync_workflow(fake_client2, test_config, test_entities, db_manager2)

        # Verify update (proves upsert_batch() ran correctly with SCD2)
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()

        # SCD2: Query active records only (valid_to IS NULL)
        cursor.execute("SELECT name FROM accounts WHERE valid_to IS NULL")
        assert cursor.fetchone()[0] == "Acme Corporation (Updated)"

        # SCD2: Should have 2 records total (old version + new version)
        cursor.execute("SELECT COUNT(*) FROM accounts")
        assert cursor.fetchone()[0] == 2  # Historical + current

        # SCD2: Should have 1 active record
        cursor.execute("SELECT COUNT(*) FROM accounts WHERE valid_to IS NULL")
        assert cursor.fetchone()[0] == 1  # Only current version is active

        # NEW: Verify option set table now has BOTH old and new values
        cursor.execute("SELECT code, label FROM _optionset_statuscode ORDER BY code")
        statuscode_values = cursor.fetchall()
        assert len(statuscode_values) == 2  # Original "Active" + new "Pending"
        assert (1, "Active") in statuscode_values  # From first sync
        assert (3, "Pending") in statuscode_values  # From second sync

        # NEW: Verify the account record has the new statuscode (query active record only)
        cursor.execute("SELECT statuscode FROM accounts WHERE valid_to IS NULL")
        assert cursor.fetchone()[0] == 3

        # NEW: Verify JOIN returns the new label (query active record only)
        cursor.execute("""
            SELECT a.name, s.label
            FROM accounts a
            LEFT JOIN _optionset_statuscode s ON a.statuscode = s.code
            WHERE a.valid_to IS NULL
        """)
        result = cursor.fetchone()
        assert result == ("Acme Corporation (Updated)", "Pending")

        conn.close()

    @pytest.mark.asyncio
    async def test_filtered_sync_transitive_closure(
        self,
        test_config,
        temp_db,
    ):
        """Test filtered entity sync with transitive closure (FilteredSyncManager)."""

        # Metadata with FK relationships
        metadata_with_fks = """<?xml version="1.0"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Microsoft.Dynamics.CRM" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="vin_candidate">
        <Key><PropertyRef Name="vin_candidateid"/></Key>
        <Property Name="vin_candidateid" Type="Edm.Guid" Nullable="false"/>
        <Property Name="_parentaccountid_value" Type="Edm.Guid"/>
        <Property Name="_createdby_value" Type="Edm.Guid"/>
        <NavigationProperty Name="parentaccountid" Type="Microsoft.Dynamics.CRM.account">
          <ReferentialConstraint Property="_parentaccountid_value" ReferencedProperty="accountid"/>
        </NavigationProperty>
        <NavigationProperty Name="createdby" Type="Microsoft.Dynamics.CRM.systemuser">
          <ReferentialConstraint Property="_createdby_value" ReferencedProperty="systemuserid"/>
        </NavigationProperty>
      </EntityType>
      <EntityType Name="account">
        <Key><PropertyRef Name="accountid"/></Key>
        <Property Name="accountid" Type="Edm.Guid" Nullable="false"/>
        <Property Name="name" Type="Edm.String"/>
      </EntityType>
      <EntityType Name="systemuser">
        <Key><PropertyRef Name="systemuserid"/></Key>
        <Property Name="systemuserid" Type="Edm.Guid" Nullable="false"/>
        <Property Name="fullname" Type="Edm.String"/>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""

        test_entities = [
            EntityConfig(name="vin_candidate", api_name="vin_candidates", filtered=False, description=""),
            EntityConfig(name="account", api_name="accounts", filtered=True, description=""),  # Filtered!
            EntityConfig(name="systemuser", api_name="systemusers", filtered=True, description=""),  # Filtered!
        ]

        # Setup fake client
        fake_client = FakeDataverseClient(test_config, "fake-token")
        fake_client.set_metadata_response(metadata_with_fks)

        # Candidates reference specific accounts and users
        fake_client.set_entity_response("vin_candidates", [
            {"vin_candidateid": "c1", "_parentaccountid_value": "a1", "_createdby_value": "u1"},
            {"vin_candidateid": "c2", "_parentaccountid_value": "a1", "_createdby_value": "u2"},
        ])

        # Many accounts, but only a1 is referenced
        fake_client.set_entity_response("accounts", [
            {"accountid": "a1", "name": "Referenced Account"},
            {"accountid": "a2", "name": "Unreferenced Account"},  # Should NOT sync
            {"accountid": "a3", "name": "Another Unreferenced"},  # Should NOT sync
        ])

        # Many users, but only u1 and u2 are referenced
        fake_client.set_entity_response("systemusers", [
            {"systemuserid": "u1", "fullname": "User One"},
            {"systemuserid": "u2", "fullname": "User Two"},
            {"systemuserid": "u3", "fullname": "User Three"},  # Should NOT sync
        ])

        with patch("builtins.print"):
            db_manager = DatabaseManager(temp_db)
            await run_sync_workflow(fake_client, test_config, test_entities, db_manager)

        # Verify FilteredSyncManager transitive closure worked
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()

        # All candidates should sync
        cursor.execute("SELECT COUNT(*) FROM vin_candidates")
        assert cursor.fetchone()[0] == 2

        # Only referenced account should sync (proves FilteredSyncManager ran!)
        cursor.execute("SELECT COUNT(*) FROM accounts")
        assert cursor.fetchone()[0] == 1
        cursor.execute("SELECT name FROM accounts")
        assert cursor.fetchone()[0] == "Referenced Account"

        # Only 2 referenced users should sync
        cursor.execute("SELECT COUNT(*) FROM systemusers")
        assert cursor.fetchone()[0] == 2
        cursor.execute("SELECT fullname FROM systemusers ORDER BY fullname")
        names = [row[0] for row in cursor.fetchall()]
        assert names == ["User One", "User Two"]

        conn.close()

    @pytest.mark.asyncio
    async def test_empty_entity_sync(
        self,
        test_config,
        temp_db,
        mock_metadata_xml,
    ):
        """Test sync handles empty entities gracefully."""

        test_entities = [
            EntityConfig(name="account", api_name="accounts", filtered=False, description=""),
        ]

        fake_client = FakeDataverseClient(test_config, "fake-token")
        fake_client.set_metadata_response(mock_metadata_xml)
        fake_client.set_entity_response("accounts", [])  # No records

        with patch("builtins.print"):
            db_manager = DatabaseManager(temp_db)
            await run_sync_workflow(fake_client, test_config, test_entities, db_manager)

        # Verify table created but empty
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='accounts'")
        assert cursor.fetchone() is not None
        cursor.execute("SELECT COUNT(*) FROM accounts")
        assert cursor.fetchone()[0] == 0
        conn.close()

    @pytest.mark.asyncio
    async def test_multiselect_option_sets(
        self,
        test_config,
        temp_db,
        mock_metadata_xml,
    ):
        """Test multi-select option sets create junction tables."""

        test_entities = [
            EntityConfig(name="account", api_name="accounts", filtered=False, description=""),
        ]

        # Setup with multi-select option set
        fake_client = FakeDataverseClient(test_config, "fake-token")
        fake_client.set_metadata_response(mock_metadata_xml)
        fake_client.set_entity_response("accounts", [
            {
                "accountid": "00000000-0000-0000-0000-000000000001",
                "name": "Acme Corp",
                "categories": "1,2,3",  # Multi-select: comma-separated codes
                "categories@OData.Community.Display.V1.FormattedValue": "Technology;Healthcare;Finance",  # Semicolon-separated labels
                "modifiedon": "2024-01-01T10:00:00Z",
                "createdon": "2024-01-01T09:00:00Z",
            },
            {
                "accountid": "00000000-0000-0000-0000-000000000002",
                "name": "Global Industries",
                "categories": "2,4",  # Different categories
                "categories@OData.Community.Display.V1.FormattedValue": "Healthcare;Manufacturing",
                "modifiedon": "2024-01-01T10:00:00Z",
                "createdon": "2024-01-01T09:00:00Z",
            },
        ])

        with patch("builtins.print"):
            db_manager = DatabaseManager(temp_db)
            await run_sync_workflow(fake_client, test_config, test_entities, db_manager)

        # Verify results
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()

        # Verify lookup table created with all unique values
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='_optionset_categories'")
        assert cursor.fetchone() is not None

        cursor.execute("SELECT code, label FROM _optionset_categories ORDER BY code")
        categories = cursor.fetchall()
        assert len(categories) == 4
        assert (1, "Technology") in categories
        assert (2, "Healthcare") in categories
        assert (3, "Finance") in categories
        assert (4, "Manufacturing") in categories

        # Verify junction table created
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='_junction_accounts_categories'")
        assert cursor.fetchone() is not None

        # Verify junction records for first account (active records only)
        cursor.execute("""
            SELECT option_code
            FROM _junction_accounts_categories
            WHERE entity_id = '00000000-0000-0000-0000-000000000001'
              AND valid_to IS NULL
            ORDER BY option_code
        """)
        acme_categories = [row[0] for row in cursor.fetchall()]
        assert acme_categories == [1, 2, 3]

        # Verify junction records for second account (active records only)
        cursor.execute("""
            SELECT option_code
            FROM _junction_accounts_categories
            WHERE entity_id = '00000000-0000-0000-0000-000000000002'
              AND valid_to IS NULL
            ORDER BY option_code
        """)
        global_categories = [row[0] for row in cursor.fetchall()]
        assert global_categories == [2, 4]

        # Verify multi-select JOIN query works (active records only)
        cursor.execute("""
            SELECT a.name, GROUP_CONCAT(c.label, ', ') as category_labels
            FROM accounts a
            LEFT JOIN _junction_accounts_categories j
              ON a.accountid = j.entity_id AND j.valid_to IS NULL
            LEFT JOIN _optionset_categories c ON j.option_code = c.code
            WHERE a.valid_to IS NULL
            GROUP BY a.accountid, a.name
            ORDER BY a.name
        """)
        results = cursor.fetchall()
        assert len(results) == 2
        # Note: SQLite's GROUP_CONCAT might order differently, so we check membership
        acme_result = [r for r in results if r[0] == "Acme Corp"][0]
        assert "Technology" in acme_result[1]
        assert "Healthcare" in acme_result[1]
        assert "Finance" in acme_result[1]

        conn.close()

        # Test update: change categories for first account
        fake_client2 = FakeDataverseClient(test_config, "fake-token")
        fake_client2.set_metadata_response(mock_metadata_xml)
        fake_client2.set_entity_response("accounts", [
            {
                "accountid": "00000000-0000-0000-0000-000000000001",
                "name": "Acme Corp",
                "categories": "3,4",  # Changed: removed 1,2 and added 4
                "categories@OData.Community.Display.V1.FormattedValue": "Finance;Manufacturing",
                "modifiedon": "2024-02-01T10:00:00Z",
                "createdon": "2024-01-01T09:00:00Z",
            },
        ])

        with patch("builtins.print"):
            db_manager2 = DatabaseManager(temp_db)
            await run_sync_workflow(fake_client2, test_config, test_entities, db_manager2)

        # Verify junction records were updated correctly (active records only)
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT option_code
            FROM _junction_accounts_categories
            WHERE entity_id = '00000000-0000-0000-0000-000000000001'
              AND valid_to IS NULL
            ORDER BY option_code
        """)
        updated_categories = [row[0] for row in cursor.fetchall()]
        assert updated_categories == [3, 4]  # Old values removed, new values added

        conn.close()
