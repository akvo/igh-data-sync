"""True end-to-end integration tests that call main workflow with mocked APIs."""

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from lib.config import Config, EntityConfig
from lib.sync.database import DatabaseManager
from sync_dataverse import run_sync_workflow
from tests.helpers.fake_dataverse_client import FakeDataverseClient


@pytest.fixture
def temp_db():
    """Create temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    yield db_path
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def test_config(temp_db):
    """Create test configuration."""
    return Config(
        api_url="https://test.crm.dynamics.com/api/data/v9.2",
        client_id="test-client-id",
        client_secret="test-client-secret",
        scope="https://test.crm.dynamics.com/.default",
        sqlite_db_path=temp_db,
    )


@pytest.fixture
def mock_metadata_xml():
    """Sample metadata with multiple entities and relationships."""
    return """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Microsoft.Dynamics.CRM" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="account">
        <Key><PropertyRef Name="accountid"/></Key>
        <Property Name="accountid" Type="Edm.Guid" Nullable="false"/>
        <Property Name="name" Type="Edm.String" MaxLength="160"/>
        <Property Name="modifiedon" Type="Edm.DateTimeOffset"/>
        <Property Name="createdon" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <EntityType Name="contact">
        <Key><PropertyRef Name="contactid"/></Key>
        <Property Name="contactid" Type="Edm.Guid" Nullable="false"/>
        <Property Name="fullname" Type="Edm.String" MaxLength="160"/>
        <Property Name="emailaddress1" Type="Edm.String" MaxLength="100"/>
        <Property Name="_parentcustomerid_value" Type="Edm.Guid"/>
        <Property Name="modifiedon" Type="Edm.DateTimeOffset"/>
        <NavigationProperty Name="parentcustomerid_account" Type="Microsoft.Dynamics.CRM.account">
          <ReferentialConstraint Property="_parentcustomerid_value" ReferencedProperty="accountid"/>
        </NavigationProperty>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""


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

        # Create fake client with canned responses
        fake_client = FakeDataverseClient(test_config, "fake-token")
        fake_client.set_metadata_response(mock_metadata_xml)
        fake_client.set_entity_response("accounts", [
            {
                "accountid": "00000000-0000-0000-0000-000000000001",
                "name": "Acme Corporation",
                "modifiedon": "2024-01-15T10:30:00Z",
                "createdon": "2024-01-01T09:00:00Z",
            },
            {
                "accountid": "00000000-0000-0000-0000-000000000002",
                "name": "Global Industries",
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

        # Initial sync
        fake_client = FakeDataverseClient(test_config, "fake-token")
        fake_client.set_metadata_response(mock_metadata_xml)
        fake_client.set_entity_response("accounts", [
            {
                "accountid": "00000000-0000-0000-0000-000000000001",
                "name": "Acme Corp",
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

        # Incremental sync with updated record
        fake_client2 = FakeDataverseClient(test_config, "fake-token")
        fake_client2.set_metadata_response(mock_metadata_xml)
        fake_client2.set_entity_response("accounts", [
            {
                "accountid": "00000000-0000-0000-0000-000000000001",
                "name": "Acme Corporation (Updated)",  # Changed!
                "modifiedon": "2024-02-01T10:00:00Z",  # Newer timestamp
                "createdon": "2024-01-01T09:00:00Z",
            },
        ])

        with patch("builtins.print"):
            db_manager2 = DatabaseManager(temp_db)
            await run_sync_workflow(fake_client2, test_config, test_entities, db_manager2)

        # Verify update (proves upsert_batch() ran correctly)
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM accounts")
        assert cursor.fetchone()[0] == "Acme Corporation (Updated)"
        cursor.execute("SELECT COUNT(*) FROM accounts")
        assert cursor.fetchone()[0] == 1  # Still 1 record (upserted)
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
