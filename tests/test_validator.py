"""Basic tests for schema validator."""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from lib.config import Config, EntityConfig
from lib.dataverse_client import DataverseClient
from lib.sync.database import DatabaseManager
from lib.validation.validator import validate_schema_before_sync


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
def test_entity():
    """Create test entity configuration."""
    return EntityConfig(
        name="account",
        api_name="accounts",
        filtered=False,
        description="Test account entity",
    )


@pytest.fixture
def mock_metadata_xml():
    """Mock $metadata XML."""
    return """<?xml version="1.0"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Microsoft.Dynamics.CRM" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="account">
        <Key><PropertyRef Name="accountid"/></Key>
        <Property Name="accountid" Type="Edm.Guid" Nullable="false"/>
        <Property Name="name" Type="Edm.String"/>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""


class TestValidator:
    """Tests for validate_schema_before_sync function."""

    @pytest.mark.asyncio
    async def test_validate_schema_new_entity(
        self,
        test_config,
        test_entity,
        temp_db,
        mock_metadata_xml,
    ):
        """Test validation when entity doesn't exist in database yet."""
        # Setup mock client
        mock_client = MagicMock(spec=DataverseClient)
        mock_client.config = test_config
        mock_client.get_metadata = AsyncMock(return_value=mock_metadata_xml)

        # Setup database manager
        db_manager = DatabaseManager(temp_db)
        db_manager.init_sync_tables()

        # Run validation
        valid_entities, entities_to_create, diffs = await validate_schema_before_sync(
            test_config,
            [test_entity],
            mock_client,
            db_manager,
        )

        # Verify results
        assert len(valid_entities) == 1
        assert valid_entities[0].name == "account"
        assert len(entities_to_create) == 1
        assert entities_to_create[0].name == "account"

        db_manager.close()
