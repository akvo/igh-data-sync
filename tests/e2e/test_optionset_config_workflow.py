"""End-to-end tests for option set configuration workflow."""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from igh_data_sync.config import EntityConfig
from igh_data_sync.sync.database import DatabaseManager
from igh_data_sync.sync.schema_initializer import initialize_tables


@pytest.fixture
def mock_metadata_xml():
    """Sample metadata XML with option set fields."""
    return """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Microsoft.Dynamics.CRM" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="vin_disease">
        <Key>
          <PropertyRef Name="vin_diseaseid"/>
        </Key>
        <Property Name="vin_diseaseid" Type="Edm.Guid" Nullable="false"/>
        <Property Name="statuscode" Type="Edm.String"/>
        <Property Name="new_globalhealtharea" Type="Edm.String"/>
        <Property Name="vin_name" Type="Edm.String"/>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""


@pytest.fixture
def temp_db():
    """Create a temporary database."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    yield db_path
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def temp_config_dir():
    """Create a temporary config directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.mark.asyncio
async def test_schema_creation_without_config_creates_text_columns(
    mock_metadata_xml, temp_db, temp_config_dir, monkeypatch
):
    """Without config, option set fields should be created as TEXT."""
    # Change to temp config dir so no config is found
    monkeypatch.chdir(temp_config_dir)

    # Mock client that returns metadata
    mock_client = AsyncMock()
    mock_client.get_metadata = AsyncMock(return_value=mock_metadata_xml)

    # Create database manager
    with DatabaseManager(temp_db) as db_manager:
        db_manager.init_sync_tables()

        # Define entity config
        entities = [
            EntityConfig(
                name="vin_disease",
                api_name="vin_diseases",
                filtered=False,
                description="Test entity",
            )
        ]

        # Initialize tables without config
        await initialize_tables(None, entities, mock_client, db_manager)

        # Verify table was created
        cursor = db_manager.conn.cursor()
        cursor.execute("SELECT sql FROM sqlite_master WHERE name='vin_diseases'")
        create_sql = cursor.fetchone()[0]

        # Check that option set fields are TEXT (without config)
        assert "statuscode TEXT" in create_sql
        assert "new_globalhealtharea TEXT" in create_sql
        assert "vin_name TEXT" in create_sql


@pytest.mark.asyncio
async def test_schema_creation_with_config_creates_integer_columns(
    mock_metadata_xml, temp_db, temp_config_dir, monkeypatch
):
    """With config, option set fields should be created as INTEGER."""
    # Define option set configuration
    config_data = {"vin_disease": ["statuscode", "new_globalhealtharea"]}

    # Mock client that returns metadata
    mock_client = AsyncMock()
    mock_client.get_metadata = AsyncMock(return_value=mock_metadata_xml)

    # Create database manager
    with DatabaseManager(temp_db) as db_manager:
        db_manager.init_sync_tables()

        # Define entity config
        entities = [
            EntityConfig(
                name="vin_disease",
                api_name="vin_diseases",
                filtered=False,
                description="Test entity",
            )
        ]

        # Initialize tables with config passed as parameter
        await initialize_tables(None, entities, mock_client, db_manager, config_data)

        # Verify table was created
        cursor = db_manager.conn.cursor()
        cursor.execute("SELECT sql FROM sqlite_master WHERE name='vin_diseases'")
        create_sql = cursor.fetchone()[0]

        # Check that option set fields are INTEGER (with config)
        assert "statuscode INTEGER" in create_sql
        assert "new_globalhealtharea INTEGER" in create_sql
        # Regular string field should still be TEXT
        assert "vin_name TEXT" in create_sql


@pytest.mark.asyncio
async def test_config_file_with_multiple_entities(temp_db, temp_config_dir, monkeypatch):
    """Config should correctly map option sets for multiple entities."""
    # Define config with multiple entities
    config_data = {
        "vin_disease": ["statuscode", "new_globalhealtharea"],
        "vin_product": ["statuscode", "vin_type"],
    }

    # Mock metadata for multiple entities
    multi_entity_xml = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Microsoft.Dynamics.CRM" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="vin_disease">
        <Key><PropertyRef Name="vin_diseaseid"/></Key>
        <Property Name="vin_diseaseid" Type="Edm.Guid" Nullable="false"/>
        <Property Name="statuscode" Type="Edm.String"/>
        <Property Name="new_globalhealtharea" Type="Edm.String"/>
      </EntityType>
      <EntityType Name="vin_product">
        <Key><PropertyRef Name="vin_productid"/></Key>
        <Property Name="vin_productid" Type="Edm.Guid" Nullable="false"/>
        <Property Name="statuscode" Type="Edm.String"/>
        <Property Name="vin_type" Type="Edm.String"/>
        <Property Name="vin_name" Type="Edm.String"/>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""

    mock_client = AsyncMock()
    mock_client.get_metadata = AsyncMock(return_value=multi_entity_xml)

    with DatabaseManager(temp_db) as db_manager:
        db_manager.init_sync_tables()

        entities = [
            EntityConfig(
                name="vin_disease",
                api_name="vin_diseases",
                filtered=False,
                description="Test disease",
            ),
            EntityConfig(
                name="vin_product",
                api_name="vin_products",
                filtered=False,
                description="Test product",
            ),
        ]

        # Pass config as parameter
        await initialize_tables(None, entities, mock_client, db_manager, config_data)

        cursor = db_manager.conn.cursor()

        # Check vin_diseases
        cursor.execute("SELECT sql FROM sqlite_master WHERE name='vin_diseases'")
        diseases_sql = cursor.fetchone()[0]
        assert "statuscode INTEGER" in diseases_sql
        assert "new_globalhealtharea INTEGER" in diseases_sql

        # Check vin_products
        cursor.execute("SELECT sql FROM sqlite_master WHERE name='vin_products'")
        products_sql = cursor.fetchone()[0]
        assert "statuscode INTEGER" in products_sql
        assert "vin_type INTEGER" in products_sql
        assert "vin_name TEXT" in products_sql  # Not in config


@pytest.mark.asyncio
async def test_config_loading_shows_informative_messages(
    mock_metadata_xml, temp_db, temp_config_dir, monkeypatch, capsys
):
    """Config loading should print helpful messages to user."""
    # Define config data
    config_data = {"vin_disease": ["statuscode", "new_globalhealtharea"]}

    mock_client = AsyncMock()
    mock_client.get_metadata = AsyncMock(return_value=mock_metadata_xml)

    with DatabaseManager(temp_db) as db_manager:
        db_manager.init_sync_tables()

        entities = [
            EntityConfig(
                name="vin_disease",
                api_name="vin_diseases",
                filtered=False,
                description="Test entity",
            )
        ]

        # Pass config as parameter
        await initialize_tables(None, entities, mock_client, db_manager, config_data)

        # Check printed output
        captured = capsys.readouterr()
        assert "Using option set config: 1 entities, 2 fields" in captured.out


@pytest.mark.asyncio
async def test_no_config_shows_warning_message(mock_metadata_xml, temp_db, temp_config_dir, monkeypatch, capsys):
    """Without config, should show helpful warning."""
    mock_client = AsyncMock()
    mock_client.get_metadata = AsyncMock(return_value=mock_metadata_xml)

    with DatabaseManager(temp_db) as db_manager:
        db_manager.init_sync_tables()

        entities = [
            EntityConfig(
                name="vin_disease",
                api_name="vin_diseases",
                filtered=False,
                description="Test entity",
            )
        ]

        # Call without config parameter (defaults to None)
        await initialize_tables(None, entities, mock_client, db_manager)

        # Check printed output
        captured = capsys.readouterr()
        assert "No option set config provided" in captured.out
        assert "tables will use TEXT for option sets" in captured.out
        assert "generate-optionset-config" in captured.out
