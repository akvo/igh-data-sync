"""Shared pytest fixtures for all tests."""

import tempfile
from pathlib import Path

import pytest

from igh_data_sync.config import Config


@pytest.fixture
def temp_db():
    """Create temporary database file that auto-cleans up."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    yield db_path
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def test_config(temp_db):
    """Create test configuration with temporary database.

    NOTE: This fixture depends on temp_db to ensure test isolation.
    Tests that don't use the database will still work, but will have
    a temp database created (and cleaned up) anyway.
    """
    return Config(
        api_url="https://test.crm.dynamics.com/api/data/v9.2",
        client_id="test-client-id",
        client_secret="test-client-secret",  # noqa: S106
        scope="https://test.crm.dynamics.com/.default",
        sqlite_db_path=temp_db,
        postgres_connection_string=None,
    )


@pytest.fixture
def test_token():
    """Create test access token."""
    return "test-access-token-12345"


@pytest.fixture
def mock_metadata_xml():
    """Sample metadata XML with multiple entities."""
    return """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Microsoft.Dynamics.CRM" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="account">
        <Key><PropertyRef Name="accountid"/></Key>
        <Property Name="accountid" Type="Edm.Guid" Nullable="false"/>
        <Property Name="name" Type="Edm.String" MaxLength="160"/>
        <Property Name="statuscode" Type="Edm.Int32"/>
        <Property Name="statecode" Type="Edm.Int32"/>
        <Property Name="categories" Type="Edm.String"/>
        <Property Name="modifiedon" Type="Edm.DateTimeOffset"/>
        <Property Name="createdon" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <EntityType Name="contact">
        <Key><PropertyRef Name="contactid"/></Key>
        <Property Name="contactid" Type="Edm.Guid" Nullable="false"/>
        <Property Name="firstname" Type="Edm.String" MaxLength="50"/>
        <Property Name="lastname" Type="Edm.String" MaxLength="50"/>
        <Property Name="preferredcontactmethodcode" Type="Edm.Int32"/>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""
