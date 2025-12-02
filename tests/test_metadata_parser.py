"""Tests for metadata XML parsing."""

import pytest

from lib.validation.metadata_parser import MetadataParser

# Sample $metadata XML for testing
SAMPLE_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Microsoft.Dynamics.CRM" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="vin_candidate">
        <Key>
          <PropertyRef Name="vin_candidateid"/>
        </Key>
        <Property Name="vin_candidateid" Type="Edm.Guid" Nullable="false"/>
        <Property Name="vin_name" Type="Edm.String" MaxLength="100" Nullable="true"/>
        <Property Name="vin_statuscode" Type="Edm.Int32" Nullable="false"/>
        <Property Name="createdon" Type="Edm.DateTimeOffset" Nullable="true"/>
        <NavigationProperty Name="createdby" Type="mscrm.systemuser">
          <ReferentialConstraint Property="_createdby_value" ReferencedProperty="systemuserid"/>
        </NavigationProperty>
      </EntityType>
      <EntityType Name="systemuser">
        <Key>
          <PropertyRef Name="systemuserid"/>
        </Key>
        <Property Name="systemuserid" Type="Edm.Guid" Nullable="false"/>
        <Property Name="fullname" Type="Edm.String" MaxLength="200" Nullable="true"/>
      </EntityType>
      <EntityType Name="abstract_entity" Abstract="true">
        <Property Name="id" Type="Edm.Guid"/>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


class TestMetadataParser:
    """Test metadata XML parsing."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.parser = MetadataParser(target_db="sqlite")

    def test_parse_basic_entity(self):
        """Test parsing a basic entity with columns."""
        schemas = self.parser.parse_metadata_xml(SAMPLE_METADATA_XML)

        # Should have parsed vin_candidate and systemuser (not abstract_entity)
        assert "vin_candidate" in schemas
        assert "systemuser" in schemas
        assert "abstract_entity" not in schemas  # Abstract entities are skipped

    def test_parse_primary_key(self):
        """Test parsing primary key."""
        schemas = self.parser.parse_metadata_xml(SAMPLE_METADATA_XML)

        candidate_schema = schemas["vin_candidate"]
        assert candidate_schema.primary_key == "vin_candidateid"

        user_schema = schemas["systemuser"]
        assert user_schema.primary_key == "systemuserid"

    def test_parse_columns(self):
        """Test parsing column definitions."""
        schemas = self.parser.parse_metadata_xml(SAMPLE_METADATA_XML)

        candidate_schema = schemas["vin_candidate"]

        # Should have 4 properties
        assert len(candidate_schema.columns) == 4

        # Check column names
        col_names = [col.name for col in candidate_schema.columns]
        assert "vin_candidateid" in col_names
        assert "vin_name" in col_names
        assert "vin_statuscode" in col_names
        assert "createdon" in col_names

    def test_parse_column_types(self):
        """Test parsing column types and mapping to SQLite."""
        schemas = self.parser.parse_metadata_xml(SAMPLE_METADATA_XML)

        candidate_schema = schemas["vin_candidate"]
        columns = {col.name: col for col in candidate_schema.columns}

        # Check Edm types are preserved
        assert columns["vin_candidateid"].edm_type == "Edm.Guid"
        assert columns["vin_name"].edm_type == "Edm.String"
        assert columns["vin_statuscode"].edm_type == "Edm.Int32"
        assert columns["createdon"].edm_type == "Edm.DateTimeOffset"

        # Check mapped database types (SQLite)
        assert columns["vin_candidateid"].db_type == "TEXT"  # Guid -> TEXT
        assert columns["vin_name"].db_type == "TEXT"  # String -> TEXT
        assert columns["vin_statuscode"].db_type == "INTEGER"  # Int32 -> INTEGER
        assert columns["createdon"].db_type == "TEXT"  # DateTimeOffset -> TEXT

    def test_parse_nullable(self):
        """Test parsing nullable attribute."""
        schemas = self.parser.parse_metadata_xml(SAMPLE_METADATA_XML)

        candidate_schema = schemas["vin_candidate"]
        columns = {col.name: col for col in candidate_schema.columns}

        # vin_candidateid is not nullable
        assert columns["vin_candidateid"].nullable is False

        # vin_name is nullable
        assert columns["vin_name"].nullable is True

    def test_parse_max_length(self):
        """Test parsing MaxLength attribute."""
        schemas = self.parser.parse_metadata_xml(SAMPLE_METADATA_XML)

        candidate_schema = schemas["vin_candidate"]
        columns = {col.name: col for col in candidate_schema.columns}

        # vin_name has MaxLength="100"
        assert columns["vin_name"].max_length == 100

        # fullname in systemuser has MaxLength="200"
        user_schema = schemas["systemuser"]
        user_columns = {col.name: col for col in user_schema.columns}
        assert user_columns["fullname"].max_length == 200

    def test_parse_foreign_keys(self):
        """Test parsing foreign keys from NavigationProperty."""
        schemas = self.parser.parse_metadata_xml(SAMPLE_METADATA_XML)

        candidate_schema = schemas["vin_candidate"]

        # Should have 1 foreign key
        assert len(candidate_schema.foreign_keys) == 1

        fk = candidate_schema.foreign_keys[0]
        assert fk.column == "_createdby_value"
        assert fk.referenced_table == "systemuser"
        assert fk.referenced_column == "systemuserid"

    def test_skip_abstract_entities(self):
        """Test that Abstract="true" entities are skipped."""
        schemas = self.parser.parse_metadata_xml(SAMPLE_METADATA_XML)

        # abstract_entity should not be in results
        assert "abstract_entity" not in schemas

    def test_invalid_xml_raises_error(self):
        """Test that invalid XML raises ValueError."""
        invalid_xml = "<invalid>not closed"

        with pytest.raises(ValueError) as exc_info:
            self.parser.parse_metadata_xml(invalid_xml)

        assert "Failed to parse XML" in str(exc_info.value)


class TestMetadataParserPostgreSQL:
    """Test metadata parsing with PostgreSQL target."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.parser = MetadataParser(target_db="postgresql")

    def test_postgresql_type_mapping(self):
        """Test that PostgreSQL types are mapped correctly."""
        schemas = self.parser.parse_metadata_xml(SAMPLE_METADATA_XML)

        candidate_schema = schemas["vin_candidate"]
        columns = {col.name: col for col in candidate_schema.columns}

        # Check PostgreSQL-specific mappings
        assert columns["vin_candidateid"].db_type == "UUID"  # Guid -> UUID
        assert columns["vin_statuscode"].db_type == "INTEGER"  # Int32 -> INTEGER
        assert columns["createdon"].db_type == "TIMESTAMP WITH TIME ZONE"
