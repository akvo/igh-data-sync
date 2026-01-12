"""Tests for metadata parser with option set configuration."""

from igh_data_sync.validation.metadata_parser import MetadataParser


def test_parser_without_option_set_config():
    """Without option set config, Edm.String fields should map to TEXT."""
    xml = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Microsoft.Dynamics.CRM" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="vin_disease">
        <Key>
          <PropertyRef Name="vin_diseaseid"/>
        </Key>
        <Property Name="vin_diseaseid" Type="Edm.Guid" Nullable="false"/>
        <Property Name="statuscode" Type="Edm.String"/>
        <Property Name="vin_name" Type="Edm.String"/>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""

    parser = MetadataParser(target_db="sqlite")
    schemas = parser.parse_metadata_xml(xml)

    assert "vin_disease" in schemas
    schema = schemas["vin_disease"]

    statuscode_col = next((c for c in schema.columns if c.name == "statuscode"), None)
    name_col = next((c for c in schema.columns if c.name == "vin_name"), None)

    assert statuscode_col is not None
    assert name_col is not None
    assert statuscode_col.db_type == "TEXT"  # Without config, should be TEXT
    assert name_col.db_type == "TEXT"


def test_parser_with_option_set_config():
    """With option set config, specified Edm.String fields should map to INTEGER."""
    xml = """<?xml version="1.0" encoding="utf-8"?>
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

    # Config specifies that statuscode and new_globalhealtharea are option sets
    option_set_config = {"vin_disease": ["statuscode", "new_globalhealtharea"]}

    parser = MetadataParser(target_db="sqlite")
    schemas = parser.parse_metadata_xml(xml, option_set_fields_by_entity=option_set_config)

    schema = schemas["vin_disease"]

    statuscode_col = next((c for c in schema.columns if c.name == "statuscode"), None)
    healtharea_col = next((c for c in schema.columns if c.name == "new_globalhealtharea"), None)
    name_col = next((c for c in schema.columns if c.name == "vin_name"), None)

    assert statuscode_col is not None
    assert healtharea_col is not None
    assert name_col is not None

    # Option set fields should be INTEGER
    assert statuscode_col.db_type == "INTEGER"
    assert healtharea_col.db_type == "INTEGER"

    # Regular string field should still be TEXT
    assert name_col.db_type == "TEXT"


def test_parser_with_partial_option_set_config():
    """Config can specify option sets for some entities but not others."""
    xml = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Microsoft.Dynamics.CRM" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="vin_disease">
        <Key>
          <PropertyRef Name="vin_diseaseid"/>
        </Key>
        <Property Name="vin_diseaseid" Type="Edm.Guid" Nullable="false"/>
        <Property Name="statuscode" Type="Edm.String"/>
      </EntityType>
      <EntityType Name="vin_product">
        <Key>
          <PropertyRef Name="vin_productid"/>
        </Key>
        <Property Name="vin_productid" Type="Edm.Guid" Nullable="false"/>
        <Property Name="statuscode" Type="Edm.String"/>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""

    # Config only specifies option sets for vin_disease, not vin_product
    option_set_config = {"vin_disease": ["statuscode"]}

    parser = MetadataParser(target_db="sqlite")
    schemas = parser.parse_metadata_xml(xml, option_set_fields_by_entity=option_set_config)

    disease_statuscode = next((c for c in schemas["vin_disease"].columns if c.name == "statuscode"), None)
    product_statuscode = next((c for c in schemas["vin_product"].columns if c.name == "statuscode"), None)

    # vin_disease.statuscode should be INTEGER (in config)
    assert disease_statuscode.db_type == "INTEGER"

    # vin_product.statuscode should be TEXT (not in config)
    assert product_statuscode.db_type == "TEXT"


def test_parser_option_sets_dont_affect_other_types():
    """Option set config should only affect Edm.String, not other types."""
    xml = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Microsoft.Dynamics.CRM" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="vin_disease">
        <Key>
          <PropertyRef Name="vin_diseaseid"/>
        </Key>
        <Property Name="vin_diseaseid" Type="Edm.Guid" Nullable="false"/>
        <Property Name="statuscode" Type="Edm.String"/>
        <Property Name="versionnumber" Type="Edm.Int64"/>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""

    # Mistakenly include versionnumber in config (but it's Edm.Int64, not Edm.String)
    option_set_config = {"vin_disease": ["statuscode", "versionnumber"]}

    parser = MetadataParser(target_db="sqlite")
    schemas = parser.parse_metadata_xml(xml, option_set_fields_by_entity=option_set_config)

    schema = schemas["vin_disease"]

    statuscode_col = next((c for c in schema.columns if c.name == "statuscode"), None)
    versionnumber_col = next((c for c in schema.columns if c.name == "versionnumber"), None)

    # statuscode (Edm.String) should be overridden to INTEGER
    assert statuscode_col.db_type == "INTEGER"

    # versionnumber (Edm.Int64) should remain INTEGER (not affected by option set flag)
    assert versionnumber_col.db_type == "INTEGER"


def test_parser_empty_option_set_config():
    """Empty option set config should behave same as no config."""
    xml = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Microsoft.Dynamics.CRM" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="vin_disease">
        <Key>
          <PropertyRef Name="vin_diseaseid"/>
        </Key>
        <Property Name="vin_diseaseid" Type="Edm.Guid" Nullable="false"/>
        <Property Name="statuscode" Type="Edm.String"/>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""

    option_set_config = {}  # Empty config

    parser = MetadataParser(target_db="sqlite")
    schemas = parser.parse_metadata_xml(xml, option_set_fields_by_entity=option_set_config)

    schema = schemas["vin_disease"]
    statuscode_col = next((c for c in schema.columns if c.name == "statuscode"), None)

    # Should be TEXT (no option sets configured)
    assert statuscode_col.db_type == "TEXT"


def test_parser_postgresql_with_option_sets():
    """Option sets should work correctly with PostgreSQL target."""
    xml = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Microsoft.Dynamics.CRM" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="vin_disease">
        <Key>
          <PropertyRef Name="vin_diseaseid"/>
        </Key>
        <Property Name="vin_diseaseid" Type="Edm.Guid" Nullable="false"/>
        <Property Name="statuscode" Type="Edm.String" MaxLength="100"/>
        <Property Name="vin_name" Type="Edm.String" MaxLength="200"/>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""

    option_set_config = {"vin_disease": ["statuscode"]}

    parser = MetadataParser(target_db="postgresql")
    schemas = parser.parse_metadata_xml(xml, option_set_fields_by_entity=option_set_config)

    schema = schemas["vin_disease"]

    statuscode_col = next((c for c in schema.columns if c.name == "statuscode"), None)
    name_col = next((c for c in schema.columns if c.name == "vin_name"), None)

    # Option set should be INTEGER even with max_length
    assert statuscode_col.db_type == "INTEGER"

    # Regular string with max_length should be VARCHAR(n)
    assert name_col.db_type == "VARCHAR(200)"
