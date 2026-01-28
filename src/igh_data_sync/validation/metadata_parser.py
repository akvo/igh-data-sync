"""Parser for OData $metadata XML to extract entity schemas."""

import xml.etree.ElementTree as ET  # noqa: S405 - parsing trusted metadata from Dataverse API, not user input
from typing import Optional

from ..type_mapping import ColumnMetadata, ForeignKeyMetadata, TableSchema, map_edm_to_db_type

# OData namespace
EDM_NAMESPACE = "http://docs.oasis-open.org/odata/ns/edm"


class MetadataParser:
    """Parses OData $metadata XML to extract entity schemas."""

    def __init__(self, target_db: str = "sqlite"):
        """
        Initialize metadata parser.

        Args:
            target_db: Target database type ('sqlite' or 'postgresql')
        """
        self.target_db = target_db

    def parse_metadata_xml(
        self,
        xml_content: str,
        option_set_fields_by_entity: Optional[dict[str, list[str]]] = None,
    ) -> dict[str, TableSchema]:
        """
        Parse $metadata XML and extract all entity schemas.

        Args:
            xml_content: XML string from $metadata endpoint
            option_set_fields_by_entity: Optional dict mapping entity name to list of
                                         option set field names (from config file)

        Returns:
            Dict mapping entity name to TableSchema

        Raises:
            ValueError: If XML is invalid or cannot be parsed
        """
        try:
            root = ET.fromstring(xml_content)  # noqa: S314 - parsing trusted XML from Dataverse API, not user input
        except ET.ParseError as e:
            msg = f"Failed to parse XML: {e}"
            raise ValueError(msg) from e

        # Find all EntityType elements
        schemas = {}

        # Namespace handling
        ns = {"edm": EDM_NAMESPACE}

        # Find all Schema elements
        for schema_elem in root.findall(".//edm:Schema", ns):
            # Find all EntityType elements within this schema
            for entity_elem in schema_elem.findall("edm:EntityType", ns):
                # Skip Abstract entities
                if entity_elem.get("Abstract") == "true":
                    continue

                entity_name = entity_elem.get("Name")
                if not entity_name:
                    continue

                # Get option set fields for this entity (convert list to set)
                option_set_fields = (
                    set(option_set_fields_by_entity.get(entity_name, [])) if option_set_fields_by_entity else set()
                )

                # Parse this entity with option set field info
                table_schema = self._parse_entity_type(entity_elem, ns, option_set_fields)
                schemas[entity_name] = table_schema

        return schemas

    def _parse_entity_type(
        self,
        entity_elem: ET.Element,
        ns: dict[str, str],
        option_set_fields: Optional[set[str]] = None,
    ) -> TableSchema:
        """
        Parse a single EntityType element.

        Args:
            entity_elem: EntityType XML element
            ns: XML namespace dict
            option_set_fields: Optional set of field names that are option sets

        Returns:
            TableSchema for this entity
        """
        entity_name = entity_elem.get("Name")

        # Parse primary key
        primary_key = MetadataParser._parse_primary_key(entity_elem, ns)

        # Parse columns (properties) with option set field info
        columns = self._parse_properties(entity_elem, ns, option_set_fields)

        # Parse foreign keys using unified detection
        # (NavigationProperty + pattern matching for _*_value and *id columns)
        foreign_keys = MetadataParser._parse_all_foreign_keys(entity_elem, ns, columns, primary_key)

        return TableSchema(
            entity_name=entity_name,
            columns=columns,
            primary_key=primary_key,
            foreign_keys=foreign_keys,
        )

    @staticmethod
    def _parse_primary_key(entity_elem: ET.Element, ns: dict[str, str]) -> Optional[str]:
        """
        Parse primary key from Key/PropertyRef element.

        Args:
            entity_elem: EntityType XML element
            ns: XML namespace dict

        Returns:
            Primary key column name, or None if not found
        """
        key_elem = entity_elem.find("edm:Key", ns)
        if key_elem is None:
            return None

        prop_ref = key_elem.find("edm:PropertyRef", ns)
        if prop_ref is None:
            return None

        return prop_ref.get("Name")

    def _parse_properties(
        self,
        entity_elem: ET.Element,
        ns: dict[str, str],
        option_set_fields: Optional[set[str]] = None,
    ) -> list[ColumnMetadata]:
        """
        Parse all Property elements to extract column definitions.

        Args:
            entity_elem: EntityType XML element
            ns: XML namespace dict
            option_set_fields: Optional set of field names that are option sets

        Returns:
            List of ColumnMetadata
        """
        if option_set_fields is None:
            option_set_fields = set()

        columns = []

        for prop_elem in entity_elem.findall("edm:Property", ns):
            name = prop_elem.get("Name")
            edm_type = prop_elem.get("Type")

            if not name or not edm_type:
                continue

            # Parse nullable attribute (default is true)
            nullable_str = prop_elem.get("Nullable", "true")
            nullable = nullable_str.lower() == "true"

            # Parse max length
            max_length = None
            max_length_str = prop_elem.get("MaxLength")
            if max_length_str and max_length_str.isdigit():
                max_length = int(max_length_str)

            # Check if this field is in the option set config
            is_option_set = name in option_set_fields

            # Map to database type (with option set override)
            db_type = map_edm_to_db_type(
                edm_type,
                self.target_db,
                max_length,
                is_option_set=is_option_set,
            )

            column = ColumnMetadata(
                name=name,
                db_type=db_type,
                edm_type=edm_type,
                nullable=nullable,
                max_length=max_length,
            )

            columns.append(column)

        return columns

    @staticmethod
    def _extract_referenced_table_from_type(type_attr: str) -> str:
        """Extract entity name from Type attribute (removes Collection wrapper)."""
        # Remove "Collection(" wrapper if present
        if type_attr.startswith("Collection("):
            type_attr = type_attr[11:-1]
        # Extract entity name (after namespace prefix)
        return type_attr.split(".")[-1] if "." in type_attr else type_attr

    @staticmethod
    def _detect_dataverse_lookup_fk(col: ColumnMetadata, columns_with_fks: set) -> Optional[ForeignKeyMetadata]:
        """Detect _fieldname_value pattern (Dataverse lookup fields)."""
        if col.name in columns_with_fks:
            return None
        col_name = col.name.lower()
        if col_name.startswith("_") and col_name.endswith("_value"):
            fieldname = col.name[1:-6]  # Strip _ prefix and _value suffix
            return ForeignKeyMetadata(
                column=col.name,
                referenced_table=fieldname,
                referenced_column=f"{fieldname}id",
            )
        return None

    @staticmethod
    def _detect_junction_table_fk(
        col: ColumnMetadata, columns_with_fks: set, primary_key: Optional[str]
    ) -> Optional[ForeignKeyMetadata]:
        """Detect *id pattern (junction tables and simple references)."""
        if col.name in columns_with_fks:
            return None
        col_name = col.name.lower()
        if not col_name.endswith("id"):
            return None
        if primary_key and col.name == primary_key:
            return None
        if col.name == "versionnumber":
            return None

        referenced_table = col.name[:-2]  # Strip 'id' suffix
        return ForeignKeyMetadata(
            column=col.name,
            referenced_table=referenced_table,
            referenced_column=col.name,
        )

    @staticmethod
    def _parse_all_foreign_keys(
        entity_elem: ET.Element,
        ns: dict[str, str],
        columns: list[ColumnMetadata],
        primary_key: Optional[str],
    ) -> list[ForeignKeyMetadata]:
        """
        Unified FK detection using NavigationProperty + column pattern matching.

        This method consolidates FK detection by:
        1. Parsing NavigationProperty elements (authoritative source)
        2. Pattern-matching remaining columns for:
           - _*_value pattern (Dataverse lookup fields)
           - *id pattern (junction table columns)

        Pattern 1: _fieldname_value
            - Example: _createdby_value → createdby.createdbyid
            - Dataverse convention for lookup/foreign key fields
            - Used by regular entities

        Pattern 2: *id (junction tables)
            - Example: accountid → account.accountid
            - Used by junction tables (many-to-many relationships)
            - No NavigationProperty elements in metadata

        Args:
            entity_elem: EntityType XML element
            ns: XML namespace dict
            columns: List of column metadata
            primary_key: Primary key column name

        Returns:
            Comprehensive list of ForeignKeyMetadata from all sources
        """
        foreign_keys = []

        # STEP 1: Parse NavigationProperty elements (authoritative source)
        for nav_prop in entity_elem.findall("edm:NavigationProperty", ns):
            ref_constraint = nav_prop.find("edm:ReferentialConstraint", ns)
            if ref_constraint is None:
                continue

            column = ref_constraint.get("Property")
            referenced_column = ref_constraint.get("ReferencedProperty")
            if not column or not referenced_column:
                continue

            type_attr = nav_prop.get("Type", "")
            referenced_table = MetadataParser._extract_referenced_table_from_type(type_attr)
            if not referenced_table:
                continue

            foreign_keys.append(
                ForeignKeyMetadata(
                    column=column,
                    referenced_table=referenced_table,
                    referenced_column=referenced_column,
                )
            )

        # STEP 2: Pattern-match remaining columns for inferred FKs
        columns_with_fks = {fk.column for fk in foreign_keys}

        for col in columns:
            # Try Dataverse pattern (_fieldname_value)
            fk = MetadataParser._detect_dataverse_lookup_fk(col, columns_with_fks)
            if fk:
                foreign_keys.append(fk)
                continue

            # Try junction pattern (*id)
            fk = MetadataParser._detect_junction_table_fk(col, columns_with_fks, primary_key)
            if fk:
                foreign_keys.append(fk)

        return foreign_keys
