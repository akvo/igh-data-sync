"""Parser for OData $metadata XML to extract entity schemas."""
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional
from ..type_mapping import (
    TableSchema, ColumnMetadata, ForeignKeyMetadata,
    map_edm_to_db_type
)


# OData namespace
EDM_NAMESPACE = 'http://docs.oasis-open.org/odata/ns/edm'


class MetadataParser:
    """Parses OData $metadata XML to extract entity schemas."""

    def __init__(self, target_db: str = 'sqlite'):
        """
        Initialize metadata parser.

        Args:
            target_db: Target database type ('sqlite' or 'postgresql')
        """
        self.target_db = target_db

    def parse_metadata_xml(self, xml_content: str) -> Dict[str, TableSchema]:
        """
        Parse $metadata XML and extract all entity schemas.

        Args:
            xml_content: XML string from $metadata endpoint

        Returns:
            Dict mapping entity name to TableSchema

        Raises:
            ValueError: If XML is invalid or cannot be parsed
        """
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:
            raise ValueError(f"Failed to parse XML: {e}")

        # Find all EntityType elements
        schemas = {}

        # Namespace handling
        ns = {'edm': EDM_NAMESPACE}

        # Find all Schema elements
        for schema_elem in root.findall('.//edm:Schema', ns):
            # Find all EntityType elements within this schema
            for entity_elem in schema_elem.findall('edm:EntityType', ns):
                # Skip Abstract entities
                if entity_elem.get('Abstract') == 'true':
                    continue

                entity_name = entity_elem.get('Name')
                if not entity_name:
                    continue

                # Parse this entity
                table_schema = self._parse_entity_type(entity_elem, ns)
                schemas[entity_name] = table_schema

        return schemas

    def _parse_entity_type(
        self,
        entity_elem: ET.Element,
        ns: Dict[str, str]
    ) -> TableSchema:
        """
        Parse a single EntityType element.

        Args:
            entity_elem: EntityType XML element
            ns: XML namespace dict

        Returns:
            TableSchema for this entity
        """
        entity_name = entity_elem.get('Name')

        # Parse primary key
        primary_key = self._parse_primary_key(entity_elem, ns)

        # Parse columns (properties)
        columns = self._parse_properties(entity_elem, ns)

        # Parse foreign keys (navigation properties with referential constraints)
        foreign_keys = self._parse_foreign_keys(entity_elem, ns)

        return TableSchema(
            entity_name=entity_name,
            columns=columns,
            primary_key=primary_key,
            foreign_keys=foreign_keys
        )

    def _parse_primary_key(
        self,
        entity_elem: ET.Element,
        ns: Dict[str, str]
    ) -> Optional[str]:
        """
        Parse primary key from Key/PropertyRef element.

        Args:
            entity_elem: EntityType XML element
            ns: XML namespace dict

        Returns:
            Primary key column name, or None if not found
        """
        key_elem = entity_elem.find('edm:Key', ns)
        if key_elem is None:
            return None

        prop_ref = key_elem.find('edm:PropertyRef', ns)
        if prop_ref is None:
            return None

        return prop_ref.get('Name')

    def _parse_properties(
        self,
        entity_elem: ET.Element,
        ns: Dict[str, str]
    ) -> List[ColumnMetadata]:
        """
        Parse all Property elements to extract column definitions.

        Args:
            entity_elem: EntityType XML element
            ns: XML namespace dict

        Returns:
            List of ColumnMetadata
        """
        columns = []

        for prop_elem in entity_elem.findall('edm:Property', ns):
            name = prop_elem.get('Name')
            edm_type = prop_elem.get('Type')

            if not name or not edm_type:
                continue

            # Parse nullable attribute (default is true)
            nullable_str = prop_elem.get('Nullable', 'true')
            nullable = nullable_str.lower() == 'true'

            # Parse max length
            max_length = None
            max_length_str = prop_elem.get('MaxLength')
            if max_length_str and max_length_str.isdigit():
                max_length = int(max_length_str)

            # Map to database type
            db_type = map_edm_to_db_type(edm_type, self.target_db, max_length)

            column = ColumnMetadata(
                name=name,
                db_type=db_type,
                edm_type=edm_type,
                nullable=nullable,
                max_length=max_length
            )

            columns.append(column)

        return columns

    def _parse_foreign_keys(
        self,
        entity_elem: ET.Element,
        ns: Dict[str, str]
    ) -> List[ForeignKeyMetadata]:
        """
        Parse NavigationProperty elements with ReferentialConstraint to extract foreign keys.

        Example XML:
        <NavigationProperty Name="createdby" Type="mscrm.systemuser">
          <ReferentialConstraint Property="_createdby_value" ReferencedProperty="systemuserid"/>
        </NavigationProperty>

        Args:
            entity_elem: EntityType XML element
            ns: XML namespace dict

        Returns:
            List of ForeignKeyMetadata
        """
        foreign_keys = []

        for nav_prop in entity_elem.findall('edm:NavigationProperty', ns):
            # Find ReferentialConstraint
            ref_constraint = nav_prop.find('edm:ReferentialConstraint', ns)
            if ref_constraint is None:
                continue

            column = ref_constraint.get('Property')
            referenced_column = ref_constraint.get('ReferencedProperty')

            if not column or not referenced_column:
                continue

            # Extract referenced table from Type attribute
            # Format: "mscrm.systemuser" or "Collection(mscrm.systemuser)"
            type_attr = nav_prop.get('Type', '')

            # Remove "Collection(" wrapper if present
            if type_attr.startswith('Collection('):
                type_attr = type_attr[11:-1]  # Remove "Collection(" and ")"

            # Extract entity name (after namespace prefix)
            if '.' in type_attr:
                referenced_table = type_attr.split('.')[-1]
            else:
                referenced_table = type_attr

            if not referenced_table:
                continue

            fk = ForeignKeyMetadata(
                column=column,
                referenced_table=referenced_table,
                referenced_column=referenced_column
            )

            foreign_keys.append(fk)

        return foreign_keys
