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

        # Parse foreign keys using unified detection
        # (NavigationProperty + pattern matching for _*_value and *id columns)
        foreign_keys = self._parse_all_foreign_keys(entity_elem, ns, columns, primary_key)

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

    def _parse_all_foreign_keys(
        self,
        entity_elem: ET.Element,
        ns: Dict[str, str],
        columns: List[ColumnMetadata],
        primary_key: Optional[str]
    ) -> List[ForeignKeyMetadata]:
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

        # STEP 2: Track which columns already have FK metadata
        columns_with_fks = {fk.column for fk in foreign_keys}

        # STEP 3: Pattern-match remaining columns for inferred FKs
        for col in columns:
            # Skip if this column already has FK metadata from NavigationProperty
            if col.name in columns_with_fks:
                continue

            col_name = col.name.lower()

            # Pattern 1: _fieldname_value (Dataverse lookup fields)
            # Example: _createdby_value, _primarycontactid_value, _owninguser_value
            if col_name.startswith('_') and col_name.endswith('_value'):
                # Strip _ prefix and _value suffix to get field name
                # _createdby_value → createdby
                fieldname = col.name[1:-6]  # Remove _ and _value

                fk = ForeignKeyMetadata(
                    column=col.name,
                    referenced_table=fieldname,
                    referenced_column=f"{fieldname}id"
                )

                foreign_keys.append(fk)
                continue  # Move to next column

            # Pattern 2: *id (junction table columns and simple references)
            # Example: accountid, vin_candidateid, vin_clinicaltrialid
            if col_name.endswith('id'):
                # Skip primary key
                if primary_key and col.name == primary_key:
                    continue

                # Skip versionnumber
                if col.name == 'versionnumber':
                    continue

                # Strip 'id' suffix to get referenced table name
                # accountid → account
                # vin_candidateid → vin_candidate
                referenced_table = col.name[:-2]  # Remove 'id'

                fk = ForeignKeyMetadata(
                    column=col.name,
                    referenced_table=referenced_table,
                    referenced_column=col.name
                )

                foreign_keys.append(fk)

        return foreign_keys
