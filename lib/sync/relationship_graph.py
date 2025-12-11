"""Relationship graph for filtered entity sync.

Parses $metadata to build a bidirectional graph of entity relationships,
enabling transitive closure ID extraction for filtered entities.
"""

from dataclasses import dataclass, field

from ..config import EntityConfig
from ..validation.metadata_parser import MetadataParser


@dataclass
class EntityRelationships:
    """Relationships for a single entity."""

    # Entities this entity references: (table, fk_column, referenced_column)
    # Example: [("vin_diseases", "_vin_disease_value", "vin_diseaseid"), ...]  # noqa: ERA001 - example for documentation
    references_to: list[tuple[str, str, str]] = field(default_factory=list)

    # Entities that reference this entity: (table, fk_column, referenced_column)
    # Example: [("vin_candidates", "_vin_disease_value", "vin_diseaseid"), ...]  # noqa: ERA001 - example for documentation
    referenced_by: list[tuple[str, str, str]] = field(default_factory=list)


class RelationshipGraph:
    """
    Bidirectional graph of entity relationships extracted from $metadata.

    Used for transitive closure ID extraction during filtered entity sync.
    """

    def __init__(self):
        """Initialize empty relationship graph."""
        self.relationships: dict[str, EntityRelationships] = {}

    @classmethod
    def build_from_metadata(
        cls,
        metadata_xml: str,
        entity_configs: list[EntityConfig],
    ) -> "RelationshipGraph":
        """
        Build relationship graph from $metadata XML.

        Args:
            metadata_xml: XML content from $metadata endpoint
            entity_configs: List of entity configurations from entities_config.json

        Returns:
            RelationshipGraph with bidirectional relationships

        Algorithm:
            1. Parse $metadata using MetadataParser to extract all entity schemas
            2. Build entity name mapping (api_name → singular name for lookup)
            3. For each entity schema:
                a. Extract foreign keys (NavigationProperty with ReferentialConstraint)
                b. For each FK: Record both directions (references_to + referenced_by)
            4. Filter to only entities in entity_configs
        """
        graph = cls()

        # Parse metadata
        parser = MetadataParser(target_db="sqlite")
        schemas = parser.parse_metadata_xml(metadata_xml)

        # Build mapping: api_name → singular name (for Dataverse schema lookup)
        # e.g., "accounts" → "account", "vin_candidates" → "vin_candidate"
        entity_map = {config.api_name: config.name for config in entity_configs}

        # Also build reverse map: singular → api_name
        name_to_api = {config.name: config.api_name for config in entity_configs}

        # Initialize relationships for all configured entities
        for api_name in entity_map:
            graph.relationships[api_name] = EntityRelationships()

        # Build bidirectional relationships
        for api_name, singular_name in entity_map.items():
            # Get schema for this entity (using singular name from $metadata)
            if singular_name not in schemas:
                continue

            schema = schemas[singular_name]

            # Process foreign keys
            for fk in schema.foreign_keys:
                # This entity references another entity
                # e.g., accounts._primarycontactid_value → contacts.contactid

                # Convert referenced table from singular to api_name
                referenced_api_name = name_to_api.get(fk.referenced_table)
                if not referenced_api_name:
                    # Referenced entity not in our config, skip
                    continue

                # Record: this entity references the other entity
                # Include referenced_column for SCD2 (business key, not surrogate key)
                graph.relationships[api_name].references_to.append((
                    referenced_api_name,
                    fk.column,
                    fk.referenced_column,
                ))

                # Record: other entity is referenced by this entity
                # Include referenced_column for SCD2 (business key, not surrogate key)
                graph.relationships[referenced_api_name].referenced_by.append((
                    api_name,
                    fk.column,
                    fk.referenced_column,
                ))

        return graph

    def get_entities_that_reference(self, entity_api_name: str) -> list[tuple[str, str, str]]:
        """
        Get all entities that reference the given entity.

        Args:
            entity_api_name: Entity API name (e.g., 'accounts')

        Returns:
            List of (table_name, fk_column, referenced_column) tuples that reference this entity
            e.g., [('vin_candidates', '_accountid_value', 'accountid'), ...]
        """
        if entity_api_name not in self.relationships:
            return []
        return self.relationships[entity_api_name].referenced_by

    def get_entities_referenced_by(self, entity_api_name: str) -> list[tuple[str, str, str]]:
        """
        Get all entities that this entity references.

        Args:
            entity_api_name: Entity API name (e.g., 'accounts')

        Returns:
            List of (table_name, fk_column, referenced_column) tuples this entity references
            e.g., [('contacts', '_primarycontactid_value', 'contactid'), ...]
        """
        if entity_api_name not in self.relationships:
            return []
        return self.relationships[entity_api_name].references_to

    def __repr__(self) -> str:
        """String representation for debugging."""
        lines = ["RelationshipGraph:"]
        for entity, rels in sorted(self.relationships.items()):
            lines.append(f"  {entity}:")
            if rels.references_to:
                lines.append(f"    references_to: {rels.references_to}")
            if rels.referenced_by:
                lines.append(f"    referenced_by: {rels.referenced_by}")
        return "\n".join(lines)
