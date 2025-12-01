"""Fetch and extract Dataverse entity schemas from $metadata."""
from typing import Dict, List
from ..type_mapping import TableSchema
from ..dataverse_client import DataverseClient
from .metadata_parser import MetadataParser


class DataverseSchemaFetcher:
    """Fetches and extracts entity schemas from Dataverse $metadata."""

    def __init__(self, client: DataverseClient, target_db: str = 'sqlite'):
        """
        Initialize schema fetcher.

        Args:
            client: Authenticated DataverseClient
            target_db: Target database type ('sqlite' or 'postgresql')
        """
        self.client = client
        self.target_db = target_db
        self.parser = MetadataParser(target_db=target_db)

    async def fetch_schemas_from_metadata(
        self,
        entity_names: List[str]
    ) -> Dict[str, TableSchema]:
        """
        Fetch schemas for specified entities from $metadata.

        This method:
        1. Fetches the complete $metadata XML (~7 MB, 800+ entities)
        2. Parses all entity schemas
        3. Filters to only the requested entity names
        4. Returns schemas mapped to database types

        Args:
            entity_names: List of entity names to fetch (logical names, singular)

        Returns:
            Dict mapping entity name to TableSchema

        Raises:
            RuntimeError: If metadata fetch or parsing fails
        """
        # Fetch $metadata XML
        print("Fetching $metadata from Dataverse...")
        metadata_xml = await self.client.get_metadata()
        print(f"Fetched $metadata ({len(metadata_xml)} bytes)")

        # Parse all schemas
        print("Parsing metadata XML...")
        all_schemas = self.parser.parse_metadata_xml(metadata_xml)
        print(f"Parsed {len(all_schemas)} entity schemas")

        # Filter to requested entities
        requested_schemas = {}
        missing_entities = []

        for entity_name in entity_names:
            if entity_name in all_schemas:
                requested_schemas[entity_name] = all_schemas[entity_name]
            else:
                missing_entities.append(entity_name)

        if missing_entities:
            print(f"Warning: {len(missing_entities)} entities not found in metadata:")
            for entity in missing_entities[:10]:  # Show first 10
                print(f"  - {entity}")
            if len(missing_entities) > 10:
                print(f"  ... and {len(missing_entities) - 10} more")

        print(f"Extracted schemas for {len(requested_schemas)} entities")

        return requested_schemas

    async def fetch_all_schemas(self) -> Dict[str, TableSchema]:
        """
        Fetch schemas for ALL entities in $metadata.

        This is useful for exploration and debugging.

        Returns:
            Dict mapping entity name to TableSchema

        Raises:
            RuntimeError: If metadata fetch or parsing fails
        """
        # Fetch $metadata XML
        print("Fetching $metadata from Dataverse...")
        metadata_xml = await self.client.get_metadata()
        print(f"Fetched $metadata ({len(metadata_xml)} bytes)")

        # Parse all schemas
        print("Parsing metadata XML...")
        all_schemas = self.parser.parse_metadata_xml(metadata_xml)
        print(f"Parsed {len(all_schemas)} entity schemas")

        return all_schemas

    async def fetch_metadata_xml(self) -> str:
        """
        Fetch raw $metadata XML from Dataverse.

        Returns:
            Raw XML string from $metadata endpoint

        Raises:
            RuntimeError: If metadata fetch fails
        """
        return await self.client.get_metadata()
