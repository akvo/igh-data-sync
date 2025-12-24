"""Basic tests for schema validator."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from lib.config import EntityConfig
from lib.dataverse_client import DataverseClient
from lib.sync.database import DatabaseManager
from lib.validation.validator import validate_schema_before_sync


@pytest.fixture
def test_entity():
    """Create test entity configuration."""
    return EntityConfig(
        name="account",
        api_name="accounts",
        filtered=False,
        description="Test account entity",
    )


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
        with DatabaseManager(temp_db) as db_manager:
            db_manager.init_sync_tables()

            # Run validation
            valid_entities, entities_to_create, _diffs = await validate_schema_before_sync(
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
