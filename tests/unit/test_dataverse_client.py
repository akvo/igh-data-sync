"""Tests for Dataverse API client."""

import pytest
from aioresponses import aioresponses

from lib.dataverse_client import DataverseClient


class TestDataverseClient:
    """Tests for DataverseClient class."""

    @pytest.mark.asyncio
    async def test_context_manager(self, test_config, test_token):
        """Test async context manager creates and closes session."""
        async with DataverseClient(test_config, test_token) as client:
            assert client.session is not None

        # Session should be closed after exiting context
        assert client.session is None or client.session.closed

    @pytest.mark.asyncio
    async def test_get_json_endpoint(self, test_config, test_token):
        """Test GET request to JSON endpoint."""
        with aioresponses() as m:
            # Mock API response
            m.get(
                "https://test.crm.dynamics.com/api/data/v9.2/accounts",
                payload={"value": [{"accountid": "123", "name": "Test Account"}]},
                status=200,
            )

            async with DataverseClient(test_config, test_token) as client:
                result = await client.get("accounts")

                assert isinstance(result, dict)
                assert "value" in result
                assert len(result["value"]) == 1
                assert result["value"][0]["name"] == "Test Account"

    @pytest.mark.asyncio
    async def test_get_metadata_endpoint(self, test_config, test_token):
        """Test GET request to $metadata endpoint returns XML."""
        with aioresponses() as m:
            # Mock $metadata response
            metadata_xml = '<?xml version="1.0"?><edmx:Edmx></edmx:Edmx>'
            m.get(
                "https://test.crm.dynamics.com/api/data/v9.2/$metadata",
                body=metadata_xml,
                status=200,
                content_type="application/xml",
            )

            async with DataverseClient(test_config, test_token) as client:
                result = await client.get("$metadata")

                assert isinstance(result, str)
                assert result == metadata_xml

    @pytest.mark.asyncio
    async def test_get_metadata_convenience_method(self, test_config, test_token):
        """Test get_metadata() convenience method."""
        with aioresponses() as m:
            metadata_xml = '<?xml version="1.0"?><edmx:Edmx></edmx:Edmx>'
            m.get(
                "https://test.crm.dynamics.com/api/data/v9.2/$metadata",
                body=metadata_xml,
                status=200,
                content_type="application/xml",
            )

            async with DataverseClient(test_config, test_token) as client:
                result = await client.get_metadata()

                assert isinstance(result, str)
                assert "edmx:Edmx" in result

    @pytest.mark.asyncio
    async def test_get_entity_count(self, test_config, test_token):
        """Test getting entity record count."""
        with aioresponses() as m:
            # Mock $count endpoint
            m.get(
                "https://test.crm.dynamics.com/api/data/v9.2/accounts/$count",
                body="42",
                status=200,
            )

            async with DataverseClient(test_config, test_token) as client:
                count = await client.get_entity_count("accounts")

                assert count == 42

    @pytest.mark.asyncio
    async def test_get_entity_count_invalid_response(self, test_config, test_token):
        """Test get_entity_count handles invalid count responses."""
        with aioresponses() as m:
            # Mock invalid $count response (plain text, not JSON)
            m.get(
                "https://test.crm.dynamics.com/api/data/v9.2/accounts/$count",
                body="invalid-count",
                status=200,
                content_type="text/plain",
            )

            async with DataverseClient(test_config, test_token) as client:
                # The error happens when trying to parse text/plain as JSON
                with pytest.raises(RuntimeError, match="HTTP request failed.*text/plain"):
                    await client.get_entity_count("accounts")

    @pytest.mark.asyncio
    async def test_semaphore_limits_concurrency(self, test_config, test_token):
        """Test that semaphore is created with correct concurrency limit."""
        client = DataverseClient(test_config, test_token, max_concurrent=25)
        assert client.semaphore._value == 25

    @pytest.mark.asyncio
    async def test_retry_delays_configured(self, test_config, test_token):
        """Test retry delays are configured for exponential backoff."""
        client = DataverseClient(test_config, test_token)
        assert client.retry_delays == [1, 2, 4, 8, 16]

    @pytest.mark.asyncio
    async def test_fetch_with_retry_rate_limiting(self, test_config, test_token):
        """Test retry logic handles 429 rate limiting."""
        with aioresponses() as m:
            # Mock 429 response with Retry-After header
            m.get(
                "https://test.crm.dynamics.com/api/data/v9.2/accounts",
                status=429,
                headers={"Retry-After": "1"},
            )

            # Mock successful retry
            m.get(
                "https://test.crm.dynamics.com/api/data/v9.2/accounts",
                payload={"value": [{"accountid": "1"}]},
                status=200,
            )

            async with DataverseClient(test_config, test_token) as client:
                result = await client.fetch_with_retry(
                    "https://test.crm.dynamics.com/api/data/v9.2/accounts",
                )

                assert len(result["value"]) == 1

    @pytest.mark.asyncio
    async def test_fetch_with_retry_unauthorized(self, test_config, test_token):
        """Test retry logic handles 401 unauthorized."""
        with aioresponses() as m:
            # Mock 401 unauthorized response
            m.get(
                "https://test.crm.dynamics.com/api/data/v9.2/accounts",
                status=401,
            )

            async with DataverseClient(test_config, test_token) as client:
                with pytest.raises(RuntimeError, match="Token expired"):
                    await client.fetch_with_retry(
                        "https://test.crm.dynamics.com/api/data/v9.2/accounts",
                    )

    @pytest.mark.asyncio
    async def test_get_request_error_handling(self, test_config, test_token):
        """Test get() handles API errors properly."""
        with aioresponses() as m:
            # Mock API error
            m.get(
                "https://test.crm.dynamics.com/api/data/v9.2/accounts",
                payload={"error": {"message": "Invalid request"}},
                status=400,
            )

            async with DataverseClient(test_config, test_token) as client:
                with pytest.raises(RuntimeError, match="API request failed with status 400"):
                    await client.get("accounts")

    @pytest.mark.asyncio
    async def test_client_not_initialized_error(self, test_config, test_token):
        """Test error when using client without context manager."""
        client = DataverseClient(test_config, test_token)

        with pytest.raises(RuntimeError, match="Client not initialized"):
            await client.get("accounts")
