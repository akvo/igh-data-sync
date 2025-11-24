"""Async HTTP client for Dataverse API."""
import aiohttp
from typing import Optional, Dict, Union
from .config import Config


class DataverseClient:
    """Async HTTP client for Dataverse Web API."""

    def __init__(self, config: Config, access_token: str):
        """
        Initialize Dataverse client.

        Args:
            config: Configuration with API URL
            access_token: OAuth access token
        """
        self.config = config
        self.access_token = access_token
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()

    async def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, str]] = None
    ) -> Union[Dict, str]:
        """
        Make GET request to Dataverse API.

        CRITICAL: Automatically detects $metadata endpoint and uses
        application/xml Accept header. For other endpoints uses
        application/json.

        Args:
            endpoint: API endpoint (e.g., '$metadata', 'accounts')
            params: Optional query parameters

        Returns:
            For JSON endpoints: Dict with parsed JSON
            For XML endpoints: String with XML content

        Raises:
            RuntimeError: If request fails
        """
        if not self.session:
            raise RuntimeError("Client not initialized. Use 'async with' context manager.")

        # Construct full URL
        if endpoint.startswith('http'):
            url = endpoint
        else:
            url = f"{self.config.api_url}/{endpoint}"

        # CRITICAL: Detect $metadata endpoint and set Accept header accordingly
        if '$metadata' in endpoint:
            accept_header = 'application/xml'
        else:
            accept_header = 'application/json'

        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': accept_header,
            'OData-MaxVersion': '4.0',
            'OData-Version': '4.0'
        }

        try:
            async with self.session.get(url, headers=headers, params=params) as response:
                # Check for errors
                if response.status != 200:
                    error_text = await response.text()
                    raise RuntimeError(
                        f"API request failed with status {response.status}: {error_text}"
                    )

                # Return XML as text, JSON as dict
                if accept_header == 'application/xml':
                    return await response.text()
                else:
                    return await response.json()

        except aiohttp.ClientError as e:
            raise RuntimeError(f"HTTP request failed: {e}")

    async def get_metadata(self) -> str:
        """
        Fetch $metadata XML document.

        This is a convenience method that explicitly fetches the OData
        metadata document containing all entity schemas.

        Returns:
            XML string with complete OData CSDL metadata

        Raises:
            RuntimeError: If request fails
        """
        return await self.get('$metadata')

    async def get_entity_count(self, entity_name: str) -> int:
        """
        Get count of records for an entity.

        Args:
            entity_name: Name of entity (e.g., 'vin_candidate')

        Returns:
            Count of records

        Raises:
            RuntimeError: If request fails
        """
        # Use $count endpoint
        endpoint = f"{entity_name}/$count"
        count_str = await self.get(endpoint)

        try:
            return int(count_str)
        except (ValueError, TypeError):
            raise RuntimeError(f"Invalid count response: {count_str}")
