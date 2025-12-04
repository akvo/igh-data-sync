"""Async HTTP client for Dataverse API."""

import asyncio
from typing import Optional, Union

import aiohttp

from .config import Config

# HTTP Status codes
HTTP_OK = 200
HTTP_UNAUTHORIZED = 401
HTTP_TOO_MANY_REQUESTS = 429
HTTP_SERVER_ERROR = 500


class DataverseClient:
    """Async HTTP client for Dataverse Web API with retry, pagination, and concurrency control."""

    def __init__(self, config: Config, access_token: str, max_concurrent: int = 50):
        """
        Initialize Dataverse client.

        Args:
            config: Configuration with API URL
            access_token: OAuth access token
            max_concurrent: Maximum concurrent requests (default: 50)
        """
        self.config = config
        self.access_token = access_token
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.retry_delays = [1, 2, 4, 8, 16]  # Exponential backoff in seconds

    async def __aenter__(self):
        """Async context manager entry."""
        # Set generous timeouts for large $metadata XML downloads (~7 MB)
        timeout = aiohttp.ClientTimeout(total=600, connect=60, sock_read=300)
        self.session = aiohttp.ClientSession(timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()

    async def get(self, endpoint: str, params: Optional[dict[str, str]] = None) -> Union[dict, str]:
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
            msg = "Client not initialized. Use 'async with' context manager."
            raise RuntimeError(msg)

        # Construct full URL
        url = endpoint if endpoint.startswith("http") else f"{self.config.api_url}/{endpoint}"

        # CRITICAL: Detect $metadata endpoint and set Accept header accordingly
        accept_header = "application/xml" if "$metadata" in endpoint else "application/json"

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": accept_header,
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
        }

        try:
            async with self.session.get(url, headers=headers, params=params) as response:
                # Check for errors
                if response.status != HTTP_OK:
                    error_text = await response.text()
                    msg = f"API request failed with status {response.status}: {error_text}"
                    raise RuntimeError(
                        msg,
                    )

                # Return XML as text, JSON as dict
                if accept_header == "application/xml":
                    return await response.text()
                else:
                    return await response.json()

        except aiohttp.ClientError as e:
            msg = f"HTTP request failed: {e}"
            raise RuntimeError(msg) from e

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
        return await self.get("$metadata")

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
            msg = f"Invalid count response: {count_str}"
            raise RuntimeError(msg) from None

    async def fetch_with_retry(
        self,
        url: str,
        params: Optional[dict[str, str]] = None,
        attempt: int = 0,
    ) -> dict:
        """
        Fetch data with exponential backoff retry logic.

        Handles:
        - 429 rate limiting (respects Retry-After header)
        - 401 token refresh (raises for re-auth)
        - Timeouts and network errors
        - 5xx server errors

        Args:
            url: Full URL to fetch
            params: Query parameters
            attempt: Current retry attempt (0-indexed)

        Returns:
            JSON response as dict

        Raises:
            RuntimeError: If all retries exhausted or unrecoverable error
        """
        async with self.semaphore:  # Limit concurrent requests
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Accept": "application/json",
                "OData-MaxVersion": "4.0",
                "OData-Version": "4.0",
                "Prefer": "odata.maxpagesize=5000,odata.include-annotations=\"OData.Community.Display.V1.FormattedValue\"",
            }

            try:
                async with self.session.get(
                    url,
                    headers=headers,
                    params=params,
                ) as response:
                    # Handle 429 rate limiting
                    if response.status == HTTP_TOO_MANY_REQUESTS:
                        retry_after = response.headers.get(
                            "Retry-After",
                            self.retry_delays[min(attempt, len(self.retry_delays) - 1)],
                        )
                        try:
                            wait_time = int(retry_after)
                        except ValueError:
                            wait_time = self.retry_delays[min(attempt, len(self.retry_delays) - 1)]

                        if attempt < len(self.retry_delays):
                            await asyncio.sleep(wait_time)
                            return await self.fetch_with_retry(url, params, attempt + 1)
                        else:
                            msg = f"Rate limited after {attempt + 1} attempts"
                            raise RuntimeError(msg)

                    # Handle 401 unauthorized (token expired)
                    if response.status == HTTP_UNAUTHORIZED:
                        msg = "Token expired - need to re-authenticate"
                        raise RuntimeError(msg)

                    # Handle 5xx server errors with retry
                    if response.status >= HTTP_SERVER_ERROR:
                        if attempt < len(self.retry_delays):
                            await asyncio.sleep(self.retry_delays[attempt])
                            return await self.fetch_with_retry(url, params, attempt + 1)
                        else:
                            error_text = await response.text()
                            msg = (
                                f"Server error after {attempt + 1} attempts: "
                                f"{response.status} - {error_text}"
                            )
                            raise RuntimeError(
                                msg,
                            )

                    # Handle other errors
                    if response.status != HTTP_OK:
                        error_text = await response.text()
                        msg = f"API request failed: {response.status} - {error_text}"
                        raise RuntimeError(msg)

                    return await response.json()

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                # Retry on network errors
                if attempt < len(self.retry_delays):
                    await asyncio.sleep(self.retry_delays[attempt])
                    return await self.fetch_with_retry(url, params, attempt + 1)
                else:
                    msg = f"Network error after {attempt + 1} attempts: {e}"
                    raise RuntimeError(msg) from e

    async def fetch_all_pages(
        self,
        entity_name: str,
        orderby: Optional[str] = None,
        filter_query: Optional[str] = None,
        select: Optional[str] = None,
    ) -> list[dict]:
        """
        Fetch all pages of data for an entity using @odata.nextLink pagination.

        Implements fallback logic for entities that don't support orderby on certain fields:
        1. Try with provided orderby field
        2. If 400 error mentions "orderby" or "attribute", fall back to no orderby
        3. No orderby mode limited to max 5000 records per page

        Args:
            entity_name: Entity name (plural, e.g., 'vin_candidates')
            orderby: Column to order by (required for deterministic paging)
            filter_query: OData $filter expression
            select: OData $select expression (comma-separated columns)

        Returns:
            List of all records across all pages

        Raises:
            RuntimeError: If request fails
        """
        # Try with orderby first
        if orderby:
            try:
                return await self._fetch_pages_with_orderby(
                    entity_name,
                    orderby,
                    filter_query,
                    select,
                )
            except RuntimeError as e:
                # Check if it's an orderby-related 400 error
                error_str = str(e).lower()
                if "400" in error_str and (
                    "orderby" in error_str or "attribute" in error_str or "principal" in error_str
                ):
                    print(f"    ⚠️  Cannot order by {orderby}, fetching without orderby...")
                    # Fall through to no-orderby mode
                else:
                    # Different error, propagate it
                    raise

        # Fallback: fetch without orderby (WARNING: limited to one page, max 5000 records)
        if orderby:  # Only warn if we had an orderby but it failed
            print(f"    ⚠️  Fetching {entity_name} without pagination (max 5000 records)")

        return await self._fetch_pages_without_orderby(entity_name, filter_query, select)

    async def _fetch_pages_with_orderby(
        self,
        entity_name: str,
        orderby: str,
        filter_query: Optional[str] = None,
        select: Optional[str] = None,
    ) -> list[dict]:
        """Fetch all pages with orderby for deterministic pagination."""
        all_records = []

        # Build initial query parameters
        params = {"$orderby": orderby}
        if filter_query:
            params["$filter"] = filter_query
        if select:
            params["$select"] = select

        # Start with first page
        url = f"{self.config.api_url}/{entity_name}"

        page_num = 1
        while url:
            # Fetch page
            response = await self.fetch_with_retry(url, params if page_num == 1 else None)

            # Extract records
            records = response.get("value", [])
            all_records.extend(records)

            # Get next page URL
            url = response.get("@odata.nextLink")
            page_num += 1

        return all_records

    async def _fetch_pages_without_orderby(
        self,
        entity_name: str,
        filter_query: Optional[str] = None,
        select: Optional[str] = None,
    ) -> list[dict]:
        """Fetch without orderby (fallback mode, limited to max 5000 records per page)."""
        # Build query parameters (no orderby)
        params = {}
        if filter_query:
            params["$filter"] = filter_query
        if select:
            params["$select"] = select

        # Fetch single page
        url = f"{self.config.api_url}/{entity_name}"
        response = await self.fetch_with_retry(url, params if params else None)

        # Extract records
        records = response.get("value", [])

        # Check if there's a next page (shouldn't be without orderby, but check anyway)
        if response.get("@odata.nextLink"):
            print(
                f"    ⚠️  Warning: {entity_name} has more records but orderby failed. "
                f"Only first 5000 fetched.",
            )

        return records
