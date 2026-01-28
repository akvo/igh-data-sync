"""Fake DataverseClient for E2E testing without real API calls."""

from typing import Any, Optional, Union


class FakeDataverseClient:
    """
    Test double for DataverseClient that returns canned responses.

    Implements the same interface as DataverseClient but with configurable
    responses for testing. Only mocks external HTTP calls - all business
    logic runs normally.
    """

    def __init__(self, config, token, max_concurrent=50):
        self.config = config
        self.token = token
        self.max_concurrent = max_concurrent
        self._metadata_response = ""
        self._entity_responses = {}
        self._entity_counts = {}
        self.session = None  # Track session state

    def set_metadata_response(self, xml: str):
        """Set canned $metadata response."""
        self._metadata_response = xml

    def set_entity_response(self, entity_name: str, records: list[dict]):
        """Set canned response for entity fetch_all_pages()."""
        self._entity_responses[entity_name] = records
        self._entity_counts[entity_name] = len(records)

    async def __aenter__(self):
        """Context manager entry (simulate session creation)."""
        self.session = "fake-session"
        return self

    async def __aexit__(self, *args):
        """Context manager exit (simulate session cleanup)."""
        self.session = None

    async def get_metadata(self) -> str:
        """Return canned metadata XML."""
        return self._metadata_response

    async def fetch_all_pages(
        self,
        entity_name: str,
        orderby: Optional[str] = None,  # noqa: ARG002 - part of API contract, unused in fake
        filter_query: Optional[str] = None,
        select: Optional[str] = None,  # noqa: ARG002 - part of API contract, unused in fake
    ) -> list[dict[str, Any]]:
        """Return canned entity records."""
        records = self._entity_responses.get(entity_name, [])

        if filter_query:
            # Handle modifiedon filters (incremental sync)
            if "modifiedon gt" in filter_query:
                # Extract timestamp and filter records
                timestamp = filter_query.split("modifiedon gt ")[1].strip()
                records = [r for r in records if r.get("modifiedon", "") > timestamp]

            # Handle ID-based filters (filtered sync)
            # Pattern: "accountid eq 'a1'" or "accountid eq 'a1' or accountid eq 'a2' or ..."  # noqa: ERA001 - example pattern for reference
            elif " eq " in filter_query:
                # Extract field name (e.g., "accountid")
                field_name = filter_query.split(" eq ", maxsplit=1)[0].strip()

                # Extract all IDs from the filter
                # Split by " or " and extract the value from each part
                parts = filter_query.split(" or ")
                allowed_ids = set()
                for part in parts:
                    if " eq " in part:
                        # Extract value between quotes: "accountid eq 'a1'" -> 'a1'
                        value = part.split(" eq ")[1].strip().strip("'\"")
                        allowed_ids.add(value)

                # Filter records to only include those with matching IDs
                records = [r for r in records if r.get(field_name) in allowed_ids]

        return records

    async def get_entity_count(self, entity_name: str) -> int:
        """Return count of canned records."""
        return self._entity_counts.get(entity_name, 0)

    async def get(self, endpoint: str) -> Union[dict, str]:
        """Generic GET method."""
        if endpoint == "$metadata":
            return await self.get_metadata()
        # Handle other endpoints as needed
        return {"value": []}
