"""OAuth authentication for Dataverse API."""
import re
import requests
from typing import Optional
from .config import Config


class DataverseAuth:
    """Handles OAuth authentication with Microsoft Dataverse."""

    def __init__(self, config: Config):
        """
        Initialize authentication handler.

        Args:
            config: Configuration with API URL and credentials
        """
        self.config = config
        self.tenant_id: Optional[str] = None
        self.token: Optional[str] = None

    def discover_tenant_id(self) -> str:
        """
        Discover tenant ID from WWW-Authenticate header.

        Makes an unauthenticated request to the Dataverse API and extracts
        the tenant ID from the WWW-Authenticate challenge header.

        Returns:
            The tenant ID (GUID)

        Raises:
            RuntimeError: If tenant ID cannot be discovered
        """
        try:
            # Make unauthenticated request to trigger WWW-Authenticate header
            response = requests.get(
                self.config.api_url,
                headers={'Accept': 'application/json'},
                timeout=10
            )

            # Look for WWW-Authenticate header
            www_auth = response.headers.get('WWW-Authenticate', '')

            if not www_auth:
                raise RuntimeError("No WWW-Authenticate header found in response")

            # Extract tenant ID from authorization_uri
            # Format: Bearer authorization_uri="https://login.microsoftonline.com/TENANT_ID/oauth2/authorize"
            match = re.search(r'authorization_uri="[^"]*?/([0-9a-f\-]{36})/oauth2', www_auth, re.IGNORECASE)

            if not match:
                raise RuntimeError(f"Could not extract tenant ID from WWW-Authenticate header: {www_auth}")

            tenant_id = match.group(1)
            return tenant_id

        except requests.RequestException as e:
            raise RuntimeError(f"Failed to discover tenant ID: {e}")

    def authenticate(self) -> str:
        """
        Authenticate with Microsoft identity platform and obtain access token.

        Returns:
            Access token for Dataverse API

        Raises:
            RuntimeError: If authentication fails
        """
        # Step 1: Discover tenant ID
        if not self.tenant_id:
            self.tenant_id = self.discover_tenant_id()

        # Step 2: Request token from Microsoft identity platform
        token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"

        token_data = {
            'client_id': self.config.client_id,
            'client_secret': self.config.client_secret,
            'scope': self.config.scope,
            'grant_type': 'client_credentials'
        }

        try:
            response = requests.post(token_url, data=token_data, timeout=30)
            response.raise_for_status()

            token_response = response.json()
            access_token = token_response.get('access_token')

            if not access_token:
                raise RuntimeError("No access_token in authentication response")

            self.token = access_token
            return access_token

        except requests.RequestException as e:
            raise RuntimeError(f"Authentication failed: {e}")
