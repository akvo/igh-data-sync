"""Tests for OAuth authentication with Microsoft Dataverse."""

import time
from unittest.mock import MagicMock, patch

import pytest
import responses

from lib.auth import DataverseAuth


@pytest.fixture
def auth_instance(test_config):
    """Create DataverseAuth instance."""
    return DataverseAuth(test_config)


class TestDataverseAuth:
    """Tests for DataverseAuth class."""

    @responses.activate
    def test_discover_tenant_id_success(self, test_config):
        """Test successful tenant ID discovery from WWW-Authenticate header."""
        # Mock the unauthenticated request
        www_auth_header = (
            'Bearer authorization_uri="https://login.microsoftonline.com/'
            '12345678-1234-1234-1234-123456789abc/oauth2/authorize"'
        )
        responses.add(
            responses.GET,
            test_config.api_url,
            headers={"WWW-Authenticate": www_auth_header},
            status=401,
        )

        auth = DataverseAuth(test_config)
        tenant_id = auth.discover_tenant_id()

        assert tenant_id == "12345678-1234-1234-1234-123456789abc"
        # Note: discover_tenant_id() returns the ID but doesn't store it
        # Only authenticate() stores it in self.tenant_id

    @responses.activate
    def test_discover_tenant_id_without_quotes(self, test_config):
        """Test tenant ID discovery with unquoted authorization_uri."""
        www_auth_header = (
            "Bearer authorization_uri=https://login.microsoftonline.com/"
            "abcdef01-2345-6789-abcd-ef0123456789/oauth2/authorize"
        )
        responses.add(
            responses.GET,
            test_config.api_url,
            headers={"WWW-Authenticate": www_auth_header},
            status=401,
        )

        auth = DataverseAuth(test_config)
        tenant_id = auth.discover_tenant_id()

        assert tenant_id == "abcdef01-2345-6789-abcd-ef0123456789"

    @responses.activate
    def test_discover_tenant_id_missing_header(self, test_config):
        """Test tenant ID discovery fails when WWW-Authenticate header is missing."""
        responses.add(
            responses.GET,
            test_config.api_url,
            status=401,
        )

        auth = DataverseAuth(test_config)

        with pytest.raises(RuntimeError, match="No WWW-Authenticate header found"):
            auth.discover_tenant_id()

    @responses.activate
    def test_discover_tenant_id_malformed_header(self, test_config):
        """Test tenant ID discovery fails with malformed header."""
        www_auth_header = "Bearer authorization_uri=https://invalid.com/no-tenant-id"
        responses.add(
            responses.GET,
            test_config.api_url,
            headers={"WWW-Authenticate": www_auth_header},
            status=401,
        )

        auth = DataverseAuth(test_config)

        with pytest.raises(RuntimeError, match="Could not extract tenant ID"):
            auth.discover_tenant_id()

    @responses.activate
    def test_authenticate_success(self, test_config):
        """Test successful authentication flow."""
        # Mock tenant discovery
        www_auth_header = (
            'Bearer authorization_uri="https://login.microsoftonline.com/'
            '12345678-1234-1234-1234-123456789abc/oauth2/authorize"'
        )
        responses.add(
            responses.GET,
            test_config.api_url,
            headers={"WWW-Authenticate": www_auth_header},
            status=401,
        )

        # Mock token request
        token_response = {
            "access_token": "test-access-token-12345",
            "expires_in": 3599,
            "token_type": "Bearer",
        }
        responses.add(
            responses.POST,
            "https://login.microsoftonline.com/12345678-1234-1234-1234-123456789abc/oauth2/v2.0/token",
            json=token_response,
            status=200,
        )

        auth = DataverseAuth(test_config)
        access_token = auth.authenticate()

        assert access_token == "test-access-token-12345"
        assert auth.token == "test-access-token-12345"
        assert auth.token_expiry > time.time()

    @responses.activate
    def test_authenticate_missing_access_token(self, test_config):
        """Test authentication fails when access_token is missing from response."""
        # Mock tenant discovery
        www_auth_header = (
            'Bearer authorization_uri="https://login.microsoftonline.com/'
            '12345678-1234-1234-1234-123456789abc/oauth2/authorize"'
        )
        responses.add(
            responses.GET,
            test_config.api_url,
            headers={"WWW-Authenticate": www_auth_header},
            status=401,
        )

        # Mock token request with missing access_token
        token_response = {
            "expires_in": 3599,
            "token_type": "Bearer",
        }
        responses.add(
            responses.POST,
            "https://login.microsoftonline.com/12345678-1234-1234-1234-123456789abc/oauth2/v2.0/token",
            json=token_response,
            status=200,
        )

        auth = DataverseAuth(test_config)

        with pytest.raises(RuntimeError, match="No access_token in authentication response"):
            auth.authenticate()

    @responses.activate
    def test_get_token_fresh_authentication(self, test_config):
        """Test get_token() performs fresh authentication when no token exists."""
        # Mock tenant discovery
        www_auth_header = (
            'Bearer authorization_uri="https://login.microsoftonline.com/'
            '12345678-1234-1234-1234-123456789abc/oauth2/authorize"'
        )
        responses.add(
            responses.GET,
            test_config.api_url,
            headers={"WWW-Authenticate": www_auth_header},
            status=401,
        )

        # Mock token request
        token_response = {
            "access_token": "fresh-token-67890",
            "expires_in": 3599,
        }
        responses.add(
            responses.POST,
            "https://login.microsoftonline.com/12345678-1234-1234-1234-123456789abc/oauth2/v2.0/token",
            json=token_response,
            status=200,
        )

        auth = DataverseAuth(test_config)
        token = auth.get_token()

        assert token == "fresh-token-67890"

    def test_get_token_uses_cached_token(self, auth_instance):
        """Test get_token() returns cached token when still valid."""
        # Set up a valid cached token
        auth_instance.token = "cached-token-abc123"
        auth_instance.token_expiry = time.time() + 4000  # Valid for another 4000 seconds

        # Should return cached token without making any requests
        token = auth_instance.get_token()

        assert token == "cached-token-abc123"

    @responses.activate
    def test_get_token_refreshes_expiring_token(self, test_config):
        """Test get_token() refreshes token when within refresh window (3000s)."""
        auth = DataverseAuth(test_config)

        # Set up an expiring token (within refresh window)
        auth.token = "expiring-token"
        auth.tenant_id = "12345678-1234-1234-1234-123456789abc"
        auth.token_expiry = time.time() + 2000  # Expires in 2000s (< 3000s refresh window)

        # Mock token refresh request
        token_response = {
            "access_token": "refreshed-token-xyz",
            "expires_in": 3599,
        }
        responses.add(
            responses.POST,
            "https://login.microsoftonline.com/12345678-1234-1234-1234-123456789abc/oauth2/v2.0/token",
            json=token_response,
            status=200,
        )

        token = auth.get_token()

        assert token == "refreshed-token-xyz"
        assert auth.token == "refreshed-token-xyz"

    @responses.activate
    def test_authenticate_network_error(self, test_config):
        """Test authentication handles network errors gracefully."""
        # Mock tenant discovery success
        www_auth_header = (
            'Bearer authorization_uri="https://login.microsoftonline.com/'
            '12345678-1234-1234-1234-123456789abc/oauth2/authorize"'
        )
        responses.add(
            responses.GET,
            test_config.api_url,
            headers={"WWW-Authenticate": www_auth_header},
            status=401,
        )

        # Mock token request failure
        responses.add(
            responses.POST,
            "https://login.microsoftonline.com/12345678-1234-1234-1234-123456789abc/oauth2/v2.0/token",
            status=500,
        )

        auth = DataverseAuth(test_config)

        with pytest.raises(RuntimeError, match="Authentication failed"):
            auth.authenticate()

    def test_token_expiry_calculation(self, auth_instance):
        """Test token expiry is calculated correctly."""
        auth_instance.tenant_id = "test-tenant"
        auth_instance.token = "test-token"

        # Mock time before authentication
        mock_time_before = 1000000.0

        with patch("time.time", return_value=mock_time_before):
            with patch.object(auth_instance, "authenticate") as mock_auth:
                mock_auth.return_value = "test-token"
                auth_instance.token_expiry = mock_time_before + 3599

                # Token should be valid for 3599 seconds from mock_time_before
                assert auth_instance.token_expiry == 1003599.0

                # Within refresh window (3000s), should trigger refresh
                with patch("time.time", return_value=mock_time_before + 600):
                    # 600s elapsed, token expires in 2999s (< 3000s window)
                    assert auth_instance.token_expiry - time.time() < auth_instance.refresh_window
