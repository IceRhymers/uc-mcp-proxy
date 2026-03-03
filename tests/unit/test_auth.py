"""Tests for DatabricksAuth httpx.Auth subclass."""

from __future__ import annotations

import httpx
import pytest

pytestmark = pytest.mark.unit


class TestDatabricksAuth:
    """Test the DatabricksAuth httpx.Auth subclass."""

    def test_sync_auth_flow_injects_bearer_token(self, mock_workspace_client):
        """sync_auth_flow should inject the Bearer token from authenticate()."""
        from uc_mcp_proxy import DatabricksAuth

        auth = DatabricksAuth(mock_workspace_client)
        request = httpx.Request("POST", "https://example.com/mcp")
        flow = auth.sync_auth_flow(request)

        authed_request = next(flow)
        assert authed_request.headers["Authorization"] == "Bearer test-oauth-token"

    @pytest.mark.anyio
    async def test_async_auth_flow_injects_bearer_token(self, mock_workspace_client):
        """async_auth_flow should inject the Bearer token from authenticate()."""
        from uc_mcp_proxy import DatabricksAuth

        auth = DatabricksAuth(mock_workspace_client)
        request = httpx.Request("POST", "https://example.com/mcp")
        flow = auth.async_auth_flow(request)

        authed_request = await flow.__anext__()
        assert authed_request.headers["Authorization"] == "Bearer test-oauth-token"

    def test_each_request_gets_fresh_token(self, mock_workspace_client):
        """authenticate() should be called per-request, not cached."""
        mock_workspace_client.config.authenticate.side_effect = [
            {"Authorization": "Bearer token-1"},
            {"Authorization": "Bearer token-2"},
        ]

        from uc_mcp_proxy import DatabricksAuth

        auth = DatabricksAuth(mock_workspace_client)

        r1 = httpx.Request("POST", "https://example.com/mcp")
        next(auth.sync_auth_flow(r1))
        assert r1.headers["Authorization"] == "Bearer token-1"

        r2 = httpx.Request("POST", "https://example.com/mcp")
        next(auth.sync_auth_flow(r2))
        assert r2.headers["Authorization"] == "Bearer token-2"

        assert mock_workspace_client.config.authenticate.call_count == 2

    def test_sync_flow_completes_without_retry(self, mock_workspace_client):
        """After sending a 200 response, the flow should complete (StopIteration)."""
        from uc_mcp_proxy import DatabricksAuth

        auth = DatabricksAuth(mock_workspace_client)
        request = httpx.Request("POST", "https://example.com/mcp")
        flow = auth.sync_auth_flow(request)

        next(flow)  # Get the authed request
        with pytest.raises(StopIteration):
            flow.send(httpx.Response(200))

    def test_handles_expired_refresh_token(self, mock_workspace_client):
        """When authenticate() raises, the error should propagate."""
        from databricks.sdk.errors import PermissionDenied

        mock_workspace_client.config.authenticate.side_effect = PermissionDenied(
            "Token expired. Run: databricks auth login --profile DEFAULT"
        )

        from uc_mcp_proxy import DatabricksAuth

        auth = DatabricksAuth(mock_workspace_client)
        request = httpx.Request("POST", "https://example.com/mcp")
        flow = auth.sync_auth_flow(request)

        with pytest.raises(PermissionDenied):
            next(flow)
