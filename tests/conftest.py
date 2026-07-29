"""Shared fixtures for uc-mcp-proxy tests."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import MagicMock

import anyio
import pytest

# ---------------------------------------------------------------------------
# Databricks SDK fixtures
# ---------------------------------------------------------------------------


#: The workspace host both client fixtures resolve to. The token-exchange
#: endpoint is derived from this, never from the app host.
HOST = "https://test-workspace.cloud.databricks.com"

#: The PAT ``mock_workspace_client_pat`` hands out. Deliberately distinctive so
#: that "this string reached no header / no message" assertions are non-vacuous:
#: a generic value could match by accident and pass while leaking.
FAKE_PAT = "dapi-fake-pat-DO-NOT-LOG"


@pytest.fixture
def mock_workspace_client():
    """Mock WorkspaceClient with OAuth token access.

    Simulates the SDK's authenticate() method which returns
    fresh auth headers on each call.
    """
    client = MagicMock()
    client.config.host = HOST
    client.config.authenticate.return_value = {"Authorization": "Bearer test-oauth-token"}
    # Real strings, not MagicMocks: error messages interpolate these, and a
    # bare MagicMock is truthy so it would render as "<MagicMock id=...>".
    client.config.profile = "test-profile"
    # An OAuth token, so the auth_type must name an OAuth flow. Declaring "pat"
    # here while handing out a bearer OAuth token described a profile that
    # cannot exist, and the omit-the-forwarded-header rule keyed on auth_type
    # turns that contradiction into a KeyError. PAT coverage now lives on
    # ``mock_workspace_client_pat`` -- wire that in rather than flipping this
    # one back.
    client.config.auth_type = "databricks-cli"
    return client


@pytest.fixture
def mock_workspace_client_pat():
    """Mock WorkspaceClient for a PAT profile — the token-exchange subject."""
    client = MagicMock()
    client.config.host = HOST
    client.config.authenticate.return_value = {"Authorization": f"Bearer {FAKE_PAT}"}
    client.config.profile = "test-profile"
    client.config.auth_type = "pat"
    return client


@pytest.fixture
def exchange_config():
    """An ``ExchangeConfig`` pointing at the fixture workspace host."""
    from uc_mcp_proxy.token_exchange import ExchangeConfig

    return ExchangeConfig(
        host=HOST,
        client_id="00000000-1111-2222-3333-444444444444",
        scopes=("sql", "dashboards.genie"),
        verify_ssl=True,
    )


# ---------------------------------------------------------------------------
# Stream fixtures (real anyio memory streams, not mocks)
# ---------------------------------------------------------------------------


@pytest.fixture
def memory_stream_pair():
    """Factory: create a (send, recv) memory object stream pair.

    Usage:
        send, recv = memory_stream_pair(buffer=16)
    """

    def _make(buffer: int = 16):
        return anyio.create_memory_object_stream(buffer)

    return _make


@pytest.fixture
def stdio_streams(memory_stream_pair):
    """Simulated stdio transport streams.

    Returns (test_send, proxy_read, proxy_write, test_recv):
    - test_send: test writes here to simulate Claude Code input
    - proxy_read: proxy reads from here (stdio read side)
    - proxy_write: proxy writes here (stdio write side)
    - test_recv: test reads here to verify proxy output to Claude Code
    """
    test_send, proxy_read = memory_stream_pair()
    proxy_write, test_recv = memory_stream_pair()
    return test_send, proxy_read, proxy_write, test_recv


@pytest.fixture
def http_streams(memory_stream_pair):
    """Simulated HTTP transport streams.

    Returns (test_send, proxy_read, proxy_write, test_recv):
    - test_send: test writes here to simulate remote server responses
    - proxy_read: proxy reads from here (HTTP read side)
    - proxy_write: proxy writes here (HTTP write side)
    - test_recv: test reads here to verify proxy sent to remote server
    """
    test_send, proxy_read = memory_stream_pair()
    proxy_write, test_recv = memory_stream_pair()
    return test_send, proxy_read, proxy_write, test_recv


# ---------------------------------------------------------------------------
# Transport mock fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_stdio_server(stdio_streams):
    """Async context manager replacing mcp.server.stdio.stdio_server.

    Yields (read_stream, write_stream) from the proxy's perspective.
    """
    _, proxy_read, proxy_write, _ = stdio_streams

    @asynccontextmanager
    async def _mock():
        yield (proxy_read, proxy_write)

    return _mock


@pytest.fixture
def mock_http_client(http_streams):
    """Async context manager replacing mcp.client.streamable_http.streamablehttp_client.

    Yields (read_stream, write_stream, get_session_id) from the proxy's perspective.
    Captures the auth kwarg for auth verification.
    """
    _, proxy_read, proxy_write, _ = http_streams
    captured = {}

    @asynccontextmanager
    async def _mock(url, *, headers=None, auth=None, terminate_on_close=True, **kwargs):
        captured["url"] = url
        captured["headers"] = headers
        captured["auth"] = auth
        captured["terminate_on_close"] = terminate_on_close
        yield (proxy_read, proxy_write, lambda: "mock-session-id")

    _mock.captured = captured
    return _mock


# ---------------------------------------------------------------------------
# Auth fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def databricks_auth(mock_workspace_client):
    """Pre-configured DatabricksAuth instance."""
    from uc_mcp_proxy import DatabricksAuth

    return DatabricksAuth(mock_workspace_client)


# ---------------------------------------------------------------------------
# Sample message fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_jsonrpc_request():
    """A sample JSON-RPC request dict (tools/list)."""
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/list",
        "params": {},
    }


@pytest.fixture
def sample_jsonrpc_response():
    """A sample JSON-RPC response dict (tools/list result)."""
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "tools": [
                {
                    "name": "chat_postmessage",
                    "description": "Sends a message to a Slack channel",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "channel": {"type": "string"},
                            "text": {"type": "string"},
                        },
                        "required": ["channel"],
                    },
                }
            ]
        },
    }
