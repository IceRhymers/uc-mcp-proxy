"""Tests for workspace-relative --url resolution."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import patch

import anyio
import pytest

from uc_mcp_proxy.__main__ import _resolve_url, run

pytestmark = pytest.mark.unit


def test_absolute_url_passthrough(mock_workspace_client):
    """An absolute URL is returned unchanged."""
    assert (
        _resolve_url("https://example.com/api/2.0/mcp/foo", mock_workspace_client)
        == "https://example.com/api/2.0/mcp/foo"
    )


def test_relative_url_joined_with_host(mock_workspace_client):
    """A leading-slash relative URL is joined against config.host."""
    assert (
        _resolve_url("/api/2.0/mcp/foo", mock_workspace_client)
        == "https://test-workspace.cloud.databricks.com/api/2.0/mcp/foo"
    )


def test_relative_url_no_leading_slash(mock_workspace_client):
    """A relative URL without a leading slash still resolves under the host."""
    assert (
        _resolve_url("api/2.0/mcp/foo", mock_workspace_client)
        == "https://test-workspace.cloud.databricks.com/api/2.0/mcp/foo"
    )


def test_relative_url_host_with_trailing_slash(mock_workspace_client):
    """Host already ending in '/' does not produce a double slash."""
    mock_workspace_client.config.host = "https://test-workspace.cloud.databricks.com/"
    assert (
        _resolve_url("/api/2.0/mcp/foo", mock_workspace_client)
        == "https://test-workspace.cloud.databricks.com/api/2.0/mcp/foo"
    )


def test_relative_url_missing_host_raises(mock_workspace_client):
    """Relative URL with no configured host exits with an error."""
    mock_workspace_client.config.host = None
    with pytest.raises(SystemExit):
        _resolve_url("/api/2.0/mcp/foo", mock_workspace_client)


def test_run_resolves_relative_url(mock_workspace_client):
    """run() forwards the resolved absolute URL to streamable_http_client."""
    captured: dict = {}

    @asynccontextmanager
    async def fake_stdio():
        send_a, recv_a = anyio.create_memory_object_stream(1)
        send_b, _recv_b = anyio.create_memory_object_stream(1)
        await send_a.aclose()
        yield (recv_a, send_b)

    @asynccontextmanager
    async def fake_http(url, *, http_client=None, **kwargs):
        captured["url"] = url
        send_a, recv_a = anyio.create_memory_object_stream(1)
        send_b, _recv_b = anyio.create_memory_object_stream(1)
        await send_a.aclose()
        yield (recv_a, send_b, lambda: "mock-session-id")

    with (
        patch("uc_mcp_proxy.__main__.WorkspaceClient", return_value=mock_workspace_client),
        patch("uc_mcp_proxy.__main__.stdio_server", side_effect=fake_stdio),
        patch("uc_mcp_proxy.__main__.streamable_http_client", side_effect=fake_http),
    ):
        anyio.run(run, "/api/2.0/mcp/foo", None, None, None, True, True)

    assert captured["url"] == "https://test-workspace.cloud.databricks.com/api/2.0/mcp/foo"
