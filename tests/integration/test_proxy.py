"""Integration tests for the full proxy flow with mocked transports."""

from __future__ import annotations

import inspect
from contextlib import asynccontextmanager
from unittest.mock import patch

import anyio
import httpx
import pytest
from mcp.client.streamable_http import streamable_http_client as _real_streamable_http_client

pytestmark = [pytest.mark.integration, pytest.mark.anyio]


async def test_proxy_forwards_request_and_response(mock_workspace_client, memory_stream_pair):
    """End-to-end: request goes in via stdio, arrives on HTTP side;
    response comes back via HTTP, arrives on stdio side."""
    from uc_mcp_proxy.__main__ import run

    # Set up stdio streams
    stdio_in_send, stdio_in_recv = memory_stream_pair(16)
    stdio_out_send, stdio_out_recv = memory_stream_pair(16)

    # Set up HTTP streams
    http_in_send, http_in_recv = memory_stream_pair(16)
    http_out_send, http_out_recv = memory_stream_pair(16)

    @asynccontextmanager
    async def fake_stdio():
        yield (stdio_in_recv, stdio_out_send)

    captured = {}

    @asynccontextmanager
    async def fake_http(url, *, auth=None, **kwargs):
        captured["url"] = url
        captured["auth"] = auth
        yield (http_in_recv, http_out_send, lambda: "mock-session-id")

    with patch("uc_mcp_proxy.__main__.WorkspaceClient", return_value=mock_workspace_client):
        with patch("uc_mcp_proxy.__main__.stdio_server", side_effect=fake_stdio):
            with patch("uc_mcp_proxy.__main__.streamable_http_client", side_effect=fake_http):
                async with anyio.create_task_group() as tg:
                    tg.start_soon(run, "https://example.com/mcp", None)

                    # Claude Code -> proxy -> remote
                    await stdio_in_send.send("request-message")
                    forwarded = await http_out_recv.receive()
                    assert forwarded == "request-message"

                    # Remote -> proxy -> Claude Code
                    await http_in_send.send("response-message")
                    returned = await stdio_out_recv.receive()
                    assert returned == "response-message"

                    # Shut down
                    await stdio_in_send.aclose()
                    await http_in_send.aclose()


async def test_proxy_passes_auth_on_http_client(mock_workspace_client, memory_stream_pair):
    """DatabricksAuth is configured on the httpx.AsyncClient handed to streamable_http_client."""
    from uc_mcp_proxy.__main__ import DatabricksAuth, run

    stdio_in_send, stdio_in_recv = memory_stream_pair(16)
    stdio_out_send, stdio_out_recv = memory_stream_pair(16)
    http_in_send, http_in_recv = memory_stream_pair(16)
    http_out_send, http_out_recv = memory_stream_pair(16)

    @asynccontextmanager
    async def fake_stdio():
        yield (stdio_in_recv, stdio_out_send)

    captured = {}

    @asynccontextmanager
    async def fake_http(url, *, http_client=None, **kwargs):
        captured["http_client"] = http_client
        yield (http_in_recv, http_out_send, lambda: "mock-session-id")

    with patch("uc_mcp_proxy.__main__.WorkspaceClient", return_value=mock_workspace_client):
        with patch("uc_mcp_proxy.__main__.stdio_server", side_effect=fake_stdio):
            with patch("uc_mcp_proxy.__main__.streamable_http_client", side_effect=fake_http):
                async with anyio.create_task_group() as tg:
                    tg.start_soon(run, "https://example.com/mcp", None)

                    # Let the proxy set up
                    await anyio.sleep(0)

                    # Shut down
                    await stdio_in_send.aclose()
                    await http_in_send.aclose()

    assert isinstance(captured["http_client"], httpx.AsyncClient)
    assert isinstance(captured["http_client"].auth, DatabricksAuth)


async def test_proxy_uses_correct_url(mock_workspace_client, memory_stream_pair):
    """streamablehttp_client is called with the URL from CLI args."""
    from uc_mcp_proxy.__main__ import run

    stdio_in_send, stdio_in_recv = memory_stream_pair(16)
    stdio_out_send, stdio_out_recv = memory_stream_pair(16)
    http_in_send, http_in_recv = memory_stream_pair(16)
    http_out_send, http_out_recv = memory_stream_pair(16)

    @asynccontextmanager
    async def fake_stdio():
        yield (stdio_in_recv, stdio_out_send)

    captured = {}

    @asynccontextmanager
    async def fake_http(url, *, auth=None, **kwargs):
        captured["url"] = url
        yield (http_in_recv, http_out_send, lambda: "mock-session-id")

    target_url = "https://my-app.cloud.databricks.com/mcp"

    with patch("uc_mcp_proxy.__main__.WorkspaceClient", return_value=mock_workspace_client):
        with patch("uc_mcp_proxy.__main__.stdio_server", side_effect=fake_stdio):
            with patch("uc_mcp_proxy.__main__.streamable_http_client", side_effect=fake_http):
                async with anyio.create_task_group() as tg:
                    tg.start_soon(run, target_url, "DEFAULT")

                    await anyio.sleep(0)

                    await stdio_in_send.aclose()
                    await http_in_send.aclose()

    assert captured["url"] == target_url


async def test_run_calls_streamable_http_client_with_supported_kwargs(mock_workspace_client, memory_stream_pair):
    """Guard: kwargs passed to streamable_http_client must bind to the real SDK signature.

    Without this, fakes with **kwargs silently accept misnamed params and the
    bug only surfaces at runtime against a live server.
    """
    from uc_mcp_proxy.__main__ import run

    stdio_in_send, stdio_in_recv = memory_stream_pair(16)
    stdio_out_send, stdio_out_recv = memory_stream_pair(16)
    http_in_send, http_in_recv = memory_stream_pair(16)
    http_out_send, http_out_recv = memory_stream_pair(16)

    @asynccontextmanager
    async def fake_stdio():
        yield (stdio_in_recv, stdio_out_send)

    captured_args: tuple = ()
    captured_kwargs: dict = {}

    @asynccontextmanager
    async def fake_http(*args, **kwargs):
        nonlocal captured_args
        captured_args = args
        captured_kwargs.update(kwargs)
        yield (http_in_recv, http_out_send, lambda: "mock-session-id")

    with patch("uc_mcp_proxy.__main__.WorkspaceClient", return_value=mock_workspace_client):
        with patch("uc_mcp_proxy.__main__.stdio_server", side_effect=fake_stdio):
            with patch("uc_mcp_proxy.__main__.streamable_http_client", side_effect=fake_http):
                async with anyio.create_task_group() as tg:
                    tg.start_soon(run, "https://example.com/mcp", None, None, None, False)
                    await anyio.sleep(0)
                    await stdio_in_send.aclose()
                    await http_in_send.aclose()

    # Bind the captured call against the real SDK signature — this raises
    # TypeError if our kwargs don't match what the installed mcp exposes.
    sig = inspect.signature(_real_streamable_http_client)
    sig.bind(*captured_args, **captured_kwargs)

    assert isinstance(captured_kwargs["http_client"], httpx.AsyncClient)
