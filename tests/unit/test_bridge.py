"""Tests for copy_stream and bridge functions."""

from __future__ import annotations

import anyio
import pytest

pytestmark = [pytest.mark.unit, pytest.mark.anyio]


async def test_copy_stream_forwards_messages(memory_stream_pair):
    """Messages written to source appear on dest in order."""
    from uc_mcp_proxy.__main__ import copy_stream

    source_send, source_recv = memory_stream_pair(16)
    dest_send, dest_recv = memory_stream_pair(16)

    async with anyio.create_task_group() as tg:
        tg.start_soon(copy_stream, source_recv, dest_send)

        await source_send.send("message-1")
        await source_send.send("message-2")
        await source_send.aclose()

    results = []
    async with dest_recv:
        async for msg in dest_recv:
            results.append(msg)

    assert results == ["message-1", "message-2"]


async def test_copy_stream_handles_source_close(memory_stream_pair):
    """When source closes, copy_stream exits without error."""
    from uc_mcp_proxy.__main__ import copy_stream

    source_send, source_recv = memory_stream_pair(4)
    dest_send, dest_recv = memory_stream_pair(4)

    await source_send.aclose()
    await copy_stream(source_recv, dest_send)


async def test_bridge_stdio_to_http(stdio_streams, http_streams):
    """A message sent via stdio arrives on the http side."""
    from uc_mcp_proxy.__main__ import bridge

    stdio_send, stdio_recv, stdio_write, stdio_read = stdio_streams
    http_send, http_recv, http_write, http_read = http_streams

    async with anyio.create_task_group() as tg:
        tg.start_soon(bridge, stdio_recv, stdio_write, http_recv, http_write)

        await stdio_send.send("request")
        forwarded = await http_read.receive()
        assert forwarded == "request"

        await stdio_send.aclose()
        await http_send.aclose()


async def test_bridge_http_to_stdio(stdio_streams, http_streams):
    """A message sent via http arrives on the stdio side."""
    from uc_mcp_proxy.__main__ import bridge

    stdio_send, stdio_recv, stdio_write, stdio_read = stdio_streams
    http_send, http_recv, http_write, http_read = http_streams

    async with anyio.create_task_group() as tg:
        tg.start_soon(bridge, stdio_recv, stdio_write, http_recv, http_write)

        await http_send.send("response")
        returned = await stdio_read.receive()
        assert returned == "response"

        await stdio_send.aclose()
        await http_send.aclose()


async def test_bridge_bidirectional_roundtrip(stdio_streams, http_streams):
    """Full roundtrip: message goes stdio->http, response comes http->stdio."""
    from uc_mcp_proxy.__main__ import bridge

    stdio_send, stdio_recv, stdio_write, stdio_read = stdio_streams
    http_send, http_recv, http_write, http_read = http_streams

    async with anyio.create_task_group() as tg:
        tg.start_soon(bridge, stdio_recv, stdio_write, http_recv, http_write)

        # Claude -> Remote
        await stdio_send.send("request")
        forwarded = await http_read.receive()
        assert forwarded == "request"

        # Remote -> Claude
        await http_send.send("response")
        returned = await stdio_read.receive()
        assert returned == "response"

        # Shutdown
        await stdio_send.aclose()
        await http_send.aclose()


async def test_bridge_exits_on_stream_close(stdio_streams, http_streams):
    """When both sides close, bridge task group exits cleanly."""
    from uc_mcp_proxy.__main__ import bridge

    stdio_send, stdio_recv, stdio_write, stdio_read = stdio_streams
    http_send, http_recv, http_write, http_read = http_streams

    await stdio_send.aclose()
    await http_send.aclose()

    # Should complete without raising
    await bridge(stdio_recv, stdio_write, http_recv, http_write)
