"""Tests for JSON-RPC _meta injection on tools/call messages."""

from __future__ import annotations

import anyio
import pytest
from mcp.shared.message import SessionMessage
from mcp.types import JSONRPCMessage, JSONRPCRequest

pytestmark = pytest.mark.unit


def _tools_call(params=None) -> SessionMessage:
    if params is None:
        params = {"name": "q", "arguments": {}}
    return SessionMessage(
        message=JSONRPCMessage(
            root=JSONRPCRequest(
                jsonrpc="2.0", id=1, method="tools/call", params=params,
            )
        )
    )


def _tools_list() -> SessionMessage:
    return SessionMessage(
        message=JSONRPCMessage(
            root=JSONRPCRequest(
                jsonrpc="2.0", id=2, method="tools/list", params={},
            )
        )
    )


def _tools_call_no_params() -> SessionMessage:
    return SessionMessage(
        message=JSONRPCMessage(
            root=JSONRPCRequest(
                jsonrpc="2.0", id=3, method="tools/call",
            )
        )
    )


def test_inject_meta_adds_meta_to_tools_call():
    from uc_mcp_proxy.__main__ import inject_meta

    out = inject_meta(_tools_call(), {"warehouse_id": "abc123"})

    assert out.message.root.params["_meta"] == {"warehouse_id": "abc123"}


def test_inject_meta_ignores_tools_list():
    from uc_mcp_proxy.__main__ import inject_meta

    out = inject_meta(_tools_list(), {"warehouse_id": "abc"})

    assert "_meta" not in (out.message.root.params or {})


def test_inject_meta_merges_with_existing_meta():
    from uc_mcp_proxy.__main__ import inject_meta

    msg = _tools_call(
        params={"name": "q", "arguments": {}, "_meta": {"progressToken": "tok-42"}},
    )
    out = inject_meta(msg, {"warehouse_id": "abc"})

    assert out.message.root.params["_meta"] == {
        "progressToken": "tok-42",
        "warehouse_id": "abc",
    }


def test_inject_meta_proxy_wins_on_collision(capsys):
    from uc_mcp_proxy.__main__ import inject_meta

    msg = _tools_call(
        params={"name": "q", "arguments": {}, "_meta": {"warehouse_id": "client-val"}},
    )
    out = inject_meta(msg, {"warehouse_id": "proxy-val"})

    assert out.message.root.params["_meta"]["warehouse_id"] == "proxy-val"
    err = capsys.readouterr().err
    assert "warehouse_id" in err
    assert "override" in err.lower()


def test_inject_meta_handles_exception_passthrough():
    from uc_mcp_proxy.__main__ import inject_meta

    err = RuntimeError("stream fault")
    assert inject_meta(err, {"warehouse_id": "abc"}) is err


def test_inject_meta_handles_missing_params():
    from uc_mcp_proxy.__main__ import inject_meta

    out = inject_meta(_tools_call_no_params(), {"warehouse_id": "abc"})

    assert out.message.root.params == {"_meta": {"warehouse_id": "abc"}}


def test_inject_meta_multiple_keys():
    from uc_mcp_proxy.__main__ import inject_meta

    out = inject_meta(_tools_call(), {"warehouse_id": "abc", "catalog": "main"})

    assert out.message.root.params["_meta"] == {
        "warehouse_id": "abc",
        "catalog": "main",
    }


def test_inject_meta_serializes_correctly():
    """Round-trip: injected _meta appears in JSON serialization via by_alias."""
    from uc_mcp_proxy.__main__ import inject_meta

    out = inject_meta(_tools_call(), {"warehouse_id": "abc"})
    payload = out.message.model_dump_json(by_alias=True, exclude_none=True)

    assert '"_meta":{"warehouse_id":"abc"}' in payload


@pytest.mark.anyio
async def test_inject_meta_stream_rewrites_only_tools_call(memory_stream_pair):
    from uc_mcp_proxy.__main__ import inject_meta_stream

    source_send, source_recv = memory_stream_pair(16)
    dest_send, dest_recv = memory_stream_pair(16)

    call = _tools_call()
    listing = _tools_list()

    async with anyio.create_task_group() as tg:
        tg.start_soon(inject_meta_stream, source_recv, dest_send, {"warehouse_id": "abc"})
        await source_send.send(call)
        await source_send.send(listing)
        await source_send.aclose()

    results = []
    async with dest_recv:
        async for m in dest_recv:
            results.append(m)

    assert results[0].message.root.params["_meta"] == {"warehouse_id": "abc"}
    assert "_meta" not in (results[1].message.root.params or {})


@pytest.mark.anyio
async def test_inject_meta_stream_passes_exception_through(memory_stream_pair):
    from uc_mcp_proxy.__main__ import inject_meta_stream

    source_send, source_recv = memory_stream_pair(4)
    dest_send, dest_recv = memory_stream_pair(4)

    err = RuntimeError("parse error")

    async with anyio.create_task_group() as tg:
        tg.start_soon(inject_meta_stream, source_recv, dest_send, {"warehouse_id": "abc"})
        await source_send.send(err)
        await source_send.aclose()

    results = []
    async with dest_recv:
        async for m in dest_recv:
            results.append(m)

    assert results == [err]
