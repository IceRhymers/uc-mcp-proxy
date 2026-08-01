"""Version-agnostic helpers for building SDK objects in tests.

SDK 2.0 turned ``JSONRPCMessage`` from a pydantic root model into a plain
union alias, so ``JSONRPCMessage(...)`` is no longer callable and the
``SessionMessage.message`` it produced no longer has ``.root``. Tests build
messages by validating a raw wire payload through the SDK's own schema, which
accepts both shapes and keeps the JSON that actually goes on the wire as the
source of truth rather than a hand-built object graph.

``httpx`` is re-exported from the proxy's compat module so tests construct
requests, responses and transports from the same library the installed SDK
uses -- otherwise ``isinstance`` checks inside the proxy would compare objects
from two different HTTP libraries.
"""

from __future__ import annotations

from typing import Any

from mcp.shared.message import SessionMessage
from mcp.types import JSONRPCMessage
from pydantic import TypeAdapter

from uc_mcp_proxy._compat import httpx, jsonrpc_payload

__all__ = ["httpx", "jsonrpc_payload", "session_message"]

_JSONRPC_ADAPTER: TypeAdapter[Any] = TypeAdapter(JSONRPCMessage)


def session_message(payload: dict[str, Any]) -> SessionMessage:
    """Build a ``SessionMessage`` from a raw JSON-RPC ``payload`` dict."""
    return SessionMessage(_JSONRPC_ADAPTER.validate_python(payload))
