"""Compatibility across MCP SDK major versions.

SDK 2.0 changed three things this proxy depends on:

1. the HTTP client library moved from ``httpx`` to ``httpx2``;
2. ``streamable_http_client`` yields ``(read, write)`` where 1.x yielded
   ``(read, write, get_session_id)``;
3. ``SessionMessage.message`` is the JSON-RPC model itself, where 1.x wrapped
   it in the ``JSONRPCMessage`` pydantic root model.

Only (1) and (3) need a shim here -- (2) is absorbed by a starred unpack at
the one call site in ``__main__``.

The HTTP library is read back off the SDK module rather than imported by a
guessed name. ``httpx`` and ``httpx2`` install side by side: httpx2 does not
replace httpx, and databricks-sdk and others still pull httpx in. So
``try: import httpx2`` would hand an SDK 1.x transport a client built from a
library that SDK never imported -- and because the proxy owns the client it
passes to ``streamable_http_client``, that mismatch surfaces as a type error
deep inside the SDK's request path rather than at import time. Asking the SDK
which module it bound is the only answer that cannot drift.
"""

from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING, Any, Protocol

from mcp.client import streamable_http as _sdk_streamable_http

__all__ = ["MessageReceiveStream", "MessageSendStream", "httpx", "jsonrpc_payload"]

#: Module names to look for on the SDK transport, newest SDK first.
_HTTPX_MODULE_NAMES = ("httpx2", "httpx")


def _resolve_httpx() -> ModuleType:
    """Return the HTTP client module the installed MCP SDK builds clients from."""
    for name in _HTTPX_MODULE_NAMES:
        module = getattr(_sdk_streamable_http, name, None)
        if isinstance(module, ModuleType):
            return module
    raise RuntimeError(
        "uc-mcp-proxy: cannot tell which HTTP client library this MCP SDK uses. "
        f"Expected mcp.client.streamable_http to import one of {_HTTPX_MODULE_NAMES}."
    )


if TYPE_CHECKING:
    # httpx2 mirrors the httpx API for every name the proxy touches, so the
    # httpx stubs describe both. Only the runtime object has to match the SDK.
    import httpx
else:
    httpx = _resolve_httpx()


def jsonrpc_payload(message: Any) -> Any:
    """Return the JSON-RPC model carried by a ``SessionMessage``.

    SDK 1.x wraps it in the ``JSONRPCMessage`` root model; 2.0 stores the
    ``JSONRPCRequest``/``JSONRPCNotification``/... directly. Both are reached
    through the attribute that exists, so neither version is special-cased.
    """
    payload = message.message
    return getattr(payload, "root", payload)


class MessageReceiveStream(Protocol):
    """The read half of a transport, as the bridge actually uses it.

    Structural on purpose. SDK 1.x hands out anyio ``MemoryObjectReceiveStream``
    objects and 2.0 hands out its own context-carrying wrappers; naming either
    concrete class here would type-check against one SDK and fail on the other.
    """

    def __aiter__(self) -> Any: ...

    async def __anext__(self) -> Any: ...

    async def aclose(self) -> None: ...


class MessageSendStream(Protocol):
    """The write half of a transport, as the bridge actually uses it."""

    async def send(self, item: Any, /) -> None: ...

    async def aclose(self) -> None: ...
