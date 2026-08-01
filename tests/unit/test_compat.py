"""Tests for the MCP SDK major-version compatibility shim.

The shim exists because SDK 2.0 renamed the HTTP client library and flattened
``SessionMessage.message``. These tests pin the two decisions that are easy to
get subtly wrong: resolving the HTTP module from the SDK rather than by import
guess, and reaching the JSON-RPC payload through whichever shape is present.
"""

from __future__ import annotations

from types import ModuleType, SimpleNamespace

import pytest
from mcp.client import streamable_http as sdk_streamable_http
from pydantic import RootModel

from tests.support import session_message
from uc_mcp_proxy import _compat
from uc_mcp_proxy.__main__ import _build_http_client
from uc_mcp_proxy.errors import HttpErrorReporter

pytestmark = pytest.mark.unit


def test_resolved_httpx_is_the_module_the_sdk_imported():
    """The proxy must build clients from the same library the SDK will use.

    Asserted against the SDK module's own binding rather than a version
    string, so this keeps holding if a later SDK renames the library again.
    """
    sdk_module = getattr(sdk_streamable_http, "httpx2", None) or sdk_streamable_http.httpx

    assert _compat.httpx is sdk_module


def test_built_client_is_an_instance_of_the_sdks_client_class():
    """The end the shim exists for: a client the installed SDK accepts."""
    reporter = HttpErrorReporter(url="https://example.com/mcp", profile="p", auth_type="pat")

    client = _build_http_client(auth=None, verify_ssl=True, reporter=reporter)

    assert isinstance(client, _compat.httpx.AsyncClient)


def test_resolve_httpx_rejects_an_sdk_that_imported_neither(monkeypatch):
    """A future SDK on a third library must fail loudly, not silently guess."""
    monkeypatch.setattr(_compat, "_sdk_streamable_http", SimpleNamespace())

    with pytest.raises(RuntimeError, match="which HTTP client library"):
        _compat._resolve_httpx()


def test_resolve_httpx_ignores_non_module_attributes(monkeypatch):
    """``httpx2`` bound to something that is not a module is not a match."""
    real = _compat.httpx
    monkeypatch.setattr(
        _compat,
        "_sdk_streamable_http",
        SimpleNamespace(httpx2="not-a-module", httpx=real),
    )

    resolved = _compat._resolve_httpx()

    assert isinstance(resolved, ModuleType)
    assert resolved is real


def test_jsonrpc_payload_reads_the_installed_sdk_shape():
    """Whatever the SDK produces, the payload exposes the JSON-RPC fields."""
    message = session_message({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {}})

    payload = _compat.jsonrpc_payload(message)

    assert payload.method == "tools/call"
    assert payload.params == {}


def test_jsonrpc_payload_unwraps_a_root_model():
    """The SDK 1.x shape: ``message`` is a pydantic root model wrapper.

    Built with a real ``RootModel`` so the branch is exercised even when the
    installed SDK is 2.x and never produces one.
    """
    inner = SimpleNamespace(method="tools/call")
    message = SimpleNamespace(message=RootModel[object](inner))

    assert _compat.jsonrpc_payload(message) is inner


def test_jsonrpc_payload_passes_through_a_bare_model():
    """The SDK 2.0 shape: ``message`` is the JSON-RPC model itself."""
    inner = SimpleNamespace(method="tools/call")
    message = SimpleNamespace(message=inner)

    assert _compat.jsonrpc_payload(message) is inner
