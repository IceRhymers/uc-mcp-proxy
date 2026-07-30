"""Tests for App-host detection, PAT-shape detection, and app discovery."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from uc_mcp_proxy.app_discovery import (
    AppDiscoveryError,
    DiscoveredApp,
    discover_app,
    is_app_host,
    looks_like_classic_pat,
)

pytestmark = pytest.mark.unit


APP_URL = "https://mcp-code-search-123.aws.databricksapps.com/mcp"


def _app(
    *, name="mcp-code-search", url="https://mcp-code-search-123.aws.databricksapps.com", client_id="cid", scopes=None
):
    """A stand-in for an SDK ``App`` — only the fields discovery reads."""
    return SimpleNamespace(
        name=name,
        url=url,
        oauth2_app_client_id=client_id,
        effective_user_api_scopes=scopes,
    )


def _client(apps, *, get=None):
    """A fake WorkspaceClient whose ``apps.list``/``apps.get`` return fixtures."""
    client = MagicMock()
    client.apps.list.return_value = iter(apps)
    if get is not None:
        client.apps.get.side_effect = get
    return client


# --- is_app_host -----------------------------------------------------------


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://mcp-code-search-123.aws.databricksapps.com/mcp", True),
        ("https://foo.azure.databricksapps.com", True),
        ("https://workspace.cloud.databricks.com/api/2.0/mcp/sql", False),
        ("https://example.com/mcp", False),
        ("/api/2.0/mcp/sql", False),  # relative: no host
        ("", False),
        # A host that merely contains the string but is not a subdomain.
        ("https://databricksapps.com.evil.com/mcp", False),
    ],
)
def test_is_app_host(url, expected):
    assert is_app_host(url) is expected


# --- looks_like_classic_pat ------------------------------------------------


@pytest.mark.parametrize(
    "token,expected",
    [
        ("dapi-EXAMPLE-not-a-real-token", True),
        ("dapi-fake", True),
        ("f7a8b9c0-1234-5678-9abc-def012345678", False),  # uuid-ish Lakebox shape
        ("abc123mixedalnum", False),
        ("", False),  # empty is not classic — handled elsewhere
    ],
)
def test_looks_like_classic_pat(token, expected):
    assert looks_like_classic_pat(token) is expected


# --- discover_app ----------------------------------------------------------


def test_discover_app_matches_by_host_and_returns_params():
    client = _client([_app(scopes=["a", "b"])])
    result = discover_app(client, APP_URL)
    assert result == DiscoveredApp(name="mcp-code-search", client_id="cid", scopes=("a", "b"))


def test_discover_app_ignores_path_and_query_when_matching():
    client = _client([_app(url="https://mcp-code-search-123.aws.databricksapps.com")])
    assert discover_app(client, APP_URL) is not None


def test_discover_app_returns_none_when_no_url_matches():
    client = _client([_app(url="https://other-app-999.aws.databricksapps.com")])
    assert discover_app(client, APP_URL) is None


def test_discover_app_returns_none_when_matched_app_has_no_client_id():
    client = _client([_app(client_id=None)])
    assert discover_app(client, APP_URL) is None


def test_discover_app_falls_back_to_get_for_scopes():
    """When list() omits scopes, get(name) is consulted — mirrors the real API."""
    listed = _app(scopes=None)
    detailed = _app(scopes=["iam.current-user:read"])
    client = _client([listed], get=lambda name: detailed)
    result = discover_app(client, APP_URL)
    assert result is not None
    assert result.scopes == ("iam.current-user:read",)
    client.apps.get.assert_called_once_with("mcp-code-search")


def test_discover_app_tolerates_get_failure_leaving_scopes_empty():
    def boom(name):
        raise RuntimeError("no permission")

    client = _client([_app(scopes=None)], get=boom)
    result = discover_app(client, APP_URL)
    assert result is not None
    assert result.scopes == ()


def test_discover_app_does_not_call_get_when_list_already_has_scopes():
    client = _client([_app(scopes=["a"])])
    discover_app(client, APP_URL)
    client.apps.get.assert_not_called()


def test_discover_app_raises_on_list_failure():
    client = MagicMock()
    client.apps.list.side_effect = RuntimeError("403 permission denied")
    with pytest.raises(AppDiscoveryError) as exc_info:
        discover_app(client, APP_URL)
    assert "permission denied" in str(exc_info.value)


def test_discover_app_returns_none_for_relative_url():
    client = _client([_app()])
    assert discover_app(client, "/api/2.0/mcp/sql") is None
