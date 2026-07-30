"""Tests for ``_resolve_exchange`` — the decision matrix that turns the flags,
the credential shape, and the target URL into an ExchangeConfig, a refusal, or
a decision to send the credential as-is."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from uc_mcp_proxy.__main__ import _resolve_exchange

pytestmark = pytest.mark.unit


HOST = "https://test-workspace.cloud.databricks.com"
APP_URL = "https://mcp-code-search-123.aws.databricksapps.com/mcp"
MANAGED_URL = "https://test-workspace.cloud.databricks.com/api/2.0/mcp/sql"

#: A Lakebox-shaped credential: 36 chars, no ``dapi`` prefix.
LAKEBOX_CRED = "abc12345-6789-def0-1234-56789abcdef0"
CLASSIC_PAT = "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"


def _app(
    *,
    name="mcp-code-search",
    url="https://mcp-code-search-123.aws.databricksapps.com",
    client_id="disc-cid",
    scopes=None,
):
    return SimpleNamespace(
        name=name,
        url=url,
        oauth2_app_client_id=client_id,
        effective_user_api_scopes=scopes,
    )


def make_client(*, auth_type="pat", host=HOST, profile="p", token=LAKEBOX_CRED, apps=(), get=None):
    client = MagicMock()
    client.config.auth_type = auth_type
    client.config.host = host
    client.config.profile = profile
    client.config.authenticate.return_value = {"Authorization": f"Bearer {token}"} if token else {}
    client.apps.list.return_value = iter(apps)
    if get is not None:
        client.apps.get.side_effect = get
    return client


def resolve(client, url, **kwargs):
    kwargs.setdefault("client_id", None)
    kwargs.setdefault("scopes", ())
    kwargs.setdefault("pat_exchange", False)
    kwargs.setdefault("verify_ssl", True)
    return _resolve_exchange(client, url, **kwargs)


# --- explicit --client-id (unchanged behavior) -----------------------------


def test_explicit_client_id_returns_config_without_discovery():
    client = make_client()
    config = resolve(client, APP_URL, client_id="explicit-cid", scopes=("sql",))
    assert config is not None
    assert config.client_id == "explicit-cid"
    assert config.scopes == ("sql",)
    assert config.host == HOST
    client.apps.list.assert_not_called()


def test_explicit_client_id_on_non_pat_exits():
    client = make_client(auth_type="databricks-cli")
    with pytest.raises(SystemExit) as exc:
        resolve(client, APP_URL, client_id="explicit-cid")
    assert "--client-id" in str(exc.value)


def test_explicit_client_id_without_host_exits():
    client = make_client(host="")
    with pytest.raises(SystemExit) as exc:
        resolve(client, APP_URL, client_id="explicit-cid")
    assert "workspace host" in str(exc.value)


# --- non-pat credential ----------------------------------------------------


def test_non_pat_returns_none_even_on_app_host():
    client = make_client(auth_type="databricks-cli")
    assert resolve(client, APP_URL) is None


def test_non_pat_with_pat_exchange_exits():
    client = make_client(auth_type="oauth-m2m")
    with pytest.raises(SystemExit) as exc:
        resolve(client, APP_URL, pat_exchange=True)
    assert "--pat-exchange" in str(exc.value)


# --- pat against a non-app (managed/external) target -----------------------


def test_pat_on_managed_host_returns_none():
    client = make_client()
    assert resolve(client, MANAGED_URL) is None
    client.apps.list.assert_not_called()


# --- pat against an app host: the automatic path ---------------------------


def test_lakebox_pat_on_app_host_auto_discovers():
    client = make_client(apps=[_app(scopes=["iam.current-user:read"])])
    config = resolve(client, APP_URL)
    assert config is not None
    assert config.client_id == "disc-cid"
    assert config.scopes == ("iam.current-user:read",)


def test_explicit_scopes_override_discovered_scopes():
    client = make_client(apps=[_app(scopes=["discovered"])])
    config = resolve(client, APP_URL, scopes=("explicit",))
    assert config is not None
    assert config.scopes == ("explicit",)


def test_discovered_empty_scopes_warns_but_returns_config(capsys):
    client = make_client(apps=[_app(scopes=None)])
    config = resolve(client, APP_URL)
    assert config is not None
    assert config.scopes == ()
    assert "no user API scopes" in capsys.readouterr().err


def test_app_host_but_no_matching_app_exits():
    client = make_client(apps=[_app(url="https://other-999.aws.databricksapps.com")])
    with pytest.raises(SystemExit) as exc:
        resolve(client, APP_URL)
    assert "no app visible" in str(exc.value)


def test_discovery_failure_exits_and_scrubs_the_pat():
    secret = "lakebox-secret-do-not-leak-000000000"

    def boom():
        raise RuntimeError(f"403 for token {secret}")

    client = make_client(token=secret)
    client.apps.list.side_effect = boom
    with pytest.raises(SystemExit) as exc:
        resolve(client, APP_URL)
    message = str(exc.value)
    assert "could not list Databricks Apps" in message
    assert secret not in message


def test_app_host_without_host_configured_exits():
    client = make_client(host="", apps=[_app()])
    with pytest.raises(SystemExit) as exc:
        resolve(client, APP_URL)
    assert "workspace host" in str(exc.value)


# --- classic dapi PAT: the refusal -----------------------------------------


def test_classic_pat_on_app_host_hard_refuses():
    client = make_client(token=CLASSIC_PAT, apps=[_app()])
    with pytest.raises(SystemExit) as exc:
        resolve(client, APP_URL)
    message = str(exc.value)
    assert "classic personal access token" in message
    assert "--pat-exchange" in message
    # Refused before any discovery, and without printing the token.
    client.apps.list.assert_not_called()
    assert CLASSIC_PAT not in message


def test_classic_pat_with_pat_exchange_warns_and_proceeds(capsys):
    client = make_client(token=CLASSIC_PAT, apps=[_app(scopes=["s"])])
    config = resolve(client, APP_URL, pat_exchange=True)
    assert config is not None
    assert config.client_id == "disc-cid"
    assert "looks like a classic" in capsys.readouterr().err


# --- forced exchange on a non-app URL --------------------------------------


def test_pat_exchange_forced_on_non_app_url_without_match_exits():
    client = make_client(apps=[])
    with pytest.raises(SystemExit) as exc:
        resolve(client, MANAGED_URL, pat_exchange=True)
    assert "not a Databricks App host" in str(exc.value)


def test_pat_exchange_forced_on_managed_url_with_explicit_client_id():
    """--client-id short-circuits: a forced exchange on a managed URL just works."""
    client = make_client()
    config = resolve(client, MANAGED_URL, client_id="cid", pat_exchange=True)
    assert config is not None
    assert config.client_id == "cid"
    client.apps.list.assert_not_called()
