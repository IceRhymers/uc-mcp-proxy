"""Tests for the credential preflight (auto-login + auth-type gating)."""

from __future__ import annotations

import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk.errors import DatabricksError, PermissionDenied

from uc_mcp_proxy.auth import (
    _RECOVERABLE_AUTH_TYPES,
    _diagnose_non_oauth_auth,
    _preflight_authenticate,
    _read_auth_type_from_cfg,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mk_client(auth_type: str = "databricks-cli", profile: str | None = "DEFAULT"):
    """Build a MagicMock that quacks like WorkspaceClient for preflight purposes."""
    client = MagicMock()
    client.config.auth_type = auth_type
    client.config.profile = profile
    client.config.authenticate.return_value = {"Authorization": "Bearer t"}
    return client


def _patch_workspace(*clients):
    """Patch WorkspaceClient to return ``clients`` in order on each call."""
    return patch(
        "uc_mcp_proxy.auth.WorkspaceClient",
        side_effect=list(clients),
    )


def _ok_runner():
    """A subprocess.run mock returning a CompletedProcess with returncode 0."""
    return MagicMock(return_value=subprocess.CompletedProcess(args=[], returncode=0))


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_preflight_succeeds_with_valid_creds(self):
        client = _mk_client(auth_type="databricks-cli")
        runner = MagicMock()
        with _patch_workspace(client):
            result = _preflight_authenticate("e2-demo", None, runner=runner)
        assert result is client
        client.config.authenticate.assert_called_once()
        runner.assert_not_called()

    def test_preflight_skipped_for_env_var_auth(self):
        client = _mk_client(auth_type="pat", profile="DEFAULT")
        runner = MagicMock()
        with _patch_workspace(client):
            result = _preflight_authenticate(None, None, runner=runner)
        assert result is client
        runner.assert_not_called()


# ---------------------------------------------------------------------------
# OAuth U2M (the only auth_type we recover automatically)
# ---------------------------------------------------------------------------


class TestOAuthU2M:
    def test_preflight_runs_login_on_permission_denied_for_databricks_cli(self):
        bad = _mk_client(auth_type="databricks-cli", profile="e2-demo")
        bad.config.authenticate.side_effect = PermissionDenied("expired")
        good = _mk_client(auth_type="databricks-cli", profile="e2-demo")
        runner = _ok_runner()

        with _patch_workspace(bad, good):
            result = _preflight_authenticate("e2-demo", None, runner=runner)

        assert result is good
        runner.assert_called_once()
        cmd = runner.call_args.args[0]
        assert cmd == ["databricks", "auth", "login", "--profile", "e2-demo"]

    def test_preflight_runs_login_on_databricks_error_for_databricks_cli(self):
        bad = _mk_client(auth_type="databricks-cli", profile="e2-demo")
        bad.config.authenticate.side_effect = DatabricksError("config invalid")
        good = _mk_client(auth_type="databricks-cli", profile="e2-demo")
        runner = _ok_runner()

        with _patch_workspace(bad, good):
            _preflight_authenticate("e2-demo", None, runner=runner)

        runner.assert_called_once()

    def test_preflight_uses_resolved_profile_when_unspecified(self):
        bad = _mk_client(auth_type="databricks-cli", profile="DEFAULT")
        bad.config.authenticate.side_effect = PermissionDenied("expired")
        good = _mk_client(auth_type="databricks-cli", profile="DEFAULT")
        runner = _ok_runner()

        with _patch_workspace(bad, good):
            _preflight_authenticate(None, None, runner=runner)

        cmd = runner.call_args.args[0]
        assert cmd[-1] == "DEFAULT"

    def test_preflight_propagates_second_authenticate_failure(self):
        bad = _mk_client(auth_type="databricks-cli", profile="DEFAULT")
        bad.config.authenticate.side_effect = PermissionDenied("expired")
        still_bad = _mk_client(auth_type="databricks-cli", profile="DEFAULT")
        still_bad.config.authenticate.side_effect = PermissionDenied("still expired")
        runner = _ok_runner()

        with _patch_workspace(bad, still_bad):
            with pytest.raises(PermissionDenied):
                _preflight_authenticate(None, None, runner=runner)


# ---------------------------------------------------------------------------
# WorkspaceClient() itself raises (eager auth in Config.__init__)
# ---------------------------------------------------------------------------


class TestEagerConstructorFailure:
    """The SDK authenticates eagerly in ``Config.__init__``, so a stale OAuth
    refresh token raises ``ValueError`` out of ``WorkspaceClient(**kwargs)``
    before we reach ``client.config.authenticate()``. Make sure we still run
    ``databricks auth login`` in that path."""

    def test_constructor_value_error_triggers_login_when_cfg_has_databricks_cli(self, tmp_path, monkeypatch):
        cfg = tmp_path / ".databrickscfg"
        cfg.write_text("[e2-demo]\nhost = https://example.cloud.databricks.com\nauth_type = databricks-cli\n")
        monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(cfg))

        good = _mk_client(auth_type="databricks-cli", profile="e2-demo")
        runner = _ok_runner()

        with patch(
            "uc_mcp_proxy.auth.WorkspaceClient",
            side_effect=[ValueError("refresh token invalid"), good],
        ):
            result = _preflight_authenticate("e2-demo", None, runner=runner)

        assert result is good
        runner.assert_called_once()
        cmd = runner.call_args.args[0]
        assert cmd == ["databricks", "auth", "login", "--profile", "e2-demo"]

    def test_constructor_value_error_with_explicit_pat_does_not_login(self, tmp_path, monkeypatch):
        """If the caller passed ``auth_type=pat`` and construction raises, we
        must not touch ~/.databrickscfg with ``databricks auth login``."""
        cfg = tmp_path / ".databrickscfg"
        cfg.write_text("[DEFAULT]\nhost = https://x\ntoken = dapi-test\n")
        monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(cfg))

        runner = MagicMock()
        with (
            patch(
                "uc_mcp_proxy.auth.WorkspaceClient",
                side_effect=ValueError("pat invalid"),
            ),
            pytest.raises(SystemExit) as excinfo,
        ):
            _preflight_authenticate(None, "pat", runner=runner)

        runner.assert_not_called()
        assert "developer/access-tokens" in str(excinfo.value)

    def test_constructor_value_error_unknown_profile_falls_back_to_autodetect(self, tmp_path, monkeypatch):
        """No cfg file + no auth_type arg → ``(auto-detect)`` diagnosis, no login."""
        monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(tmp_path / "missing.cfg"))

        runner = MagicMock()
        with (
            patch(
                "uc_mcp_proxy.auth.WorkspaceClient",
                side_effect=ValueError("no creds"),
            ),
            pytest.raises(SystemExit) as excinfo,
        ):
            _preflight_authenticate(None, None, runner=runner)

        runner.assert_not_called()
        assert "DATABRICKS_TOKEN" in str(excinfo.value)


# ---------------------------------------------------------------------------
# _read_auth_type_from_cfg direct coverage
# ---------------------------------------------------------------------------


class TestReadAuthTypeFromCfg:
    def test_reads_auth_type_for_named_profile(self, tmp_path, monkeypatch):
        cfg = tmp_path / ".databrickscfg"
        cfg.write_text("[e2-demo]\nhost = https://example\nauth_type = databricks-cli\n")
        monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(cfg))
        assert _read_auth_type_from_cfg("e2-demo") == "databricks-cli"

    def test_returns_none_when_file_missing(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(tmp_path / "nope.cfg"))
        assert _read_auth_type_from_cfg("DEFAULT") is None

    def test_returns_none_when_profile_absent(self, tmp_path, monkeypatch):
        cfg = tmp_path / ".databrickscfg"
        cfg.write_text("[other]\nhost = https://x\n")
        monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(cfg))
        assert _read_auth_type_from_cfg("missing") is None

    def test_returns_none_when_field_absent(self, tmp_path, monkeypatch):
        cfg = tmp_path / ".databrickscfg"
        cfg.write_text("[myprof]\nhost = https://x\n")
        monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(cfg))
        assert _read_auth_type_from_cfg("myprof") is None

    def test_malformed_file_returns_none(self, tmp_path, monkeypatch):
        cfg = tmp_path / ".databrickscfg"
        cfg.write_text("not a valid ini [[[")
        monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(cfg))
        assert _read_auth_type_from_cfg("anything") is None


# ---------------------------------------------------------------------------
# Auth-type gating — the safety rail
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "auth_type, must_contain",
    [
        ("pat", "developer/access-tokens"),
        ("oauth-m2m", "client_id"),
        ("service-principal", "client_id"),
        ("azure-cli", "az login"),
        ("azure-client-secret", "Azure"),
        ("azure-msi", "Azure"),
        ("github-oidc", "federated"),
        ("google-credentials", "federated"),
        ("google-id", "federated"),
        ("env", "DATABRICKS_TOKEN"),
        ("some-future-thing", "will not run"),
    ],
)
class TestAuthTypeGating:
    def test_preflight_does_not_run_login(self, auth_type, must_contain):
        client = _mk_client(auth_type=auth_type, profile="myprof")
        client.config.authenticate.side_effect = PermissionDenied("nope")
        runner = MagicMock()

        with _patch_workspace(client), pytest.raises(SystemExit) as excinfo:
            _preflight_authenticate("myprof", None, runner=runner)

        runner.assert_not_called()
        assert must_contain in str(excinfo.value)


def test_preflight_does_not_run_login_for_auto_detect():
    """When auth_type is None on the config, fallback ``(auto-detect)`` triggers."""
    client = _mk_client(auth_type=None, profile="DEFAULT")
    client.config.authenticate.side_effect = PermissionDenied("nope")
    runner = MagicMock()

    with _patch_workspace(client), pytest.raises(SystemExit) as excinfo:
        _preflight_authenticate(None, None, runner=runner)

    runner.assert_not_called()
    assert "DATABRICKS_TOKEN" in str(excinfo.value)


def test_databrickscfg_is_not_modified_when_pat_fails(tmp_path, monkeypatch):
    """The most important test: a failed PAT preflight must not touch ~/.databrickscfg."""
    cfg = tmp_path / ".databrickscfg"
    cfg.write_text("[DEFAULT]\nhost = https://example.com\ntoken = dapi-test-pat\n")
    before_bytes = cfg.read_bytes()
    before_mtime = cfg.stat().st_mtime_ns

    monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(cfg))

    client = _mk_client(auth_type="pat", profile="DEFAULT")
    client.config.authenticate.side_effect = PermissionDenied("expired pat")
    runner = MagicMock()

    with _patch_workspace(client), pytest.raises(SystemExit):
        _preflight_authenticate(None, None, runner=runner)

    runner.assert_not_called()
    assert cfg.read_bytes() == before_bytes
    assert cfg.stat().st_mtime_ns == before_mtime


# ---------------------------------------------------------------------------
# Subprocess hygiene
# ---------------------------------------------------------------------------


class TestSubprocessHygiene:
    def test_preflight_uses_stderr_not_stdout(self):
        bad = _mk_client(auth_type="databricks-cli", profile="DEFAULT")
        bad.config.authenticate.side_effect = PermissionDenied("expired")
        good = _mk_client(auth_type="databricks-cli", profile="DEFAULT")
        runner = _ok_runner()

        with _patch_workspace(bad, good):
            _preflight_authenticate(None, None, runner=runner)

        assert runner.call_args.kwargs.get("stdout") is sys.stderr
        assert runner.call_args.kwargs.get("stderr") is sys.stderr

    def test_preflight_handles_missing_databricks_cli(self):
        bad = _mk_client(auth_type="databricks-cli", profile="DEFAULT")
        bad.config.authenticate.side_effect = PermissionDenied("expired")
        runner = MagicMock(side_effect=FileNotFoundError("no databricks"))

        with _patch_workspace(bad), pytest.raises(SystemExit) as excinfo:
            _preflight_authenticate(None, None, runner=runner)

        assert "docs.databricks.com" in str(excinfo.value)

    def test_preflight_propagates_login_failure(self):
        bad = _mk_client(auth_type="databricks-cli", profile="DEFAULT")
        bad.config.authenticate.side_effect = PermissionDenied("expired")
        runner = MagicMock(return_value=subprocess.CompletedProcess(args=[], returncode=2))

        with _patch_workspace(bad), pytest.raises(SystemExit) as excinfo:
            _preflight_authenticate(None, None, runner=runner)

        assert "exit 2" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Opt-out flag
# ---------------------------------------------------------------------------


class TestOptOut:
    def test_no_auto_login_skips_preflight(self):
        """run(no_auto_login=True) must construct the client without preflight."""
        import asyncio
        from contextlib import asynccontextmanager

        import anyio

        from uc_mcp_proxy import __main__ as main_mod

        @asynccontextmanager
        async def fake_stdio():
            send_a, recv_a = anyio.create_memory_object_stream(1)
            send_b, _recv_b = anyio.create_memory_object_stream(1)
            await send_a.aclose()
            yield (recv_a, send_b)

        @asynccontextmanager
        async def fake_http(url, *, http_client=None, **kwargs):
            send_a, recv_a = anyio.create_memory_object_stream(1)
            send_b, _recv_b = anyio.create_memory_object_stream(1)
            await send_a.aclose()
            yield (recv_a, send_b, lambda: "mock-session-id")

        with (
            patch.object(main_mod, "_preflight_authenticate") as preflight,
            patch.object(main_mod, "WorkspaceClient", return_value=_mk_client()),
            patch.object(main_mod, "stdio_server", side_effect=fake_stdio),
            patch.object(main_mod, "streamable_http_client", side_effect=fake_http),
        ):
            asyncio.run(
                main_mod.run(
                    "https://x/mcp",
                    profile="p",
                    auth_type="pat",
                    no_auto_login=True,
                )
            )
            preflight.assert_not_called()


# ---------------------------------------------------------------------------
# _diagnose_non_oauth_auth direct coverage
# ---------------------------------------------------------------------------


class TestDiagnose:
    def test_recoverable_set_only_contains_databricks_cli(self):
        assert "databricks-cli" in _RECOVERABLE_AUTH_TYPES
        assert len(_RECOVERABLE_AUTH_TYPES) == 1

    @pytest.mark.parametrize(
        "auth_type",
        [
            "pat",
            "oauth-m2m",
            "service-principal",
            "azure-cli",
            "azure-client-secret",
            "azure-msi",
            "github-oidc",
            "google-credentials",
            "google-id",
            "env",
            "(auto-detect)",
            "weird-future",
        ],
    )
    def test_diagnose_returns_string_with_profile(self, auth_type):
        msg = _diagnose_non_oauth_auth(auth_type, "myprof")
        assert isinstance(msg, str) and msg
        # Most messages mention the profile name; the env/auto-detect branch
        # is the explicit exception (it points at env vars instead).
        if auth_type not in {"env", "(auto-detect)"}:
            assert "myprof" in msg
