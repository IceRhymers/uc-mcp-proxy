"""Tests for CLI argument parsing and client construction."""

from __future__ import annotations

import sys
from unittest.mock import patch, MagicMock

import pytest

pytestmark = pytest.mark.unit


def test_requires_url_argument():
    """Proxy fails without --url."""
    with patch.object(sys, "argv", ["uc-mcp-proxy"]):
        with pytest.raises(SystemExit) as exc_info:
            from uc_mcp_proxy.__main__ import main
            main()
        assert exc_info.value.code == 2


def test_accepts_url_and_profile():
    """Valid --url and --profile args are parsed correctly."""
    with patch.object(sys, "argv", [
        "uc-mcp-proxy", "--url", "https://example.com/mcp", "--profile", "MY_PROFILE"
    ]):
        with patch("uc_mcp_proxy.__main__.asyncio.run") as mock_run:
            from uc_mcp_proxy.__main__ import main
            main()
            mock_run.assert_called_once()


def test_default_profile_is_none():
    """Without --profile, profile defaults to None (SDK default chain)."""
    with patch.object(sys, "argv", ["uc-mcp-proxy", "--url", "https://example.com/mcp"]):
        with patch("uc_mcp_proxy.__main__.run") as mock_run:
            mock_run.return_value = MagicMock()  # mock coroutine
            with patch("uc_mcp_proxy.__main__.asyncio.run"):
                from uc_mcp_proxy.__main__ import main
                main()
                mock_run.assert_called_once_with("https://example.com/mcp", None, None, None, verify_ssl=True)


def test_creates_workspace_client_with_profile():
    """WorkspaceClient is constructed with the correct profile kwarg."""
    with patch.object(sys, "argv", [
        "uc-mcp-proxy", "--url", "https://example.com/mcp", "--profile", "MY_PROFILE"
    ]):
        with patch("uc_mcp_proxy.__main__.run") as mock_run:
            mock_run.return_value = MagicMock()  # mock coroutine
            with patch("uc_mcp_proxy.__main__.asyncio.run"):
                from uc_mcp_proxy.__main__ import main
                main()
                mock_run.assert_called_once_with("https://example.com/mcp", "MY_PROFILE", None, None, verify_ssl=True)


def test_creates_workspace_client_with_auth_type():
    """--auth-type is passed through to run()."""
    with patch.object(sys, "argv", [
        "uc-mcp-proxy", "--url", "https://example.com/mcp", "--auth-type", "databricks-cli"
    ]):
        with patch("uc_mcp_proxy.__main__.run") as mock_run:
            mock_run.return_value = MagicMock()
            with patch("uc_mcp_proxy.__main__.asyncio.run"):
                from uc_mcp_proxy.__main__ import main
                main()
                mock_run.assert_called_once_with("https://example.com/mcp", None, "databricks-cli", None, verify_ssl=True)


def test_single_meta_parsed_correctly():
    """--meta KEY=VALUE is parsed into a dict and passed to run()."""
    with patch.object(sys, "argv", [
        "uc-mcp-proxy", "--url", "https://example.com/mcp",
        "--meta", "warehouse_id=abc123",
    ]):
        with patch("uc_mcp_proxy.__main__.run") as mock_run:
            mock_run.return_value = MagicMock()
            with patch("uc_mcp_proxy.__main__.asyncio.run"):
                from uc_mcp_proxy.__main__ import main
                main()
                mock_run.assert_called_once_with(
                    "https://example.com/mcp", None, None,
                    {"warehouse_id": "abc123"},
                    verify_ssl=True,
                )


def test_multiple_meta_parsed_correctly():
    """Multiple --meta flags produce a dict with all entries."""
    with patch.object(sys, "argv", [
        "uc-mcp-proxy", "--url", "https://example.com/mcp",
        "--meta", "warehouse_id=abc123",
        "--meta", "catalog=main",
    ]):
        with patch("uc_mcp_proxy.__main__.run") as mock_run:
            mock_run.return_value = MagicMock()
            with patch("uc_mcp_proxy.__main__.asyncio.run"):
                from uc_mcp_proxy.__main__ import main
                main()
                mock_run.assert_called_once_with(
                    "https://example.com/mcp", None, None,
                    {"warehouse_id": "abc123", "catalog": "main"},
                    verify_ssl=True,
                )


def test_meta_without_value_exits_with_error():
    """--meta bad (no =) produces a non-zero exit."""
    with patch.object(sys, "argv", [
        "uc-mcp-proxy", "--url", "https://example.com/mcp",
        "--meta", "bad",
    ]):
        with pytest.raises(SystemExit) as exc_info:
            from uc_mcp_proxy.__main__ import main
            main()
        assert exc_info.value.code == 1


def test_no_meta_passes_none():
    """No --meta flags → meta=None."""
    with patch.object(sys, "argv", ["uc-mcp-proxy", "--url", "https://example.com/mcp"]):
        with patch("uc_mcp_proxy.__main__.run") as mock_run:
            mock_run.return_value = MagicMock()
            with patch("uc_mcp_proxy.__main__.asyncio.run"):
                from uc_mcp_proxy.__main__ import main
                main()
                mock_run.assert_called_once_with("https://example.com/mcp", None, None, None, verify_ssl=True)


def test_no_verify_ssl_passes_verify_ssl_false():
    """--no-verify-ssl passes verify_ssl=False to run()."""
    with patch.object(sys, "argv", [
        "uc-mcp-proxy", "--url", "https://example.com/mcp", "--no-verify-ssl"
    ]):
        with patch("uc_mcp_proxy.__main__.run") as mock_run:
            mock_run.return_value = MagicMock()
            with patch("uc_mcp_proxy.__main__.asyncio.run"):
                from uc_mcp_proxy.__main__ import main
                main()
                mock_run.assert_called_once_with(
                    "https://example.com/mcp", None, None, None, verify_ssl=False
                )


def test_no_verify_ssl_prints_warning(capsys):
    """--no-verify-ssl prints a warning to stderr."""
    with patch.object(sys, "argv", [
        "uc-mcp-proxy", "--url", "https://example.com/mcp", "--no-verify-ssl"
    ]):
        with patch("uc_mcp_proxy.__main__.run") as mock_run:
            mock_run.return_value = MagicMock()
            with patch("uc_mcp_proxy.__main__.asyncio.run"):
                from uc_mcp_proxy.__main__ import main
                main()
        captured = capsys.readouterr()
        assert "warning" in captured.err.lower()
        assert "ssl" in captured.err.lower()


def test_without_no_verify_ssl_defaults_to_verify_true():
    """Without --no-verify-ssl, verify_ssl defaults to True."""
    with patch.object(sys, "argv", ["uc-mcp-proxy", "--url", "https://example.com/mcp"]):
        with patch("uc_mcp_proxy.__main__.run") as mock_run:
            mock_run.return_value = MagicMock()
            with patch("uc_mcp_proxy.__main__.asyncio.run"):
                from uc_mcp_proxy.__main__ import main
                main()
                _, kwargs = mock_run.call_args
                assert kwargs.get("verify_ssl", True) is True
