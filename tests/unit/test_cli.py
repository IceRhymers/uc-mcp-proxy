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
                mock_run.assert_called_once_with("https://example.com/mcp", None, None, None)


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
                mock_run.assert_called_once_with("https://example.com/mcp", "MY_PROFILE", None, None)


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
                mock_run.assert_called_once_with("https://example.com/mcp", None, "databricks-cli", None)


def test_single_header_parsed_correctly():
    """--header KEY=VALUE is parsed into a dict and passed to run()."""
    with patch.object(sys, "argv", [
        "uc-mcp-proxy", "--url", "https://example.com/mcp",
        "--header", "x-databricks-warehouse-id=abc123",
    ]):
        with patch("uc_mcp_proxy.__main__.run") as mock_run:
            mock_run.return_value = MagicMock()
            with patch("uc_mcp_proxy.__main__.asyncio.run"):
                from uc_mcp_proxy.__main__ import main
                main()
                mock_run.assert_called_once_with(
                    "https://example.com/mcp", None, None,
                    {"x-databricks-warehouse-id": "abc123"},
                )


def test_multiple_headers_parsed_correctly():
    """Multiple --header flags produce a dict with all entries."""
    with patch.object(sys, "argv", [
        "uc-mcp-proxy", "--url", "https://example.com/mcp",
        "--header", "x-databricks-warehouse-id=abc123",
        "--header", "x-databricks-catalog=main",
    ]):
        with patch("uc_mcp_proxy.__main__.run") as mock_run:
            mock_run.return_value = MagicMock()
            with patch("uc_mcp_proxy.__main__.asyncio.run"):
                from uc_mcp_proxy.__main__ import main
                main()
                mock_run.assert_called_once_with(
                    "https://example.com/mcp", None, None,
                    {
                        "x-databricks-warehouse-id": "abc123",
                        "x-databricks-catalog": "main",
                    },
                )


def test_header_without_value_exits_with_error():
    """--header bad (no =) produces a non-zero exit."""
    with patch.object(sys, "argv", [
        "uc-mcp-proxy", "--url", "https://example.com/mcp",
        "--header", "bad",
    ]):
        with pytest.raises(SystemExit) as exc_info:
            from uc_mcp_proxy.__main__ import main
            main()
        assert exc_info.value.code == 1


def test_no_headers_passes_none():
    """No --header flags → headers=None (backward-compatible)."""
    with patch.object(sys, "argv", ["uc-mcp-proxy", "--url", "https://example.com/mcp"]):
        with patch("uc_mcp_proxy.__main__.run") as mock_run:
            mock_run.return_value = MagicMock()
            with patch("uc_mcp_proxy.__main__.asyncio.run"):
                from uc_mcp_proxy.__main__ import main
                main()
                mock_run.assert_called_once_with("https://example.com/mcp", None, None, None)
