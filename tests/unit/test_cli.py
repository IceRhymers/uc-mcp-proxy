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
                mock_run.assert_called_once_with("https://example.com/mcp", None, None)


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
                mock_run.assert_called_once_with("https://example.com/mcp", "MY_PROFILE", None)


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
                mock_run.assert_called_once_with("https://example.com/mcp", None, "databricks-cli")
