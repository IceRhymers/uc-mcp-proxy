"""Tests for CLI argument parsing and client construction."""

from __future__ import annotations

import sys
from contextlib import asynccontextmanager
from unittest.mock import MagicMock, patch

import anyio
import httpx
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
    with patch.object(sys, "argv", ["uc-mcp-proxy", "--url", "https://example.com/mcp", "--profile", "MY_PROFILE"]):
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
                mock_run.assert_called_once_with(
                    "https://example.com/mcp",
                    None,
                    None,
                    None,
                    verify_ssl=True,
                    no_auto_login=False,
                    client_id=None,
                    scopes=(),
                )


def test_creates_workspace_client_with_profile():
    """WorkspaceClient is constructed with the correct profile kwarg."""
    with patch.object(sys, "argv", ["uc-mcp-proxy", "--url", "https://example.com/mcp", "--profile", "MY_PROFILE"]):
        with patch("uc_mcp_proxy.__main__.run") as mock_run:
            mock_run.return_value = MagicMock()  # mock coroutine
            with patch("uc_mcp_proxy.__main__.asyncio.run"):
                from uc_mcp_proxy.__main__ import main

                main()
                mock_run.assert_called_once_with(
                    "https://example.com/mcp",
                    "MY_PROFILE",
                    None,
                    None,
                    verify_ssl=True,
                    no_auto_login=False,
                    client_id=None,
                    scopes=(),
                )


def test_creates_workspace_client_with_auth_type():
    """--auth-type is passed through to run()."""
    with (
        patch.object(
            sys, "argv", ["uc-mcp-proxy", "--url", "https://example.com/mcp", "--auth-type", "databricks-cli"]
        ),
        patch("uc_mcp_proxy.__main__.run") as mock_run,
    ):
        mock_run.return_value = MagicMock()
        with patch("uc_mcp_proxy.__main__.asyncio.run"):
            from uc_mcp_proxy.__main__ import main

            main()
            mock_run.assert_called_once_with(
                "https://example.com/mcp",
                None,
                "databricks-cli",
                None,
                verify_ssl=True,
                no_auto_login=False,
                client_id=None,
                scopes=(),
            )


def test_single_meta_parsed_correctly():
    """--meta KEY=VALUE is parsed into a dict and passed to run()."""
    with (
        patch.object(
            sys,
            "argv",
            [
                "uc-mcp-proxy",
                "--url",
                "https://example.com/mcp",
                "--meta",
                "warehouse_id=abc123",
            ],
        ),
        patch("uc_mcp_proxy.__main__.run") as mock_run,
    ):
        mock_run.return_value = MagicMock()
        with patch("uc_mcp_proxy.__main__.asyncio.run"):
            from uc_mcp_proxy.__main__ import main

            main()
            mock_run.assert_called_once_with(
                "https://example.com/mcp",
                None,
                None,
                {"warehouse_id": "abc123"},
                verify_ssl=True,
                no_auto_login=False,
                client_id=None,
                scopes=(),
            )


def test_multiple_meta_parsed_correctly():
    """Multiple --meta flags produce a dict with all entries."""
    with (
        patch.object(
            sys,
            "argv",
            [
                "uc-mcp-proxy",
                "--url",
                "https://example.com/mcp",
                "--meta",
                "warehouse_id=abc123",
                "--meta",
                "catalog=main",
            ],
        ),
        patch("uc_mcp_proxy.__main__.run") as mock_run,
    ):
        mock_run.return_value = MagicMock()
        with patch("uc_mcp_proxy.__main__.asyncio.run"):
            from uc_mcp_proxy.__main__ import main

            main()
            mock_run.assert_called_once_with(
                "https://example.com/mcp",
                None,
                None,
                {"warehouse_id": "abc123", "catalog": "main"},
                verify_ssl=True,
                no_auto_login=False,
                client_id=None,
                scopes=(),
            )


def test_meta_without_value_exits_with_error():
    """--meta bad (no =) produces a non-zero exit."""
    with patch.object(
        sys,
        "argv",
        [
            "uc-mcp-proxy",
            "--url",
            "https://example.com/mcp",
            "--meta",
            "bad",
        ],
    ):
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
                mock_run.assert_called_once_with(
                    "https://example.com/mcp",
                    None,
                    None,
                    None,
                    verify_ssl=True,
                    no_auto_login=False,
                    client_id=None,
                    scopes=(),
                )


def test_no_verify_ssl_passes_verify_ssl_false():
    """--no-verify-ssl passes verify_ssl=False to run()."""
    with patch.object(sys, "argv", ["uc-mcp-proxy", "--url", "https://example.com/mcp", "--no-verify-ssl"]):
        with patch("uc_mcp_proxy.__main__.run") as mock_run:
            mock_run.return_value = MagicMock()
            with patch("uc_mcp_proxy.__main__.asyncio.run"):
                from uc_mcp_proxy.__main__ import main

                main()
                mock_run.assert_called_once_with(
                    "https://example.com/mcp",
                    None,
                    None,
                    None,
                    verify_ssl=False,
                    no_auto_login=False,
                    client_id=None,
                    scopes=(),
                )


def test_no_verify_ssl_prints_warning(capsys):
    """--no-verify-ssl prints a warning to stderr."""
    with patch.object(sys, "argv", ["uc-mcp-proxy", "--url", "https://example.com/mcp", "--no-verify-ssl"]):
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


@pytest.mark.parametrize("verify_ssl", [True, False])
def test_run_builds_httpx_client_with_expected_verify(mock_workspace_client, verify_ssl):
    """run(verify_ssl=...) constructs httpx.AsyncClient with the matching verify= kwarg.

    This is the load-bearing plumbing check for --no-verify-ssl: prove the flag
    actually reaches the httpx constructor, not just the run() signature.
    """
    from uc_mcp_proxy.__main__ import run

    captured: dict = {}
    real_async_client = httpx.AsyncClient

    def spy(*args, **kwargs):
        captured.update(kwargs)
        return real_async_client(*args, **kwargs)

    @asynccontextmanager
    async def fake_stdio():
        send_a, recv_a = anyio.create_memory_object_stream(1)
        send_b, recv_b = anyio.create_memory_object_stream(1)
        # Close source sends so bridge()'s copy_stream tasks EOF immediately
        # and run() returns without hanging.
        await send_a.aclose()
        yield (recv_a, send_b)

    @asynccontextmanager
    async def fake_http(url, *, http_client=None, **kwargs):
        send_a, recv_a = anyio.create_memory_object_stream(1)
        send_b, recv_b = anyio.create_memory_object_stream(1)
        await send_a.aclose()
        yield (recv_a, send_b, lambda: "mock-session-id")

    with patch("uc_mcp_proxy.__main__.WorkspaceClient", return_value=mock_workspace_client):
        with patch("uc_mcp_proxy.__main__.stdio_server", side_effect=fake_stdio):
            with patch("uc_mcp_proxy.__main__.streamable_http_client", side_effect=fake_http):
                with patch("uc_mcp_proxy.__main__.httpx.AsyncClient", side_effect=spy):
                    anyio.run(run, "https://example.com/mcp", None, None, None, verify_ssl, True)

    assert captured.get("verify") is verify_ssl


def test_client_id_is_passed_to_run():
    """--client-id reaches run() as a kwarg."""
    with patch.object(
        sys, "argv", ["uc-mcp-proxy", "--url", "https://example.com/mcp", "--client-id", "app-client-id"]
    ):
        with patch("uc_mcp_proxy.__main__.run") as mock_run:
            mock_run.return_value = MagicMock()
            with patch("uc_mcp_proxy.__main__.asyncio.run"):
                from uc_mcp_proxy.__main__ import main

                main()
                _, kwargs = mock_run.call_args
                assert kwargs["client_id"] == "app-client-id"


def test_single_scope_is_parsed():
    """A single --scope value is parsed into a one-element tuple."""
    with patch.object(
        sys,
        "argv",
        ["uc-mcp-proxy", "--url", "https://example.com/mcp", "--client-id", "app-client-id", "--scope", "sql"],
    ):
        with patch("uc_mcp_proxy.__main__.run") as mock_run:
            mock_run.return_value = MagicMock()
            with patch("uc_mcp_proxy.__main__.asyncio.run"):
                from uc_mcp_proxy.__main__ import main

                main()
                _, kwargs = mock_run.call_args
                assert kwargs["scopes"] == ("sql",)


def test_repeated_scope_flags_are_collected():
    """Repeated --scope flags accumulate in the order given."""
    with patch.object(
        sys,
        "argv",
        [
            "uc-mcp-proxy",
            "--url",
            "https://example.com/mcp",
            "--client-id",
            "app-client-id",
            "--scope",
            "sql",
            "--scope",
            "dashboards.genie",
        ],
    ):
        with patch("uc_mcp_proxy.__main__.run") as mock_run:
            mock_run.return_value = MagicMock()
            with patch("uc_mcp_proxy.__main__.asyncio.run"):
                from uc_mcp_proxy.__main__ import main

                main()
                _, kwargs = mock_run.call_args
                assert kwargs["scopes"] == ("sql", "dashboards.genie")


def test_space_separated_scope_value_is_split():
    """A single --scope value with embedded spaces splits into the same tuple as repeated flags."""
    with patch.object(
        sys,
        "argv",
        [
            "uc-mcp-proxy",
            "--url",
            "https://example.com/mcp",
            "--client-id",
            "app-client-id",
            "--scope",
            "sql dashboards.genie",
        ],
    ):
        with patch("uc_mcp_proxy.__main__.run") as mock_run:
            mock_run.return_value = MagicMock()
            with patch("uc_mcp_proxy.__main__.asyncio.run"):
                from uc_mcp_proxy.__main__ import main

                main()
                _, kwargs = mock_run.call_args
                assert kwargs["scopes"] == ("sql", "dashboards.genie")


def test_scopes_are_deduped_preserving_first_seen_order():
    """Dedupe order must be deterministic.

    The resulting tuple is joined into the exchange request body, and this
    repo runs tests in randomized order (pytest-randomly), so a set-based
    dedupe would surface as an intermittent failure with a nearly invisible
    cause.
    """
    with patch.object(
        sys,
        "argv",
        [
            "uc-mcp-proxy",
            "--url",
            "https://example.com/mcp",
            "--client-id",
            "app-client-id",
            "--scope",
            "b a",
            "--scope",
            "b",
            "--scope",
            "c",
        ],
    ):
        with patch("uc_mcp_proxy.__main__.run") as mock_run:
            mock_run.return_value = MagicMock()
            with patch("uc_mcp_proxy.__main__.asyncio.run"):
                from uc_mcp_proxy.__main__ import main

                main()
                _, kwargs = mock_run.call_args
                assert kwargs["scopes"] == ("b", "a", "c")


def test_no_scope_produces_empty_tuple():
    """--client-id with no --scope at all produces scopes=()."""
    with patch.object(
        sys, "argv", ["uc-mcp-proxy", "--url", "https://example.com/mcp", "--client-id", "app-client-id"]
    ):
        with patch("uc_mcp_proxy.__main__.run") as mock_run:
            mock_run.return_value = MagicMock()
            with patch("uc_mcp_proxy.__main__.asyncio.run"):
                from uc_mcp_proxy.__main__ import main

                main()
                _, kwargs = mock_run.call_args
                assert kwargs["scopes"] == ()


def test_scope_without_client_id_exits_one(capsys):
    """--scope without --client-id is rejected with exit code 1 and a message naming --client-id."""
    with patch.object(sys, "argv", ["uc-mcp-proxy", "--url", "https://example.com/mcp", "--scope", "sql"]):
        with pytest.raises(SystemExit) as exc_info:
            from uc_mcp_proxy.__main__ import main

            main()
        assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "--client-id" in captured.err


def test_client_id_with_non_pat_auth_type_exits_before_preflight():
    """--client-id combined with an explicit non-pat --auth-type exits before _preflight_authenticate runs.

    _preflight_authenticate can open a browser, and doing that only to reject
    the invocation afterward is the worst first experience for the commonest
    misconfiguration.
    """
    with patch.object(
        sys,
        "argv",
        [
            "uc-mcp-proxy",
            "--url",
            "https://example.com/mcp",
            "--client-id",
            "app-client-id",
            "--auth-type",
            "databricks-cli",
        ],
    ):
        with patch("uc_mcp_proxy.__main__._preflight_authenticate") as mock_preflight:
            with patch("uc_mcp_proxy.__main__.run") as mock_run:
                mock_run.return_value = MagicMock()
                with patch("uc_mcp_proxy.__main__.asyncio.run"):
                    from uc_mcp_proxy.__main__ import main

                    with pytest.raises(SystemExit):
                        main()
                    mock_preflight.assert_not_called()


def test_client_id_with_non_pat_auth_type_exits_under_no_auto_login_too():
    """The same rejection fires even with --no-auto-login, which would otherwise skip preflight entirely."""
    with patch.object(
        sys,
        "argv",
        [
            "uc-mcp-proxy",
            "--url",
            "https://example.com/mcp",
            "--client-id",
            "app-client-id",
            "--auth-type",
            "databricks-cli",
            "--no-auto-login",
        ],
    ):
        with patch("uc_mcp_proxy.__main__._preflight_authenticate") as mock_preflight:
            with patch("uc_mcp_proxy.__main__.run") as mock_run:
                mock_run.return_value = MagicMock()
                with patch("uc_mcp_proxy.__main__.asyncio.run"):
                    from uc_mcp_proxy.__main__ import main

                    with pytest.raises(SystemExit):
                        main()
                    mock_preflight.assert_not_called()


def test_client_id_with_explicit_pat_auth_type_is_accepted():
    """Non-vacuous counterpart to the rejection tests: --auth-type pat is a valid combination with --client-id."""
    with patch.object(
        sys,
        "argv",
        [
            "uc-mcp-proxy",
            "--url",
            "https://example.com/mcp",
            "--client-id",
            "app-client-id",
            "--auth-type",
            "pat",
        ],
    ):
        with patch("uc_mcp_proxy.__main__.run") as mock_run:
            mock_run.return_value = MagicMock()
            with patch("uc_mcp_proxy.__main__.asyncio.run"):
                from uc_mcp_proxy.__main__ import main

                main()
                mock_run.assert_called_once()


def test_client_id_without_auth_type_defers_to_run():
    """--client-id with no --auth-type is not rejected at parse time.

    The authoritative check lives in run(), since real PAT profiles usually
    name no auth_type at all.
    """
    with patch.object(
        sys, "argv", ["uc-mcp-proxy", "--url", "https://example.com/mcp", "--client-id", "app-client-id"]
    ):
        with patch("uc_mcp_proxy.__main__.run") as mock_run:
            mock_run.return_value = MagicMock()
            with patch("uc_mcp_proxy.__main__.asyncio.run"):
                from uc_mcp_proxy.__main__ import main

                main()
                mock_run.assert_called_once()


def test_no_verify_ssl_with_client_id_warns_about_the_request_body(capsys):
    """--no-verify-ssl combined with --client-id adds a warning naming the request body."""
    with patch.object(
        sys,
        "argv",
        [
            "uc-mcp-proxy",
            "--url",
            "https://example.com/mcp",
            "--no-verify-ssl",
            "--client-id",
            "app-client-id",
        ],
    ):
        with patch("uc_mcp_proxy.__main__.run") as mock_run:
            mock_run.return_value = MagicMock()
            with patch("uc_mcp_proxy.__main__.asyncio.run"):
                from uc_mcp_proxy.__main__ import main

                main()
        captured = capsys.readouterr()
        assert "warning" in captured.err.lower()
        assert "ssl" in captured.err.lower()
        assert "body" in captured.err.lower()


def test_no_verify_ssl_without_client_id_does_not_warn_about_a_body(capsys):
    """Non-vacuous counterpart: without --client-id there is no body warning, only the plain SSL one."""
    with patch.object(sys, "argv", ["uc-mcp-proxy", "--url", "https://example.com/mcp", "--no-verify-ssl"]):
        with patch("uc_mcp_proxy.__main__.run") as mock_run:
            mock_run.return_value = MagicMock()
            with patch("uc_mcp_proxy.__main__.asyncio.run"):
                from uc_mcp_proxy.__main__ import main

                main()
        captured = capsys.readouterr()
        assert "warning" in captured.err.lower()
        assert "body" not in captured.err.lower()


def test_parse_scopes_empty_list_produces_empty_tuple():
    """_parse_scopes([]) -> ()."""
    from uc_mcp_proxy.__main__ import _parse_scopes

    assert _parse_scopes([]) == ()


def test_parse_scopes_splits_on_multiple_spaces():
    """_parse_scopes(["a b  c"]) -> ("a", "b", "c"), tolerating multiple spaces."""
    from uc_mcp_proxy.__main__ import _parse_scopes

    assert _parse_scopes(["a b  c"]) == ("a", "b", "c")


def test_parse_scopes_dedupes_exact_duplicates():
    """_parse_scopes(["a", "a"]) -> ("a",)."""
    from uc_mcp_proxy.__main__ import _parse_scopes

    assert _parse_scopes(["a", "a"]) == ("a",)


def test_proxy_client_refuses_compressed_responses(mock_workspace_client):
    """Diagnostic reads are capped in bytes, and compression defeats a byte cap.

    httpx decodes on the response's ``Content-Encoding`` before yielding, so a
    gzip body can expand three orders of magnitude past the cap inside a single
    chunk. Neither an error body nor an SSE stream gains anything from being
    compressed. Asserted because a future ``headers=`` argument here would
    silently drop it.
    """
    from uc_mcp_proxy.__main__ import _build_http_client
    from uc_mcp_proxy.errors import HttpErrorReporter

    reporter = HttpErrorReporter(url="https://example.com/mcp", profile="p", auth_type="pat")
    client = _build_http_client(auth=MagicMock(), verify_ssl=True, reporter=reporter)

    assert client.headers["accept-encoding"] == "identity"


def test_exchange_client_refuses_compressed_responses():
    """Same bound on the token-exchange client, which reads on the event loop."""
    import httpx as _httpx

    from uc_mcp_proxy.token_exchange import ExchangeConfig, exchange_pat

    seen: list[str] = []

    def handler(request: _httpx.Request) -> _httpx.Response:
        seen.append(request.headers.get("accept-encoding", ""))
        return _httpx.Response(200, json={"access_token": "t"})

    exchange_pat(
        "dapi-fake",
        ExchangeConfig(host="https://w.cloud.databricks.com", client_id="c", scopes=(), verify_ssl=True),
        transport=_httpx.MockTransport(handler),
    )

    assert seen == ["identity"]
