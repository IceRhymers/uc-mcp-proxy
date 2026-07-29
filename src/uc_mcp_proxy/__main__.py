"""MCP stdio-to-Streamable-HTTP proxy with Databricks OAuth."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections.abc import AsyncGenerator, Generator
from typing import Any
from urllib.parse import urljoin, urlsplit

import anyio
import httpx
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from databricks.sdk import WorkspaceClient
from mcp.client.streamable_http import streamable_http_client
from mcp.server.stdio import stdio_server
from mcp.shared.message import SessionMessage
from mcp.types import JSONRPCRequest

from uc_mcp_proxy.auth import _preflight_authenticate
from uc_mcp_proxy.errors import HttpErrorReporter, is_only_http_status_errors, stamp_role


class DatabricksAuth(httpx.Auth):
    """httpx Auth that injects fresh Databricks OAuth tokens per-request.

    Calls ``WorkspaceClient.config.authenticate()`` on every request to obtain
    a current OAuth bearer token, ensuring tokens are never stale.
    """

    def __init__(self, client: WorkspaceClient) -> None:
        self._client = client

    def _apply_headers(self, request: httpx.Request) -> None:
        headers = self._client.config.authenticate()
        request.headers.update(headers)
        # Also forward the token so the Databricks App can use per-user identity
        auth_value = headers.get("Authorization", "")
        if auth_value.startswith("Bearer "):
            request.headers["X-Forwarded-Access-Token"] = auth_value[len("Bearer ") :]

    def sync_auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        self._apply_headers(request)
        yield request

    async def async_auth_flow(self, request: httpx.Request) -> AsyncGenerator[httpx.Request, httpx.Response]:
        self._apply_headers(request)
        yield request


async def copy_stream(source: MemoryObjectReceiveStream[Any], dest: MemoryObjectSendStream[Any]) -> None:
    """Copy all messages from source to dest, closing dest when source is exhausted."""
    try:
        async for message in source:
            await dest.send(message)
    finally:
        await dest.aclose()


def inject_meta(
    message: SessionMessage | Exception,
    meta: dict[str, str],
) -> SessionMessage | Exception:
    """Merge ``meta`` into ``params._meta`` on ``tools/call`` requests.

    Exceptions, notifications, responses, and non-``tools/call`` requests pass
    through unchanged. Mutates the incoming ``SessionMessage`` in place — this
    matches how the MCP SDK streams deliver per-message objects (not shared).
    On key collision the proxy value wins and a warning is printed to stderr.
    """
    # Scoped to tools/call because that is the only method Databricks documents
    # for meta params today; _meta is valid on any request per MCP spec.
    if isinstance(message, Exception):
        return message
    root = message.message.root
    if not isinstance(root, JSONRPCRequest) or root.method != "tools/call":
        return message
    if root.params is None:
        root.params = {}
    existing = root.params.get("_meta") or {}
    for key, value in meta.items():
        if key in existing:
            print(
                f"warning: --meta {key!r} overrides client _meta.{key}",
                file=sys.stderr,
            )
        existing[key] = value
    root.params["_meta"] = existing
    return message


async def inject_meta_stream(
    source: MemoryObjectReceiveStream[Any],
    dest: MemoryObjectSendStream[Any],
    meta: dict[str, str],
) -> None:
    """Like copy_stream, but applies inject_meta to each forwarded message."""
    try:
        async for message in source:
            await dest.send(inject_meta(message, meta))
    finally:
        await dest.aclose()


async def bridge(
    stdio_read: MemoryObjectReceiveStream[Any],
    stdio_write: MemoryObjectSendStream[Any],
    http_read: MemoryObjectReceiveStream[Any],
    http_write: MemoryObjectSendStream[Any],
    meta: dict[str, str] | None = None,
) -> None:
    """Bidirectional bridge between stdio and HTTP stream pairs.

    When ``meta`` is set, client→server messages are rewritten to carry
    proxy-configured ``_meta`` on ``tools/call`` requests. The server→client
    direction is always a transparent copy.
    """
    async with anyio.create_task_group() as tg:
        if meta:
            tg.start_soon(inject_meta_stream, stdio_read, http_write, meta)
        else:
            tg.start_soon(copy_stream, stdio_read, http_write)
        tg.start_soon(copy_stream, http_read, stdio_write)


def _resolve_url(url: str, client: WorkspaceClient) -> str:
    """Resolve ``url`` against the workspace host from ``client.config``.

    If ``url`` already has a scheme (e.g. ``https://...``) it is returned
    unchanged. Otherwise it is joined against ``client.config.host`` so that
    callers can pass a workspace-relative path like ``/api/2.0/mcp/foo``.
    """
    if urlsplit(url).scheme:
        return url
    host = client.config.host
    if not host:
        raise SystemExit(
            f"uc-mcp-proxy: --url {url!r} is relative but no workspace host is configured in the Databricks profile."
        )
    base = host if host.endswith("/") else host + "/"
    return urljoin(base, url.lstrip("/"))


#: Exit status when the remote server refused a request the proxy needed.
_EXIT_HTTP_ERROR = 1


def _abort() -> None:
    """End the process now, skipping the normal async unwind.

    Returning from ``run()`` is not enough to make the proxy exit.
    ``stdio_server`` yields from inside its own task group, whose
    ``stdin_reader`` sits in ``async for line in stdin`` -- a blocking
    ``readline`` on a worker thread. anyio cannot cancel a blocking thread
    read, so that task group never finishes and ``__aexit__`` waits forever.
    A live MCP client holds stdin open indefinitely, so in practice the proxy
    would print its diagnosis and then hang, which is worse than the crash
    this module set out to replace: it claims to be exiting and does not.

    The diagnosis is already on stderr by the time this runs, so the only
    thing left to preserve is the buffers. Patched out in tests, which use a
    stdio stub that unwinds cleanly and can therefore assert on ``SystemExit``.
    """
    sys.stderr.flush()
    sys.stdout.flush()
    os._exit(_EXIT_HTTP_ERROR)


def _build_http_client(
    *,
    auth: httpx.Auth,
    verify_ssl: bool,
    reporter: HttpErrorReporter,
    transport: httpx.AsyncBaseTransport | None = None,
) -> httpx.AsyncClient:
    """Build the HTTP client the MCP SDK will use, wired for error reporting.

    ``transport`` is a test-only seam and is mutually exclusive with
    ``verify_ssl``: httpx ignores ``verify`` when a transport is supplied.
    """
    return httpx.AsyncClient(
        follow_redirects=True,
        verify=verify_ssl,
        timeout=httpx.Timeout(30.0, read=300.0),
        auth=auth,
        transport=transport,
        event_hooks={"request": [stamp_role], "response": [reporter.on_response]},
    )


async def run(
    url: str,
    profile: str | None = None,
    auth_type: str | None = None,
    meta: dict[str, str] | None = None,
    verify_ssl: bool = True,
    no_auto_login: bool = False,
    transport: httpx.AsyncBaseTransport | None = None,
) -> None:
    """Run the proxy: bridge stdio transport to Streamable HTTP with Databricks OAuth.

    ``url`` may be absolute or workspace-relative; relative values are resolved
    against ``client.config.host`` from the Databricks profile.

    Raises ``SystemExit`` with a diagnosis when the remote server refuses a
    request the proxy needed to make. ``transport`` is a test-only seam.
    """
    if no_auto_login:
        kwargs: dict[str, Any] = {}
        if profile:
            kwargs["profile"] = profile
        if auth_type:
            kwargs["auth_type"] = auth_type
        client = WorkspaceClient(**kwargs)
    else:
        client = _preflight_authenticate(profile, auth_type)
    auth = DatabricksAuth(client)
    resolved_url = _resolve_url(url, client)
    reporter = HttpErrorReporter(
        url=resolved_url,
        profile=client.config.profile or "DEFAULT",
        auth_type=client.config.auth_type or "(auto-detect)",
    )

    try:
        async with (
            stdio_server() as (stdio_read, stdio_write),
            _build_http_client(
                auth=auth,
                verify_ssl=verify_ssl,
                reporter=reporter,
                transport=transport,
            ) as httpx_client,
            streamable_http_client(
                resolved_url,
                http_client=httpx_client,
            ) as (
                http_read,
                http_write,
                _get_session_id,
            ),
        ):
            try:
                async with anyio.create_task_group() as tg:

                    async def _watch_fatal() -> None:
                        await reporter.fatal.wait()
                        tg.cancel_scope.cancel()
                        _abort()

                    tg.start_soon(_watch_fatal)
                    await bridge(stdio_read, stdio_write, http_read, http_write, meta)
                    tg.cancel_scope.cancel()
            finally:
                # Set before the transports unwind so the SDK's teardown DELETE
                # is not reported. In a ``finally`` because the SDK raising is
                # the ordinary exit path, not just the cancelled one.
                reporter.shutting_down = True
    except BaseException as exc:
        # For every request-role failure the hook fires first, so the SDK's own
        # ``raise_for_status`` still unwinds behind us. Swallow that only when
        # we already diagnosed it and nothing unrelated rode along -- otherwise
        # a Ctrl-C would be reported as a credential rejection.
        if not reporter.reported or not is_only_http_status_errors(exc):
            raise
        reporter.fatal_message = reporter.fatal_message or reporter.last_message

    if reporter.fatal_message is not None:
        # Exit by status, not by message: the hook already printed the
        # diagnosis, and ``SystemExit(<str>)`` would make CPython print it a
        # second time. ``fatal_message`` stays the internal record.
        raise SystemExit(_EXIT_HTTP_ERROR)


def main() -> None:
    """CLI entry point: parse args and run the proxy."""
    parser = argparse.ArgumentParser(
        description="MCP stdio-to-Streamable-HTTP proxy with Databricks OAuth",
    )
    parser.add_argument(
        "--url",
        required=True,
        help=(
            "Remote MCP server URL. Accepts an absolute URL "
            "(https://workspace/api/2.0/mcp/...) or a workspace-relative path "
            "(/api/2.0/mcp/...), which is resolved against the host from the "
            "Databricks profile."
        ),
    )
    parser.add_argument("--profile", default=None, help="Databricks CLI profile")
    parser.add_argument("--auth-type", default=None, help="Databricks auth type (e.g. databricks-cli)")
    parser.add_argument(
        "--meta",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help=(
            "Meta parameter injected into the JSON-RPC tools/call _meta object "
            "(e.g. --meta warehouse_id=abc123). Repeatable. Proxy values win "
            "on key collision with client-provided _meta."
        ),
    )
    parser.add_argument(
        "--no-verify-ssl",
        action="store_true",
        help="Disable SSL certificate verification (for self-signed certificates).",
    )
    parser.add_argument(
        "--no-auto-login",
        action="store_true",
        help=(
            "Skip the auto-login preflight. Fail immediately if credentials are "
            "missing or expired. Use in CI / headless contexts where no browser "
            "is available."
        ),
    )
    args = parser.parse_args()

    if args.no_verify_ssl:
        print(
            "warning: SSL certificate verification is disabled (--no-verify-ssl). Use only in trusted environments.",
            file=sys.stderr,
        )

    meta: dict[str, str] | None = None
    if args.meta:
        meta = {}
        for m in args.meta:
            key, _, value = m.partition("=")
            if not value:
                print(f"Error: --meta must be KEY=VALUE, got: {m!r}", file=sys.stderr)
                sys.exit(1)
            meta[key] = value

    asyncio.run(
        run(
            args.url,
            args.profile,
            args.auth_type,
            meta,
            verify_ssl=not args.no_verify_ssl,
            no_auto_login=args.no_auto_login,
        )
    )


if __name__ == "__main__":  # pragma: no cover
    main()
