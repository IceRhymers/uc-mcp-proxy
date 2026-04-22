"""MCP stdio-to-Streamable-HTTP proxy with Databricks OAuth."""

from __future__ import annotations

import argparse
import asyncio
import sys
from typing import Generator, AsyncGenerator

import anyio
import httpx
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from databricks.sdk import WorkspaceClient
from mcp.client.streamable_http import streamablehttp_client
from mcp.server.stdio import stdio_server
from mcp.shared.message import SessionMessage
from mcp.types import JSONRPCRequest


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
            request.headers["X-Forwarded-Access-Token"] = auth_value[len("Bearer "):]

    def sync_auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        self._apply_headers(request)
        yield request

    async def async_auth_flow(self, request: httpx.Request) -> AsyncGenerator[httpx.Request, httpx.Response]:
        self._apply_headers(request)
        yield request


async def copy_stream(source: MemoryObjectReceiveStream, dest: MemoryObjectSendStream) -> None:
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
    source: MemoryObjectReceiveStream,
    dest: MemoryObjectSendStream,
    meta: dict[str, str],
) -> None:
    """Like copy_stream, but applies inject_meta to each forwarded message."""
    try:
        async for message in source:
            await dest.send(inject_meta(message, meta))
    finally:
        await dest.aclose()


async def bridge(
    stdio_read: MemoryObjectReceiveStream,
    stdio_write: MemoryObjectSendStream,
    http_read: MemoryObjectReceiveStream,
    http_write: MemoryObjectSendStream,
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


async def run(
    url: str,
    profile: str | None = None,
    auth_type: str | None = None,
    meta: dict[str, str] | None = None,
    verify_ssl: bool = True,
) -> None:
    """Run the proxy: bridge stdio transport to Streamable HTTP with Databricks OAuth."""
    kwargs: dict = {}
    if profile:
        kwargs["profile"] = profile
    if auth_type:
        kwargs["auth_type"] = auth_type
    client = WorkspaceClient(**kwargs)
    auth = DatabricksAuth(client)

    async with stdio_server() as (stdio_read, stdio_write):
        async with httpx.AsyncClient(verify=verify_ssl) as httpx_client:
            async with streamablehttp_client(url, auth=auth, httpx_client=httpx_client) as (
                http_read,
                http_write,
                _get_session_id,
            ):
                await bridge(stdio_read, stdio_write, http_read, http_write, meta)


def main() -> None:
    """CLI entry point: parse args and run the proxy."""
    parser = argparse.ArgumentParser(
        description="MCP stdio-to-Streamable-HTTP proxy with Databricks OAuth",
    )
    parser.add_argument("--url", required=True, help="Remote MCP server URL")
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
    args = parser.parse_args()

    if args.no_verify_ssl:
        print(
            "warning: SSL certificate verification is disabled (--no-verify-ssl). "
            "Use only in trusted environments.",
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

    asyncio.run(run(args.url, args.profile, args.auth_type, meta, verify_ssl=not args.no_verify_ssl))


if __name__ == "__main__":  # pragma: no cover
    main()
