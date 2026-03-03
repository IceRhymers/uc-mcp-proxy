"""MCP stdio-to-Streamable-HTTP proxy with Databricks OAuth."""

from __future__ import annotations

import argparse
import asyncio
from typing import Generator, AsyncGenerator

import anyio
import httpx
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from databricks.sdk import WorkspaceClient
from mcp.client.streamable_http import streamablehttp_client
from mcp.server.stdio import stdio_server


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


async def bridge(
    stdio_read: MemoryObjectReceiveStream,
    stdio_write: MemoryObjectSendStream,
    http_read: MemoryObjectReceiveStream,
    http_write: MemoryObjectSendStream,
) -> None:
    """Bidirectional bridge between stdio and HTTP stream pairs."""
    async with anyio.create_task_group() as tg:
        tg.start_soon(copy_stream, stdio_read, http_write)
        tg.start_soon(copy_stream, http_read, stdio_write)


async def run(url: str, profile: str | None = None, auth_type: str | None = None) -> None:
    """Run the proxy: bridge stdio transport to Streamable HTTP with Databricks OAuth."""
    kwargs: dict = {}
    if profile:
        kwargs["profile"] = profile
    if auth_type:
        kwargs["auth_type"] = auth_type
    client = WorkspaceClient(**kwargs)
    auth = DatabricksAuth(client)

    async with stdio_server() as (stdio_read, stdio_write):
        async with streamablehttp_client(url, auth=auth) as (
            http_read,
            http_write,
            _get_session_id,
        ):
            await bridge(stdio_read, stdio_write, http_read, http_write)


def main() -> None:
    """CLI entry point: parse args and run the proxy."""
    parser = argparse.ArgumentParser(
        description="MCP stdio-to-Streamable-HTTP proxy with Databricks OAuth",
    )
    parser.add_argument("--url", required=True, help="Remote MCP server URL")
    parser.add_argument("--profile", default=None, help="Databricks CLI profile")
    parser.add_argument("--auth-type", default=None, help="Databricks auth type (e.g. databricks-cli)")
    args = parser.parse_args()
    asyncio.run(run(args.url, args.profile, args.auth_type))


if __name__ == "__main__":  # pragma: no cover
    main()
