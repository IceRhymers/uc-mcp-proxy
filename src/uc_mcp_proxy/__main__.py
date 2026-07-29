"""MCP stdio-to-Streamable-HTTP proxy with Databricks OAuth."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from collections.abc import AsyncGenerator, Callable, Generator
from typing import Any, NoReturn
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
from uc_mcp_proxy.errors import (
    HttpErrorReporter,
    arm_retry,
    is_only_diagnosed_errors,
    set_remediation,
    stamp_role,
)
from uc_mcp_proxy.token_exchange import ExchangeConfig, ExchangedToken, TokenExchangeError, exchange_pat

#: Substituted for ``errors.py``'s generic 401/403 remediation whenever the
#: exchange is active. Deliberately status-neutral, and deliberately silent
#: about OAuth U2M: a browser login is the one remedy this feature exists to
#: make unnecessary, and the audience for it has no browser.
_EXCHANGE_REMEDIATION = (
    "The PAT was exchanged for an app-scoped OAuth token and the app still refused it. "
    "Check that --client-id is the app's `oauth2_app_client_id` and that --scope covers "
    "its `effective_user_api_scopes` (`databricks apps get <app-name> -o json`), and that "
    "this identity has CAN USE on the app."
)


def _never_shutting_down() -> bool:
    return False


class DatabricksAuth(httpx.Auth):
    """httpx Auth that injects fresh Databricks credentials per-request.

    Calls ``WorkspaceClient.config.authenticate()`` on every request to obtain
    a current bearer token, ensuring tokens are never stale.

    With ``exchange`` set, the profile's PAT is additionally traded for an
    app-scoped OAuth token via RFC 8693 (see ``token_exchange``), cached until
    it nears expiry, and re-minted once on a 401. Without it, behaviour is
    unchanged apart from the forwarded-header rule below.

    There is deliberately no ``requires_request_body``. httpx reads that flag
    only inside its *base* ``sync_auth_flow``/``async_auth_flow``, and this
    class overrides both -- setting it would be dead code that reads like a
    guarantee. The flows below materialize the body themselves.
    """

    def __init__(
        self,
        client: WorkspaceClient,
        *,
        exchange: ExchangeConfig | None = None,
        on_fatal: Callable[[str], None] | None = None,
        shutting_down: Callable[[], bool] = _never_shutting_down,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._client = client
        self._exchange = exchange
        self._on_fatal = on_fatal
        self._shutting_down = shutting_down
        self._transport = transport
        self._cached: ExchangedToken | None = None
        # A re-entrancy tripwire, NOT a lock. On a single-threaded event loop a
        # lock protects nothing here, and its one reachable failure mode is a
        # permanent whole-process deadlock: a future ``await`` in the critical
        # section lets task A hold the lock across a yield while task B blocks
        # the only thread, taking ``_watch_fatal`` down with it so ``_abort``
        # can never fire. This raises instead, loudly and recoverably.
        self._exchanging = False

    def _fail(self, message: str, *, cause: BaseException | None = None) -> NoReturn:
        """Diagnose a proxy-side auth failure and abandon this request.

        Always raises. The ``NoReturn`` is not decoration: returning here would
        dispatch the request with whatever credential happened to be on it,
        which on this path is nothing at all.
        """
        if self._on_fatal is not None:
            self._on_fatal(message)
        raise TokenExchangeError(message) from cause

    def _token(self, pat: str, exchange: ExchangeConfig, *, stale: str | None = None) -> str:
        """Return a usable app-scoped token, minting one only when required.

        Synchronous on purpose. Read-check-exchange-store is atomic with respect
        to other tasks on the shared event loop *only because there is no await
        point inside it*. Do not introduce ``await`` or
        ``anyio.to_thread.run_sync`` here without reworking the tripwire above
        into real mutual exclusion -- a well-meaning "don't block the loop"
        refactor is exactly what turns one exchange an hour into a thundering
        herd of concurrent ones.

        ``stale`` is the token a 401 just invalidated, and the first branch is
        what collapses concurrent 401s onto a single exchange.
        """
        cached = self._cached
        # (a) Another task already replaced what we sent, so it is by
        #     definition fresher. Expiry is irrelevant and must not be
        #     consulted: re-checking it here is how a dedupe silently stops
        #     deduping and every request pays for an exchange.
        if cached is not None and stale is not None and cached.access_token != stale:
            return cached.access_token
        # (b) The ordinary cache hit.
        if cached is not None and stale is None and not cached.expired(time.monotonic()):
            return cached.access_token
        # (c) Mint one.
        if self._exchanging:
            self._fail(
                "uc-mcp-proxy: internal error -- the token exchange re-entered itself, which "
                "means an await point was introduced into the synchronous exchange path. "
                "Please report this."
            )
        self._exchanging = True
        try:
            fresh = exchange_pat(pat, exchange, transport=self._transport)
        except TokenExchangeError as exc:
            self._fail(str(exc), cause=exc)
        finally:
            # In a ``finally`` so a failed exchange does not brick auth for the
            # rest of the process.
            self._exchanging = False
        self._cached = fresh
        return fresh.access_token

    def _apply_headers(self, request: httpx.Request, *, stale: str | None = None) -> None:
        headers = self._client.config.authenticate()

        if self._exchange is None:
            request.headers.update(headers)
            auth_value = headers.get("Authorization", "")
            # Forward the token so a Databricks App can use per-user identity --
            # but never for a PAT profile. Apps hand this header to arbitrary
            # user-authored code, a PAT is long-lived and full-privilege where
            # the platform's own value is a short-lived scoped token, and
            # httpx's redirect handling strips ``Authorization`` on a
            # cross-origin hop but not this header, so a redirect would carry
            # the PAT to a foreign origin with the real credential already
            # removed. Do not restore the unconditional form.
            if auth_value.startswith("Bearer ") and self._client.config.auth_type != "pat":
                request.headers["X-Forwarded-Access-Token"] = auth_value[len("Bearer ") :]
            return

        pat = headers.get("Authorization", "").removeprefix("Bearer ")
        if not pat:
            self._fail(
                f"uc-mcp-proxy: profile {self._client.config.profile or 'DEFAULT'!r} produced no "
                f"Authorization header, so there is no PAT to exchange. Check that the profile "
                f"still has a `token = ...` entry, or that DATABRICKS_TOKEN is set."
            )
        token = self._token(pat, self._exchange, stale=stale)
        # Nothing is written until the exchange has succeeded. Updating first
        # and overwriting after would leave the raw PAT on the request in the
        # window where the exchange raises -- and that window is exactly the
        # common failure (a wrong --client-id), dispatching the credential to
        # the app host.
        request.headers.update({key: value for key, value in headers.items() if key.lower() != "authorization"})
        request.headers["Authorization"] = f"Bearer {token}"
        request.headers.pop("X-Forwarded-Access-Token", None)
        if not self._shutting_down():
            arm_retry(request, armed=stale is None)
            set_remediation(request, _EXCHANGE_REMEDIATION)

    def _should_retry(self, response: httpx.Response) -> bool:
        """True if this 401 gets one more attempt with a freshly minted token.

        Never a loop: a second 401 is permanent by definition, and the app's
        401 is empty-bodied and identical to the no-credential one, so there is
        no way to tell expiry from permission loss -- and no need to.
        """
        return self._exchange is not None and response.status_code == 401 and not self._shutting_down()

    def _stale_token(self, request: httpx.Request) -> str:
        """The token the failed attempt carried, bare so it compares to the cache."""
        authorization: str = request.headers.get("Authorization", "")
        return authorization.removeprefix("Bearer ")

    def sync_auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        if self._exchange is not None:
            request.read()
        self._apply_headers(request)
        response = yield request
        if not self._should_retry(response):
            return
        self._apply_headers(request, stale=self._stale_token(request))
        yield request

    async def async_auth_flow(self, request: httpx.Request) -> AsyncGenerator[httpx.Request, httpx.Response]:
        if self._exchange is not None:
            # A no-op for every request this proxy issues today -- httpx reads
            # ``json=`` and empty bodies at construction, and this short-circuits
            # on the already-materialized content. It exists so that a future
            # streaming body cannot silently break the retry below, which
            # re-sends the same request object.
            await request.aread()
        self._apply_headers(request)
        response = yield request
        if not self._should_retry(response):
            return
        # Re-yield the ORIGINAL request, never ``response.request``. Under a
        # redirect the latter is that hop's snapshot -- httpx copies extensions
        # per hop rather than sharing them -- so disarming it would write into a
        # dict that is then discarded, the rebuilt retry would still read as
        # armed, both 401s would be suppressed, and the proxy would hang with no
        # diagnosis at all: strictly worse than the traceback it replaced.
        self._apply_headers(request, stale=self._stale_token(request))
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


def _client_id_requires_pat(auth_type: str | None, profile: str | None) -> str:
    """Diagnosis for ``--client-id`` on a profile that has no PAT to exchange."""
    return (
        f"uc-mcp-proxy: --client-id enables RFC 8693 token exchange, which is implemented only "
        f"for PAT subject tokens. Profile {profile or 'DEFAULT'!r} resolved to "
        f"auth_type={auth_type or '(auto-detect)'}. Drop --client-id, or use a PAT profile. "
        f"(DATABRICKS_TOKEN also resolves to 'pat'.)"
    )


async def run(
    url: str,
    profile: str | None = None,
    auth_type: str | None = None,
    meta: dict[str, str] | None = None,
    verify_ssl: bool = True,
    no_auto_login: bool = False,
    transport: httpx.AsyncBaseTransport | None = None,
    *,
    client_id: str | None = None,
    scopes: tuple[str, ...] = (),
    exchange_transport: httpx.BaseTransport | None = None,
) -> None:
    """Run the proxy: bridge stdio transport to Streamable HTTP with Databricks OAuth.

    ``url`` may be absolute or workspace-relative; relative values are resolved
    against ``client.config.host`` from the Databricks profile.

    ``client_id`` turns on the RFC 8693 exchange: the profile's PAT is traded
    for a token scoped to that Databricks App. ``scopes`` is optional.

    Raises ``SystemExit`` with a diagnosis when the remote server refuses a
    request the proxy needed to make. ``transport`` and ``exchange_transport``
    are test-only seams.
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
    resolved_url = _resolve_url(url, client)
    # Built before the auth object, which needs its ``report_fatal`` as the
    # channel for a failure that has no server response behind it.
    reporter = HttpErrorReporter(
        url=resolved_url,
        profile=client.config.profile or "DEFAULT",
        auth_type=client.config.auth_type or "(auto-detect)",
    )

    exchange: ExchangeConfig | None = None
    if client_id:
        # Authoritative gate. ``main()`` rejects an explicit contradiction
        # earlier, before a browser can open, but only the constructed client
        # knows what an unspecified auth_type actually resolved to.
        if client.config.auth_type != "pat":
            raise SystemExit(_client_id_requires_pat(client.config.auth_type, client.config.profile))
        # ``_resolve_url``'s host guard covers a *relative* --url only; an
        # absolute one returns before it is ever consulted. The token endpoint
        # is derived from the workspace host either way.
        if not client.config.host:
            raise SystemExit(
                "uc-mcp-proxy: --client-id needs a workspace host to derive the token endpoint, "
                "and no host is configured in this profile."
            )
        exchange = ExchangeConfig(
            host=client.config.host,
            client_id=client_id,
            scopes=scopes,
            verify_ssl=verify_ssl,
        )

    auth = DatabricksAuth(
        client,
        exchange=exchange,
        on_fatal=reporter.report_fatal,
        shutting_down=lambda: reporter.shutting_down,
        transport=exchange_transport,
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
        #
        # ``diagnosed``, not ``reported``: the latter is keyed on
        # ``(role, status)`` and only the response hook can fill it, so a
        # proxy-side failure -- an exchange the workspace refused -- would leave
        # it empty and re-raise here as the traceback this module removed.
        if not reporter.diagnosed or not is_only_diagnosed_errors(exc):
            raise
        reporter.fatal_message = reporter.fatal_message or reporter.last_message

    if reporter.fatal_message is not None:
        # Exit by status, not by message: the hook already printed the
        # diagnosis, and ``SystemExit(<str>)`` would make CPython print it a
        # second time. ``fatal_message`` stays the internal record.
        raise SystemExit(_EXIT_HTTP_ERROR)


def _parse_scopes(values: list[str]) -> tuple[str, ...]:
    """Flatten and de-duplicate ``--scope`` values, preserving first-seen order.

    ``dict.fromkeys`` rather than a ``set``: the result is joined into the
    request body, and set iteration order varies per process, so a set would
    make the exchange request differ run to run -- surfacing under this repo's
    randomized test order as an intermittent failure with a nearly invisible
    cause.
    """
    return tuple(dict.fromkeys(scope for value in values for scope in value.split()))


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
        "--client-id",
        default=None,
        help=(
            "Enable RFC 8693 token exchange for a Databricks App: trade this profile's "
            "PAT for an app-scoped OAuth token. Pass the app's `oauth2_app_client_id` "
            "from `databricks apps get <app-name> -o json`. Requires a PAT profile. "
            "--scope is optional."
        ),
    )
    parser.add_argument(
        "--scope",
        action="append",
        default=[],
        metavar="SCOPE",
        help=(
            "OAuth scope requested by --client-id, from the app's "
            "`effective_user_api_scopes`. Repeatable, and a single value may list "
            "several space-separated scopes. Ignored without --client-id."
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

    if args.scope and not args.client_id:
        print("Error: --scope has no effect without --client-id.", file=sys.stderr)
        sys.exit(1)

    # Cheap gate on the flags as typed, so an explicit contradiction is rejected
    # before ``_preflight_authenticate`` can open a browser to satisfy it. It
    # only catches a stated auth_type -- most real PAT profiles name none, and
    # DATABRICKS_TOKEN names none either -- so ``run()`` still holds the
    # authoritative check once the client has resolved.
    if args.client_id and args.auth_type and args.auth_type != "pat":
        raise SystemExit(_client_id_requires_pat(args.auth_type, args.profile))

    if args.no_verify_ssl:
        print(
            "warning: SSL certificate verification is disabled (--no-verify-ssl). Use only in trusted environments.",
            file=sys.stderr,
        )
        if args.client_id:
            print(
                "warning: with --client-id the PAT is sent in a request BODY to the token "
                "endpoint, not only as a header. Disabling verification exposes it to anyone "
                "who can intercept that connection.",
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
            client_id=args.client_id,
            scopes=_parse_scopes(args.scope),
        )
    )


if __name__ == "__main__":  # pragma: no cover
    main()
