"""End-to-end HTTP-error tests that drive the real MCP SDK transport.

Every test here runs ``run()`` against a scripted ``httpx.AsyncBaseTransport``
handed in through the test-only ``transport`` seam, so the real
``streamable_http_client`` -- and the two failure paths it owns -- executes
unmodified. Nothing touches the network, which is why these are ``unit`` tests
by this repo's definition.

Patching ``stdio_server`` alone is not enough: with nothing written, no POST is
ever issued and the run hangs. Each test pushes real ``SessionMessage``\\ s into
the stdio read stream so the SDK actually talks to the transport. Every test is
wrapped in ``anyio.fail_after`` so a regression fails loudly instead of hanging.
"""

from __future__ import annotations

import functools
import itertools
import json
import os
import subprocess
import sys
import threading
from collections.abc import Callable
from contextlib import asynccontextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import anyio
import httpx
import pytest
from mcp.shared.message import SessionMessage
from mcp.types import JSONRPCMessage, JSONRPCNotification, JSONRPCRequest

from tests.conftest import FAKE_PAT
from uc_mcp_proxy import __main__ as main
from uc_mcp_proxy.errors import HttpErrorReporter, _leaves

pytestmark = [pytest.mark.unit, pytest.mark.anyio]

URL = "https://example.com/mcp"
REDIRECT_URL = "https://example.com/redirected"
#: A redirect target on a *different* origin, so httpx applies its cross-origin
#: header rules rather than treating the hop as same-site.
FOREIGN_URL = "https://elsewhere.example.net/mcp"
#: The token ``mock_workspace_client.config.authenticate()`` hands out.
BEARER_TOKEN = "test-oauth-token"
CLIENT_ID = "00000000-1111-2222-3333-444444444444"
SESSION_ID = "session-secret-0f1e2d"
PROTOCOL_VERSION = "2025-06-18"
TIMEOUT = 10


# ---------------------------------------------------------------------------
# Message builders
# ---------------------------------------------------------------------------


def _initialize(request_id: int = 1) -> SessionMessage:
    """The ``initialize`` request an MCP client sends first."""
    return SessionMessage(
        JSONRPCMessage(
            JSONRPCRequest(
                jsonrpc="2.0",
                id=request_id,
                method="initialize",
                params={
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "test-client", "version": "1.0"},
                },
            )
        )
    )


def _request(method: str, request_id: int) -> SessionMessage:
    """An arbitrary JSON-RPC request (a tool call, from the SDK's point of view)."""
    return SessionMessage(
        JSONRPCMessage(JSONRPCRequest(jsonrpc="2.0", id=request_id, method=method, params={})),
    )


def _initialized_notification() -> SessionMessage:
    """The notification whose POST triggers the SDK's ``start_get_stream``."""
    return SessionMessage(JSONRPCMessage(JSONRPCNotification(jsonrpc="2.0", method="notifications/initialized")))


# ---------------------------------------------------------------------------
# Canned server responses
# ---------------------------------------------------------------------------


def _initialize_ok(request_id: int = 1, session_id: str = SESSION_ID) -> httpx.Response:
    """A 200 ``initialize`` result carrying an ``mcp-session-id``.

    The session id is what makes the SDK issue a teardown DELETE on close, and
    what puts an ``mcp-session-id`` header on every later request.
    """
    return httpx.Response(
        200,
        headers={"mcp-session-id": session_id},
        json={
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "serverInfo": {"name": "test-server", "version": "1.0"},
            },
        },
    )


def _is_initialize(request: httpx.Request) -> bool:
    return b'"initialize"' in request.content


#: The same initialize request as a raw dict, for the subprocess test, which
#: writes JSON-RPC over a real pipe rather than pushing SessionMessage objects.
_INITIALIZE_WIRE = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": PROTOCOL_VERSION,
        "capabilities": {},
        "clientInfo": {"name": "test-client", "version": "1.0"},
    },
}


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _ScriptedTransport(httpx.AsyncBaseTransport):
    """Answers every request from ``responder`` and records what it was asked."""

    def __init__(self, responder: Any) -> None:
        self._responder = responder
        self.requests: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        await request.aread()
        self.requests.append(request)
        return self._responder(request)

    @property
    def methods(self) -> list[str]:
        return [request.method for request in self.requests]


class _RecordingReporter(HttpErrorReporter):
    """``HttpErrorReporter`` a test can await a diagnostic from.

    Needed because the two non-fatal paths (a stream 401, a suppressed
    teardown) never terminate the run, so there is no other edge to
    synchronize on -- and polling with sleeps is exactly what makes a test
    flaky. Only observes; the real ``on_response`` still does all the work.
    """

    def __init__(self, url: str, profile: str, auth_type: str) -> None:
        super().__init__(url=url, profile=profile, auth_type=auth_type)
        self.emitted: list[str] = []
        self._progress = anyio.Event()

    async def on_response(self, response: httpx.Response) -> None:
        seen = len(self.reported)
        await super().on_response(response)
        if len(self.reported) > seen:
            self.emitted.append(self.last_message or "")
            self._progress.set()
            self._progress = anyio.Event()

    async def wait_for_report(self, count: int = 1) -> None:
        while len(self.emitted) < count:
            await self._progress.wait()


class _Proxy:
    """Runs ``run()`` over mocked stdio and captures how it terminated."""

    def __init__(self, responder: Any) -> None:
        self.transport = _ScriptedTransport(responder)
        self.reporter: _RecordingReporter | None = None
        self.exit: SystemExit | None = None
        self.aborted: list[bool] = []
        self.error: BaseException | None = None
        self.finished = anyio.Event()
        self._reporter_ready = anyio.Event()
        self._to_proxy, self._proxy_read = anyio.create_memory_object_stream(16)
        self._proxy_write, self._from_proxy = anyio.create_memory_object_stream(16)

    # -- wiring -------------------------------------------------------------

    def install(self, monkeypatch: pytest.MonkeyPatch, workspace_client: Any) -> None:
        proxy = self

        @asynccontextmanager
        async def fake_stdio() -> Any:
            yield (proxy._proxy_read, proxy._proxy_write)

        def make_reporter(**kwargs: str) -> _RecordingReporter:
            proxy.reporter = _RecordingReporter(**kwargs)
            proxy._reporter_ready.set()
            return proxy.reporter

        monkeypatch.setattr(main, "stdio_server", fake_stdio)
        monkeypatch.setattr(main, "HttpErrorReporter", make_reporter)
        monkeypatch.setattr(main, "_preflight_authenticate", lambda *a, **k: workspace_client)
        # The real ``_abort`` calls ``os._exit`` -- it would take the test
        # runner down with it. Stubbing it lets the stack unwind so these
        # tests can assert on ``SystemExit``; that ``_abort`` fires at all in
        # production is covered by the subprocess test at the bottom of this
        # file, which is the only shape that can observe it.
        monkeypatch.setattr(main, "_abort", lambda: proxy.aborted.append(True))

    async def _run(self, url: str, **run_kwargs: Any) -> None:
        try:
            await main.run(url, transport=self.transport, **run_kwargs)
        except SystemExit as exc:
            self.exit = exc
        except Exception as exc:
            # Recorded rather than raised so the test can assert on it. Note
            # what is *not* caught: cancellation is a BaseException, so
            # ``fail_after`` still tears a hung run down.
            self.error = exc
        finally:
            self.finished.set()

    # -- the client side of stdio ------------------------------------------

    async def send(self, message: SessionMessage) -> None:
        await self._to_proxy.send(message)

    async def receive(self) -> Any:
        return await self._from_proxy.receive()

    async def close(self) -> None:
        await self._to_proxy.aclose()


class _ExchangeTransport(httpx.MockTransport):
    """A token-exchange endpoint that records every request it was asked.

    ``exchange_pat`` builds a *synchronous* ``httpx.Client``, so this is driven
    through ``handle_request`` rather than the async path the proxy's own
    transport uses. Counting matters: several invariants here are about how
    *many* exchanges a scenario costs, not just whether one happened.
    """

    def __init__(self, responder: Callable[[httpx.Request], httpx.Response]) -> None:
        self.requests: list[httpx.Request] = []

        def _recording(request: httpx.Request) -> httpx.Response:
            self.requests.append(request)
            return responder(request)

        super().__init__(_recording)

    @property
    def count(self) -> int:
        return len(self.requests)


def _minting_exchange() -> Callable[[httpx.Request], httpx.Response]:
    """An exchange endpoint that hands out a fresh, distinguishable token each time."""
    tokens = itertools.count(1)

    def responder(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"access_token": f"app-token-{next(tokens)}", "expires_in": 3600})

    return responder


@asynccontextmanager
async def _running(
    responder: Any,
    monkeypatch: pytest.MonkeyPatch,
    workspace_client: Any,
    *,
    url: str = URL,
    **run_kwargs: Any,
) -> Any:
    """Start ``run()`` in the background and yield the driver for it."""
    proxy = _Proxy(responder)
    proxy.install(monkeypatch, workspace_client)
    async with anyio.create_task_group() as tg:
        tg.start_soon(functools.partial(proxy._run, url, **run_kwargs))
        await proxy._reporter_ready.wait()
        try:
            yield proxy
        finally:
            await proxy.close()
            await proxy.finished.wait()


# ---------------------------------------------------------------------------
# 5. End-to-end clean exit on a request-role failure
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", [401, 500])
async def test_request_role_failure_exits_cleanly(status, monkeypatch, mock_workspace_client, capsys):
    """A failed tool call exits with the diagnosis and no traceback.

    The session is established first so the SDK's teardown DELETE is reached on
    this path -- the SDK-raise path -- which is the only coverage of
    ``reporter.shutting_down = True`` in ``run()``'s ``finally``.

    Note this is true here because ``_abort`` is stubbed. In production
    ``os._exit`` preempts the unwind and no DELETE is sent -- which is not a
    regression: the pre-fix code hung before completing it either.
    """

    def responder(request: httpx.Request) -> httpx.Response:
        if request.method == "DELETE":
            return httpx.Response(401, json={"error": "teardown refused"})
        if _is_initialize(request):
            return _initialize_ok()
        return httpx.Response(status, json={"error": "refused"})

    with anyio.fail_after(TIMEOUT):
        async with _running(responder, monkeypatch, mock_workspace_client) as proxy:
            await proxy.send(_initialize())
            await proxy.receive()  # session established
            await proxy.send(_request("tools/list", 2))
            await proxy.finished.wait()

    assert proxy.error is None, f"a non-SystemExit escaped run(): {proxy.error!r}"
    assert proxy.exit is not None, "run() did not exit on a request-role failure"

    # Exit by status, not by message: a str SystemExit code would make CPython
    # print the diagnosis a second time, on top of the hook's own print.
    assert proxy.exit.code == 1

    # The proxy also asked to terminate the process outright, because the real
    # stdio_server cannot be unwound while stdin is held open.
    assert proxy.aborted == [True]

    err = capsys.readouterr().err
    assert "Traceback" not in err
    assert "HTTPStatusError" not in err
    # The diagnosis appears exactly once, and carries the full context.
    headline = "rejected your credentials" if status == 401 else "the remote MCP server failed"
    assert err.count(headline) == 1
    assert str(status) in err
    assert URL in err
    assert "test-profile" in err
    assert "databricks-cli" in err
    assert err.rstrip().endswith("Exiting.")

    assert proxy.reporter is not None
    assert proxy.reporter.fatal_message is not None
    assert str(status) in proxy.reporter.fatal_message

    # The teardown DELETE fired and was suppressed: exactly one diagnostic.
    # This is the SDK-raise path, the one where an earlier design set
    # ``shutting_down`` too late for it to hold -- so assert the flag directly
    # rather than relying on the ``role == "teardown"`` guard to carry the test.
    assert "DELETE" in proxy.transport.methods
    assert proxy.reporter is not None
    assert proxy.reporter.shutting_down is True
    assert len(proxy.reporter.emitted) == 1
    assert "teardown refused" not in err


# ---------------------------------------------------------------------------
# 6. Live-session GET SSE failure
# ---------------------------------------------------------------------------


async def test_live_session_get_stream_401_warns_without_exiting(monkeypatch, mock_workspace_client, capsys):
    """A 401 on the background GET stream is reported but is not fatal."""

    def responder(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(401, json={"error": "stream refused"})
        if request.method == "DELETE":
            return httpx.Response(200)
        if _is_initialize(request):
            return _initialize_ok()
        return httpx.Response(202)  # notifications/initialized

    with anyio.fail_after(TIMEOUT):
        async with _running(responder, monkeypatch, mock_workspace_client) as proxy:
            await proxy.send(_initialize())
            await proxy.receive()
            # Triggers start_get_stream() inside the SDK's post_writer.
            await proxy.send(_initialized_notification())
            assert proxy.reporter is not None
            await proxy.reporter.wait_for_report()
            assert proxy.exit is None, "a stream failure must not terminate the proxy"

    assert proxy.exit is None
    # _abort() hard-kills the process: a stream failure must never reach it.
    assert proxy.aborted == []
    assert proxy.error is None
    assert "GET" in proxy.transport.methods

    (message,) = proxy.reporter.emitted
    assert "401" in message
    assert "The proxy will keep running" in message
    assert "Exiting." not in message

    err = capsys.readouterr().err
    assert "401" in err
    assert "Traceback" not in err


# ---------------------------------------------------------------------------
# 7. Teardown DELETE suppressed
# ---------------------------------------------------------------------------


async def test_teardown_delete_401_is_silent_and_exits_zero(monkeypatch, mock_workspace_client, capsys):
    """A successful session whose DELETE is refused exits zero, silently."""

    def responder(request: httpx.Request) -> httpx.Response:
        if request.method == "DELETE":
            return httpx.Response(401, json={"error": "teardown refused"})
        return _initialize_ok()

    with anyio.fail_after(TIMEOUT):
        async with _running(responder, monkeypatch, mock_workspace_client) as proxy:
            await proxy.send(_initialize())
            await proxy.receive()

    assert proxy.exit is None, "a refused teardown must not fail the run"
    # _abort() hard-kills the process: a refused teardown must never reach it.
    assert proxy.aborted == []
    assert proxy.error is None
    assert "DELETE" in proxy.transport.methods, "the DELETE never fired; suppression is untested"

    assert proxy.reporter is not None
    assert proxy.reporter.shutting_down is True
    assert proxy.reporter.emitted == []

    err = capsys.readouterr().err
    assert err == ""


# ---------------------------------------------------------------------------
# 13a / 13b. A redirect must not rewrite the session role
# ---------------------------------------------------------------------------


async def test_redirected_post_stays_request_role_and_exits(monkeypatch, mock_workspace_client, capsys):
    """POST -> 302 -> 401 stays fatal even though httpx rewrites POST to GET.

    Without role stamping the response hook only ever sees the flipped verb, so
    a rejected tool call would be reported as a recoverable stream failure.
    """

    def responder(request: httpx.Request) -> httpx.Response:
        if str(request.url) == URL and request.method == "POST":
            return httpx.Response(302, headers={"location": REDIRECT_URL})
        return httpx.Response(401, json={"error": "refused after redirect"})

    with anyio.fail_after(TIMEOUT):
        async with _running(responder, monkeypatch, mock_workspace_client) as proxy:
            await proxy.send(_initialize())
            await proxy.finished.wait()

    # The verb really did flip -- otherwise this test guards nothing.
    assert proxy.transport.methods == ["POST", "GET"]

    assert proxy.error is None
    assert proxy.exit is not None, "a redirected request-role failure must still exit"

    assert proxy.reporter is not None
    (message,) = proxy.reporter.emitted
    assert "401" in message
    assert message.endswith("Exiting.")
    assert "keep running" not in message

    assert "Traceback" not in capsys.readouterr().err


async def test_redirected_get_stream_stays_stream_role(monkeypatch, mock_workspace_client, capsys):
    """GET -> 302 -> 401 on the background stream stays non-fatal.

    Mirror of the POST case: it guards the double-failure mode where the role
    is lost entirely and the ``"request"`` default would misclassify a stream.
    """

    def responder(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            if str(request.url) == URL:
                return httpx.Response(302, headers={"location": REDIRECT_URL})
            return httpx.Response(401, json={"error": "stream refused after redirect"})
        if request.method == "DELETE":
            return httpx.Response(200)
        if _is_initialize(request):
            return _initialize_ok()
        return httpx.Response(202)

    with anyio.fail_after(TIMEOUT):
        async with _running(responder, monkeypatch, mock_workspace_client) as proxy:
            await proxy.send(_initialize())
            await proxy.receive()
            await proxy.send(_initialized_notification())
            assert proxy.reporter is not None
            await proxy.reporter.wait_for_report()
            assert proxy.exit is None, "a redirected stream failure must not terminate the proxy"

    assert proxy.exit is None
    # _abort() hard-kills the process: a clean shutdown must never reach it.
    assert proxy.aborted == []
    assert proxy.error is None
    assert proxy.transport.methods.count("GET") == 2, "the redirect hop did not happen"

    (message,) = proxy.reporter.emitted
    assert "401" in message
    assert "The proxy will keep running" in message
    assert "Exiting." not in message

    assert "Traceback" not in capsys.readouterr().err


# ---------------------------------------------------------------------------
# 14. stdout belongs to JSON-RPC framing
# ---------------------------------------------------------------------------


async def test_failing_run_writes_nothing_to_stdout(monkeypatch, mock_workspace_client, capsys):
    """Not one byte reaches stdout across a failing run; diagnostics go to stderr."""

    def responder(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "boom"})

    with anyio.fail_after(TIMEOUT):
        async with _running(responder, monkeypatch, mock_workspace_client) as proxy:
            await proxy.send(_initialize())
            await proxy.finished.wait()

    assert proxy.exit is not None

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "500" in captured.err


# ---------------------------------------------------------------------------
# 15. Credentials never reach the diagnostic
# ---------------------------------------------------------------------------


async def test_echoed_credentials_are_redacted_from_output(monkeypatch, mock_workspace_client, capsys):
    """A server that echoes the bearer token and session id leaks neither."""

    def responder(request: httpx.Request) -> httpx.Response:
        if request.method == "DELETE":
            return httpx.Response(200)
        if _is_initialize(request):
            return _initialize_ok()
        # The session is live by now, so both secrets are on this request.
        assert request.headers["mcp-session-id"] == SESSION_ID
        return httpx.Response(
            403,
            json={
                "error": "rejected",
                "authorization": request.headers["authorization"],
                "forwarded_token": request.headers["x-forwarded-access-token"],
                "session": request.headers["mcp-session-id"],
            },
        )

    with anyio.fail_after(TIMEOUT):
        async with _running(responder, monkeypatch, mock_workspace_client) as proxy:
            await proxy.send(_initialize())
            await proxy.receive()
            await proxy.send(_request("tools/call", 2))
            await proxy.finished.wait()

    assert proxy.exit is not None

    assert proxy.reporter is not None
    captured = capsys.readouterr()
    combined = captured.out + captured.err + str(proxy.reporter.fatal_message)
    assert BEARER_TOKEN not in combined
    assert SESSION_ID not in combined
    # Non-vacuous: the echoed body did reach the diagnostic, redacted.
    assert "<redacted>" in captured.err
    assert "403" in captured.err


# ---------------------------------------------------------------------------
# 16. The ordering the whole suppression design rests on
# ---------------------------------------------------------------------------


async def test_response_hook_precedes_auth_flow():
    """httpx runs response hooks BEFORE handing the response to the auth flow.

    Everything about the retry depends on this. Without suppression the reporter
    reaches the first 401 first and aborts the process before the retry can ever
    be dispatched, so the retry would be present, correct, and dead.

    It is an httpx *internal*, not a documented contract. ``pyproject.toml``
    caps httpx below the next minor precisely because of it; this test is the
    gate for raising that cap. A reordering then shows up as a loud CI failure
    rather than a feature that silently stops retrying.
    """
    order: list[str] = []

    class _Probe(httpx.Auth):
        async def async_auth_flow(self, request):
            response = yield request
            order.append(f"auth:{response.status_code}")
            if response.status_code == 401:
                yield request

    async def hook(response: httpx.Response) -> None:
        order.append(f"hook:{response.status_code}")

    attempts = itertools.count(1)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401 if next(attempts) == 1 else 200, json={})

    async with httpx.AsyncClient(
        auth=_Probe(),
        transport=httpx.MockTransport(handler),
        event_hooks={"response": [hook]},
    ) as client:
        response = await client.post(URL, json={})

    assert response.status_code == 200, "the two-yield retry did not run at all"
    assert order[:2] == ["hook:401", "auth:401"], f"hook/auth ordering changed: {order}"


# ---------------------------------------------------------------------------
# 17. The exchange recovers a mid-session 401
# ---------------------------------------------------------------------------


async def test_exchange_401_recovers_mid_session(monkeypatch, mock_workspace_client_pat, capsys):
    """A tool call refused mid-session is retried with a fresh token and survives.

    The scenario the whole feature exists for: the app token expires about an
    hour in, long after any manual test has finished. Also pins the exchange
    *count* -- the cache must make this cost one re-mint, not one per request.
    """
    exchange = _ExchangeTransport(_minting_exchange())
    tool_calls = itertools.count(1)
    tokens_seen: list[str] = []

    def responder(request: httpx.Request) -> httpx.Response:
        if request.method == "DELETE":
            return httpx.Response(200)
        if _is_initialize(request):
            return _initialize_ok()
        tokens_seen.append(request.headers.get("authorization", ""))
        if next(tool_calls) == 1:
            return httpx.Response(401, json={"error": "token expired"})
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": 2, "result": {}})

    with anyio.fail_after(TIMEOUT):
        async with _running(
            responder,
            monkeypatch,
            mock_workspace_client_pat,
            client_id=CLIENT_ID,
            scopes=("sql",),
            exchange_transport=exchange,
        ) as proxy:
            await proxy.send(_initialize())
            await proxy.receive()
            await proxy.send(_request("tools/list", 2))
            await proxy.receive()  # the retry succeeded, so a result came back

    assert proxy.error is None
    assert proxy.exit is None, "a recovered 401 must not terminate the proxy"
    assert proxy.aborted == [], "_abort hard-kills the process; a recovered 401 must never reach it"

    # One lazy exchange at the first request, one re-mint after the 401.
    assert exchange.count == 2, f"expected exactly one re-exchange, saw {exchange.count} exchanges"
    # The retry really did carry a different credential -- otherwise this test
    # would pass even if the cache were handing back the rejected token.
    assert tokens_seen[0] != tokens_seen[1]
    assert FAKE_PAT not in "".join(tokens_seen)

    err = capsys.readouterr().err
    assert err.count("refreshing and retrying once") == 1
    assert "Exiting." not in err
    assert "Traceback" not in err
    assert proxy.reporter is not None
    assert proxy.reporter.reported == set(), "a suppressed 401 must not consume a dedup slot"


# ---------------------------------------------------------------------------
# 18. A second refusal is permanent
# ---------------------------------------------------------------------------


async def test_second_401_exits_with_the_exchange_remediation(monkeypatch, mock_workspace_client_pat, capsys):
    """A freshly minted token refused again is fatal, and says what to check.

    The retry is once, never a loop: an app that refuses a token minted seconds
    ago is telling us about configuration, not expiry.
    """
    exchange = _ExchangeTransport(_minting_exchange())

    def responder(request: httpx.Request) -> httpx.Response:
        if request.method == "DELETE":
            return httpx.Response(200)
        return httpx.Response(401, json={"error": "not authorized for this app"})

    with anyio.fail_after(TIMEOUT):
        async with _running(
            responder,
            monkeypatch,
            mock_workspace_client_pat,
            client_id=CLIENT_ID,
            exchange_transport=exchange,
        ) as proxy:
            await proxy.send(_initialize())
            await proxy.finished.wait()

    assert proxy.error is None
    assert proxy.exit is not None, "a twice-refused credential must terminate the proxy"
    assert proxy.exit.code == 1

    err = capsys.readouterr().err
    assert "Traceback" not in err
    assert "HTTPStatusError" not in err
    # The exchange-specific remediation replaced the generic "may have expired"
    # text, which would be a false statement here: it did not expire.
    assert "--client-id" in err
    assert "--scope" in err
    assert "may have expired" not in err
    assert err.rstrip().endswith("Exiting.")
    # Suppressed once, then reported once.
    assert err.count("refreshing and retrying once") == 1
    assert err.count("rejected your credentials") == 1


# ---------------------------------------------------------------------------
# 19. Redirect x retry -- the silent-hang guard
# ---------------------------------------------------------------------------


async def test_redirected_exchange_401_is_suppressed_then_diagnosed(monkeypatch, mock_workspace_client_pat, capsys):
    """POST -> 302 -> 401, retried, -> 302 -> 401 still ends in one diagnosis.

    httpx copies ``extensions`` per redirect hop rather than sharing them, so
    disarming ``response.request`` -- that hop's discarded snapshot -- instead of
    the original would leave the rebuilt retry still reading as armed. Both 401s
    would then be suppressed and the proxy would hang with no output at all,
    which is strictly worse than the traceback this module replaced.
    """
    exchange = _ExchangeTransport(_minting_exchange())

    def responder(request: httpx.Request) -> httpx.Response:
        if request.method == "DELETE":
            return httpx.Response(200)
        if str(request.url) == URL and request.method == "POST":
            return httpx.Response(302, headers={"location": REDIRECT_URL})
        return httpx.Response(401, json={"error": "refused after redirect"})

    with anyio.fail_after(TIMEOUT):
        async with _running(
            responder,
            monkeypatch,
            mock_workspace_client_pat,
            client_id=CLIENT_ID,
            exchange_transport=exchange,
        ) as proxy:
            await proxy.send(_initialize())
            await proxy.finished.wait()

    # The full hop sequence, not just the first two. The redirect really did
    # happen (POST rewritten to GET), and the retry re-walked the chain from the
    # original request. Re-yielding ``response.request`` wholesale instead would
    # give ``["POST", "GET", "GET"]``: the retried tool call re-sent as a
    # bodyless GET, and -- because httpx strips ``Authorization`` on a
    # cross-origin hop -- ``_stale_token`` reading back "", which sends ``_token``
    # into its stale-mismatch branch and hands straight back the token that was
    # just refused. The retry would then never re-mint anything.
    assert proxy.transport.methods == ["POST", "GET", "POST", "GET"]
    assert proxy.exit is not None, "the second 401 must still be diagnosed through a redirect"
    assert proxy.exit.code == 1

    err = capsys.readouterr().err
    assert err.count("refreshing and retrying once") == 1
    assert err.count("rejected your credentials") == 1
    assert "Traceback" not in err


# ---------------------------------------------------------------------------
# 20. An exchange the workspace refuses
# ---------------------------------------------------------------------------


async def test_exchange_failure_exits_one_without_traceback(monkeypatch, mock_workspace_client_pat, capsys):
    """A refused exchange is diagnosed, not raised -- and it does exit non-zero.

    A wrong ``--client-id``, a wrong ``--scope``, and a revoked PAT are this
    feature's three commonest failures and all three land here. The failure
    produces no response from the app, so nothing lands in the reporter's
    ``(role, status)`` set -- which is exactly why the backstop reads
    ``diagnosed`` instead. Getting that wrong yields either a traceback, or a
    fatal message followed by exit 0.
    """
    exchange = _ExchangeTransport(lambda request: httpx.Response(400, json={"error": "invalid audience"}))

    def responder(request: httpx.Request) -> httpx.Response:
        return _initialize_ok()

    with anyio.fail_after(TIMEOUT):
        async with _running(
            responder,
            monkeypatch,
            mock_workspace_client_pat,
            client_id=CLIENT_ID,
            exchange_transport=exchange,
        ) as proxy:
            await proxy.send(_initialize())
            await proxy.finished.wait()

    assert proxy.error is None, f"a non-SystemExit escaped run(): {proxy.error!r}"
    assert proxy.exit is not None, "a refused exchange must terminate the proxy"
    assert proxy.exit.code == 1

    captured = capsys.readouterr()
    assert captured.out == "", "stdout belongs to JSON-RPC framing"
    err = captured.err
    assert "Traceback" not in err
    assert "HTTPStatusError" not in err
    assert "refused the PAT token exchange" in err
    assert "--client-id" in err
    assert "invalid audience" in err
    assert err.rstrip().endswith("Exiting.")
    # The credential is in the body of that request as well as its header.
    assert FAKE_PAT not in err
    # Printed once, however many in-flight requests re-ran the failing exchange.
    assert err.count("refused the PAT token exchange") == 1


# ---------------------------------------------------------------------------
# 21. A refused exchange on the background stream stays non-fatal
# ---------------------------------------------------------------------------


async def test_exchange_get_stream_401_warns_without_exiting(monkeypatch, mock_workspace_client_pat):
    """A 401 the GET stream cannot recover from warns; it does not terminate."""
    exchange = _ExchangeTransport(_minting_exchange())

    def responder(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(401, json={"error": "stream refused"})
        if request.method == "DELETE":
            return httpx.Response(200)
        if _is_initialize(request):
            return _initialize_ok()
        return httpx.Response(202)

    with anyio.fail_after(TIMEOUT):
        async with _running(
            responder,
            monkeypatch,
            mock_workspace_client_pat,
            client_id=CLIENT_ID,
            exchange_transport=exchange,
        ) as proxy:
            await proxy.send(_initialize())
            await proxy.receive()
            await proxy.send(_initialized_notification())
            assert proxy.reporter is not None
            await proxy.reporter.wait_for_report()
            assert proxy.exit is None, "a stream failure must not terminate the proxy"

    assert proxy.exit is None
    assert proxy.aborted == []
    assert proxy.error is None

    (message,) = proxy.reporter.emitted
    assert "401" in message
    assert "The proxy will keep running" in message
    assert "Exiting." not in message

    # Bounded by the SDK's own MAX_RECONNECTION_ATTEMPTS = 2, so the reconnect
    # loop cannot amplify the exchange without bound. Asserted rather than
    # enforced by proxy-side state: a test fails loudly if the SDK raises that
    # constant, where a guard flag would silently absorb the change.
    assert exchange.count <= 3, f"reconnects amplified the exchange to {exchange.count}"


# ---------------------------------------------------------------------------
# 22. A refused exchange during teardown is not the user's problem
# ---------------------------------------------------------------------------


async def test_teardown_exchange_failure_is_silent_and_exits_zero(monkeypatch, mock_workspace_client_pat, capsys):
    """A session that outlived its token exits zero even if the last refresh fails.

    The SDK's teardown DELETE goes out through the same auth flow, so a
    multi-hour session reaches an expired cache on the way out. Network gone,
    laptop sleeping, VPN dropped -- all ordinary at shutdown. Reporting it would
    make a clean session exit 1 over a token it never needed, which is what this
    module already says about a refused teardown: the user did not ask for it,
    and a server that refuses it has not harmed a session that already ended.
    """
    attempts = itertools.count(1)

    def exchange_responder(request: httpx.Request) -> httpx.Response:
        if next(attempts) == 1:
            # A positive lifetime below the 60s renewal margin, so the cache is
            # stale the moment it is written and teardown is forced to re-mint.
            # Not ``0``: that is clamped to the default, because a zero or
            # negative lifetime would otherwise cost one blocking exchange per
            # request rather than one per hour.
            return httpx.Response(200, json={"access_token": "app-token-1", "expires_in": 1})
        return httpx.Response(400, json={"error": "workspace unreachable"})

    exchange = _ExchangeTransport(exchange_responder)

    def responder(request: httpx.Request) -> httpx.Response:
        if request.method == "DELETE":
            return httpx.Response(200)
        return _initialize_ok()

    with anyio.fail_after(TIMEOUT):
        async with _running(
            responder,
            monkeypatch,
            mock_workspace_client_pat,
            client_id=CLIENT_ID,
            exchange_transport=exchange,
        ) as proxy:
            await proxy.send(_initialize())
            await proxy.receive()

    assert proxy.exit is None, "a teardown-time exchange failure must not fail the run"
    assert proxy.aborted == []
    assert proxy.error is None
    assert proxy.reporter is not None
    assert proxy.reporter.shutting_down is True
    assert proxy.reporter.fatal_message is None
    assert exchange.count >= 2, "teardown never re-minted; the silence is untested"

    assert capsys.readouterr().err == ""


# ---------------------------------------------------------------------------
# 23. The forwarded-token rule, both directions
# ---------------------------------------------------------------------------


async def test_exchange_path_sends_no_forwarded_token(monkeypatch, mock_workspace_client_pat):
    """On the exchange path the app gets no client-supplied forwarded token.

    Asserted against requests the real SDK produced, and on every header value
    rather than one named key, so renaming the header cannot defeat it.
    """
    exchange = _ExchangeTransport(_minting_exchange())

    def responder(request: httpx.Request) -> httpx.Response:
        if request.method == "DELETE":
            return httpx.Response(200)
        return _initialize_ok()

    with anyio.fail_after(TIMEOUT):
        async with _running(
            responder,
            monkeypatch,
            mock_workspace_client_pat,
            client_id=CLIENT_ID,
            exchange_transport=exchange,
        ) as proxy:
            await proxy.send(_initialize())
            await proxy.receive()

    assert proxy.transport.requests, "no request was captured; the assertion would be vacuous"
    for request in proxy.transport.requests:
        assert "x-forwarded-access-token" not in request.headers
        assert FAKE_PAT not in "".join(request.headers.values())


async def test_non_pat_profile_still_forwards_the_token(monkeypatch, mock_workspace_client):
    """For non-``pat`` auth types the forwarded header is byte-identical to before.

    The counterpart that keeps the rule above from being a blanket removal: only
    PAT profiles lose this header, and that difference is deliberate.
    """

    def responder(request: httpx.Request) -> httpx.Response:
        if request.method == "DELETE":
            return httpx.Response(200)
        return _initialize_ok()

    with anyio.fail_after(TIMEOUT):
        async with _running(responder, monkeypatch, mock_workspace_client) as proxy:
            await proxy.send(_initialize())
            await proxy.receive()

    first = proxy.transport.requests[0]
    assert first.headers["authorization"] == f"Bearer {BEARER_TOKEN}"
    assert first.headers["x-forwarded-access-token"] == BEARER_TOKEN


async def test_pat_never_survives_a_cross_origin_redirect(monkeypatch, mock_workspace_client_pat):
    """A redirect to a foreign origin carries no PAT, in any header.

    httpx strips ``Authorization`` on a cross-origin hop but strips no header of
    ours, so mirroring the credential into a second header would hand the PAT to
    the new origin with the real credential already removed. Omitting it for
    every ``pat`` profile -- not only on the exchange path -- is what closes that.
    """
    seen: list[httpx.Request] = []

    def responder(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if str(request.url) == URL:
            return httpx.Response(302, headers={"location": FOREIGN_URL})
        return httpx.Response(401, json={"error": "refused"})

    with anyio.fail_after(TIMEOUT):
        async with _running(responder, monkeypatch, mock_workspace_client_pat) as proxy:
            await proxy.send(_initialize())
            await proxy.finished.wait()

    foreign = [request for request in seen if request.url.host == "elsewhere.example.net"]
    assert foreign, "the cross-origin hop never happened; this test guards nothing"
    for request in foreign:
        assert FAKE_PAT not in "".join(request.headers.values())
        assert "x-forwarded-access-token" not in request.headers


async def test_oauth_token_never_survives_a_cross_origin_redirect(monkeypatch, mock_workspace_client):
    """A redirect off the target origin strips the forwarded token for EVERY auth type.

    The `pat` carve-out above is not enough on its own. httpx pops
    ``Authorization`` on a cross-origin hop but knows nothing about
    ``X-Forwarded-Access-Token``, so a single 302 from the app would hand a live
    workspace OAuth token to an origin the user never named -- and hand it over
    with the real credential already stripped, so the foreign host learns a
    token it was never sent directly. `databricks-cli` is the auth type the
    README *recommends* for Apps, so this is the common configuration.
    """
    seen: list[httpx.Request] = []

    def responder(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if str(request.url) == URL:
            return httpx.Response(302, headers={"location": FOREIGN_URL})
        return httpx.Response(401, json={"error": "refused"})

    with anyio.fail_after(TIMEOUT):
        async with _running(responder, monkeypatch, mock_workspace_client) as proxy:
            await proxy.send(_initialize())
            await proxy.finished.wait()

    same_origin = [r for r in seen if r.url.host == "example.com"]
    foreign = [r for r in seen if r.url.host == "elsewhere.example.net"]

    # Non-vacuous on both sides: the header really is sent to the intended
    # origin, and really is gone by the time the hop lands elsewhere.
    assert same_origin, "the first hop never happened"
    assert same_origin[0].headers["x-forwarded-access-token"] == BEARER_TOKEN
    assert foreign, "the cross-origin hop never happened; this test guards nothing"
    for request in foreign:
        assert "x-forwarded-access-token" not in request.headers
        assert BEARER_TOKEN not in "".join(request.headers.values())


# ---------------------------------------------------------------------------
# The proxy must actually terminate -- observable only from outside the process
# ---------------------------------------------------------------------------


def _serve_401() -> tuple[HTTPServer, int]:
    """A localhost server that answers every POST with 401."""

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            body = b'{"error":"invalid token"}'
            self.send_response(401)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args: object) -> None:
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, server.server_address[1]


def test_proxy_process_exits_on_401_with_stdin_still_open():
    """The proxy must exit, not merely print that it is exiting.

    Every other test in this file stubs ``stdio_server``, so none of them can
    see this: the real one yields from inside a task group whose ``stdin_reader``
    blocks in ``readline`` on a worker thread. anyio cannot cancel a blocking
    thread read, so returning from ``run()`` leaves ``__aexit__`` waiting
    forever. A live MCP client holds stdin open for the whole session, which is
    exactly the condition reproduced here -- stdin is deliberately NOT closed.

    Spawns a subprocess against a localhost socket rather than the network, so
    it stays a unit test by this repo's definition, and finishes in ~1s.
    """
    server, port = _serve_401()
    try:
        env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src"),
            "DATABRICKS_HOST": f"http://127.0.0.1:{port}",
            "DATABRICKS_TOKEN": "dapi-fake-token",
            "DATABRICKS_CONFIG_FILE": os.devnull,
        }
        # The env is built from scratch rather than inherited so a developer's
        # real DATABRICKS_* settings cannot reach the child. On Windows that
        # also drops SystemRoot, without which winsock fails to initialize and
        # the child dies importing asyncio's proactor loop (WinError 10106)
        # before it ever reaches the code under test.
        for name in ("SystemRoot", "SystemDrive"):
            value = os.environ.get(name)
            if value:
                env[name] = value
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uc_mcp_proxy",
                "--url",
                f"http://127.0.0.1:{port}/mcp",
                "--no-auto-login",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        try:
            assert proc.stdin is not None
            proc.stdin.write(json.dumps(_INITIALIZE_WIRE) + "\n")
            proc.stdin.flush()
            # stdin stays open on purpose -- closing it would mask the bug.
            # ``communicate()`` closes it, so wait instead. Output is a few
            # hundred bytes, far under the pipe buffer, so this cannot deadlock.
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            raise AssertionError(
                "the proxy hung instead of exiting: it printed its diagnosis and then waited on stdin forever"
            ) from None
        finally:
            assert proc.stdout is not None
            assert proc.stderr is not None
            stdout, stderr = proc.stdout.read(), proc.stderr.read()
            proc.stdin.close()
            proc.stdout.close()
            proc.stderr.close()
    finally:
        server.shutdown()

    assert proc.returncode != 0, "a refused credential must be a non-zero exit"
    assert stdout == "", f"stdout must stay clean for JSON-RPC framing, got {stdout!r}"
    assert "rejected your credentials" in stderr
    assert "Traceback" not in stderr
    # Printed once: by the hook, not again by CPython's SystemExit handler.
    assert stderr.count("rejected your credentials") == 1


# ---------------------------------------------------------------------------
# The backstop must re-raise anything it did not diagnose
# ---------------------------------------------------------------------------


async def test_unrelated_exception_is_not_swallowed_by_the_backstop(monkeypatch, mock_workspace_client, capsys):
    """``except BaseException`` is the riskiest construct here -- pin the re-raise.

    ``is_only_diagnosed_errors`` is tested exhaustively as a predicate, but
    nothing otherwise exercises the wiring that keeps a genuine bug (or a
    Ctrl-C) from being swallowed and misreported as a credential rejection.
    """
    boom = RuntimeError("transport exploded")

    def responder(request: httpx.Request) -> httpx.Response:
        raise boom

    with anyio.fail_after(TIMEOUT):
        async with _running(responder, monkeypatch, mock_workspace_client) as proxy:
            await proxy.send(_initialize())
            await proxy.finished.wait()

    assert proxy.exit is None, "an unrelated failure must not be reported as an exit"
    assert proxy.error is not None, "the backstop swallowed an exception it did not diagnose"
    assert boom in _leaves(proxy.error)
    assert proxy.aborted == []

    err = capsys.readouterr().err
    assert "rejected your credentials" not in err
