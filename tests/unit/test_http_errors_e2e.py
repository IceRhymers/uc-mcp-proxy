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

import json
import os
import subprocess
import sys
import threading
from contextlib import asynccontextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import anyio
import httpx
import pytest
from mcp.shared.message import SessionMessage
from mcp.types import JSONRPCMessage, JSONRPCNotification, JSONRPCRequest

from uc_mcp_proxy import __main__ as main
from uc_mcp_proxy.errors import HttpErrorReporter, _leaves

pytestmark = [pytest.mark.unit, pytest.mark.anyio]

URL = "https://example.com/mcp"
REDIRECT_URL = "https://example.com/redirected"
#: The token ``mock_workspace_client.config.authenticate()`` hands out.
BEARER_TOKEN = "test-oauth-token"
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

    async def _run(self, url: str) -> None:
        try:
            await main.run(url, transport=self.transport)
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


@asynccontextmanager
async def _running(
    responder: Any,
    monkeypatch: pytest.MonkeyPatch,
    workspace_client: Any,
    *,
    url: str = URL,
) -> Any:
    """Start ``run()`` in the background and yield the driver for it."""
    proxy = _Proxy(responder)
    proxy.install(monkeypatch, workspace_client)
    async with anyio.create_task_group() as tg:
        tg.start_soon(proxy._run, url)
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
    assert "pat" in err
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

    ``is_only_http_status_errors`` is tested exhaustively as a predicate, but
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
