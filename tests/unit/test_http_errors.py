"""Tests for the HTTP error diagnosis hooks in ``uc_mcp_proxy.errors``.

These are hook-level tests: responses are built by hand and handed straight to
``HttpErrorReporter.on_response``, so each test pins one behaviour of the hook
without standing up the MCP SDK. The end-to-end counterparts live in
``test_http_errors_e2e.py``.
"""

from __future__ import annotations

import asyncio
import builtins
import time
from collections.abc import AsyncIterator

import anyio
import httpx
import pytest

pytestmark = pytest.mark.unit

# ``BaseExceptionGroup`` is a builtin only on 3.11+. The package supports 3.10,
# where anyio pulls in the ``exceptiongroup`` backport as a hard requirement.
# Fetched via ``builtins`` rather than named directly so that linting against
# the package's py310 target does not read it as an undefined name.
_BaseExceptionGroup = getattr(builtins, "BaseExceptionGroup", None)
if _BaseExceptionGroup is None:  # pragma: no cover - version-dependent
    from exceptiongroup import BaseExceptionGroup as _BaseExceptionGroup

URL = "https://example.com/mcp"
PROFILE = "test-profile"
AUTH_TYPE = "oauth-u2m"


def make_reporter():
    """A reporter configured with the identifiers every message must name."""
    from uc_mcp_proxy.errors import HttpErrorReporter

    return HttpErrorReporter(url=URL, profile=PROFILE, auth_type=AUTH_TYPE)


def make_response(status, *, role="request", text="", headers=None, stream=None):
    """A response whose request carries ``role`` in its extensions."""
    from uc_mcp_proxy.errors import _ROLE_KEY

    request = httpx.Request("POST", URL, headers=headers, extensions={_ROLE_KEY: role})
    if stream is not None:
        return httpx.Response(status, request=request, stream=stream)
    return httpx.Response(status, request=request, text=text)


# ---------------------------------------------------------------------------
# 1-2: request-role failures are diagnosed and fatal
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_request_role_401_reports_identifiers_and_is_fatal(capsys):
    """A request-role 401 names status, url, profile and auth_type on stderr."""
    from uc_mcp_proxy.errors import ROLE_REQUEST

    reporter = make_reporter()
    await reporter.on_response(make_response(401, role=ROLE_REQUEST))

    err = capsys.readouterr().err
    assert "401" in err
    assert URL in err
    assert PROFILE in err
    assert AUTH_TYPE in err
    assert reporter.fatal.is_set()
    assert reporter.fatal_message is not None


@pytest.mark.anyio
async def test_request_role_401_emits_no_traceback(capsys):
    """The diagnosis replaces the traceback rather than accompanying it."""
    reporter = make_reporter()
    await reporter.on_response(make_response(401))

    assert "Traceback" not in capsys.readouterr().err


@pytest.mark.anyio
async def test_request_role_401_disposition_says_exiting(capsys):
    """The closing line tells the user the proxy is shutting down."""
    reporter = make_reporter()
    await reporter.on_response(make_response(401))

    assert "exiting" in capsys.readouterr().err.lower()


@pytest.mark.anyio
async def test_request_role_500_reports_server_side_error(capsys):
    """A 500 is described as a server-side failure."""
    reporter = make_reporter()
    await reporter.on_response(make_response(500))

    assert "server-side error" in capsys.readouterr().err.lower()


@pytest.mark.anyio
async def test_request_role_500_does_not_claim_credentials_were_rejected(capsys):
    """A 500 must not be misdiagnosed as an authentication failure."""
    reporter = make_reporter()
    await reporter.on_response(make_response(500))

    err = capsys.readouterr().err.lower()
    assert "credential" not in err
    assert "rejected your credentials" not in err
    assert "not an authentication problem" in err


@pytest.mark.anyio
async def test_request_role_500_is_fatal():
    """A 500 on the request channel still stops the proxy."""
    reporter = make_reporter()
    await reporter.on_response(make_response(500))

    assert reporter.fatal.is_set()
    assert reporter.fatal_message is not None


# ---------------------------------------------------------------------------
# 3: stream-role failures are reported but survivable, and deduped
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_stream_role_401_reports_without_being_fatal(capsys):
    """A background-stream 401 is diagnosed but does not stop the proxy."""
    from uc_mcp_proxy.errors import ROLE_STREAM

    reporter = make_reporter()
    await reporter.on_response(make_response(401, role=ROLE_STREAM))

    assert "401" in capsys.readouterr().err
    assert not reporter.fatal.is_set()
    assert reporter.fatal_message is None


@pytest.mark.anyio
async def test_stream_role_401_disposition_states_survival_and_bounded_retries(capsys):
    """The stream disposition promises continuation *and* an end to retries."""
    from uc_mcp_proxy.errors import ROLE_STREAM

    reporter = make_reporter()
    await reporter.on_response(make_response(401, role=ROLE_STREAM))

    err = capsys.readouterr().err.lower()
    assert "keep running" in err
    assert "bounded number of times" in err
    assert "will then stop" in err


@pytest.mark.anyio
async def test_repeated_stream_role_401_is_deduped(capsys):
    """An identical second failure emits nothing and adds no report key."""
    from uc_mcp_proxy.errors import ROLE_STREAM

    reporter = make_reporter()
    await reporter.on_response(make_response(401, role=ROLE_STREAM))
    capsys.readouterr()

    await reporter.on_response(make_response(401, role=ROLE_STREAM))

    assert capsys.readouterr().err == ""
    assert reporter.reported == {(ROLE_STREAM, 401)}


# ---------------------------------------------------------------------------
# 4: a successful streaming response is never touched
# ---------------------------------------------------------------------------

SSE_CHUNKS = [b"event: message\n", b'data: {"jsonrpc":"2.0"}\n', b"\n"]


class LazySSEStream(httpx.AsyncByteStream):
    """A stream that yields only when iterated, so consumption is observable."""

    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            yield chunk


class StreamingTransport(httpx.AsyncBaseTransport):
    """Returns a 200 whose body is lazy -- httpx reads ``content=`` eagerly."""

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=LazySSEStream(list(SSE_CHUNKS)),
        )


@pytest.mark.anyio
async def test_streaming_200_is_not_consumed_or_reported_by_the_hook(capsys):
    """The hook must not read a 2xx body: that would eat the SSE stream."""
    from uc_mcp_proxy.errors import stamp_role

    reporter = make_reporter()
    seen = {}

    async def record(response: httpx.Response) -> None:
        seen["consumed"] = response.is_stream_consumed

    transport = StreamingTransport()
    async with httpx.AsyncClient(
        transport=transport,
        event_hooks={"request": [stamp_role], "response": [reporter.on_response, record]},
    ) as client:
        async with client.stream("GET", URL) as response:
            received = b"".join([chunk async for chunk in response.aiter_raw()])

    assert capsys.readouterr().err == ""
    assert seen["consumed"] is False
    assert received == b"".join(SSE_CHUNKS)


# ---------------------------------------------------------------------------
# 8-9: remediation text describes a remote rejection, not a local credential gap
# ---------------------------------------------------------------------------

#: ``auth.py``'s local-credential wording. Correct there, a false statement here:
#: preflight already minted a token before any remote 4xx could arrive.
LOCAL_CREDENTIAL_WORDING = ("missing or expired", "generate a new token", "databricks_token")


@pytest.mark.parametrize("status", [401, 403])
@pytest.mark.anyio
async def test_remote_rejection_avoids_local_credential_wording(capsys, status):
    """Neither a 401 nor a 403 may be blamed on the user's local credentials."""
    reporter = make_reporter()
    await reporter.on_response(make_response(status))

    err = capsys.readouterr().err.lower()
    for phrase in LOCAL_CREDENTIAL_WORDING:
        assert phrase not in err


@pytest.mark.anyio
async def test_403_remediation_describes_an_authorization_failure(capsys):
    """A 403 says the credential authenticated but lacks authorization."""
    reporter = make_reporter()
    await reporter.on_response(make_response(403))

    err = capsys.readouterr().err.lower()
    assert "authenticated successfully" in err
    assert "not authorized" in err
    assert "databricks app" in err
    assert "oauth u2m" in err


@pytest.mark.anyio
async def test_401_remediation_says_the_token_was_minted_then_rejected(capsys):
    """A 401 attributes the failure to the server, not to a missing token."""
    reporter = make_reporter()
    await reporter.on_response(make_response(401))

    err = capsys.readouterr().err.lower()
    assert "minted successfully" in err
    assert "server rejected it" in err


# ---------------------------------------------------------------------------
# 10: a 404 means different things with and without a live session
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_404_without_session_id_blames_the_url_and_is_fatal(capsys):
    """No session yet means the initialize POST hit the wrong endpoint."""
    reporter = make_reporter()
    await reporter.on_response(make_response(404))

    err = capsys.readouterr().err
    assert "--url" in err
    assert "not an authentication failure" in err.lower()
    assert reporter.fatal.is_set()


@pytest.mark.anyio
async def test_404_with_session_id_reports_session_expiry_and_is_fatal(capsys):
    """An in-session 404 is a server-side session expiry, and still fatal."""
    reporter = make_reporter()
    await reporter.on_response(make_response(404, headers={"mcp-session-id": "abc123"}))

    err = capsys.readouterr().err.lower()
    assert "session expired" in err
    assert "--url" not in err
    assert reporter.fatal.is_set()


@pytest.mark.anyio
async def test_404_messages_differ_by_session_state(capsys):
    """The two 404 conditions must not be reported with the same words."""
    no_session = make_reporter()
    await no_session.on_response(make_response(404))
    in_session = make_reporter()
    await in_session.on_response(make_response(404, headers={"mcp-session-id": "abc123"}))
    capsys.readouterr()

    assert no_session.fatal_message != in_session.fatal_message


# ---------------------------------------------------------------------------
# 11: a stalled error body must not hang or raise
# ---------------------------------------------------------------------------


class StallingStream(httpx.AsyncByteStream):
    """A body that never arrives, to exercise the read guard."""

    async def __aiter__(self) -> AsyncIterator[bytes]:
        await anyio.Event().wait()
        yield b""  # pragma: no cover - unreachable, the event is never set


@pytest.mark.anyio
async def test_stalled_body_read_times_out_without_raising(capsys, monkeypatch):
    """The hook gives up on the body, reports anyway, and omits the snippet."""
    monkeypatch.setattr("uc_mcp_proxy.errors._BODY_READ_TIMEOUT", 0.05)

    reporter = make_reporter()
    started = time.monotonic()
    await reporter.on_response(make_response(500, stream=StallingStream()))
    elapsed = time.monotonic() - started

    err = capsys.readouterr().err
    assert elapsed < 1.0
    assert "500" in err
    assert "server:" not in err


# ---------------------------------------------------------------------------
# 12: nothing escapes on_response except cancellation
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_formatter_failure_degrades_to_a_fallback_line(capsys, monkeypatch):
    """A formatting bug must not become the traceback this module prevents."""
    from uc_mcp_proxy.errors import HttpErrorReporter

    reporter = make_reporter()
    await reporter.on_response(make_response(401))
    first_message = reporter.last_message
    capsys.readouterr()

    def boom(*args, **kwargs):
        raise RuntimeError("formatter is broken")

    monkeypatch.setattr(HttpErrorReporter, "_format", boom)
    await reporter.on_response(make_response(500))

    err = capsys.readouterr().err
    assert "500" in err
    assert URL in err
    assert "Traceback" not in err
    assert reporter.last_message is not None
    assert reporter.last_message != first_message
    assert "500" in reporter.last_message


@pytest.mark.anyio
async def test_cancellation_is_not_swallowed_by_the_hook(capsys):
    """The guards catch ``Exception``; cancellation must still propagate.

    Cancelled while the body read is in flight: if ``on_response`` absorbed the
    cancellation it would return normally and go on to print a diagnosis, so
    the unreached statement and the silent stderr both witness propagation.
    ``trio.Cancelled`` cannot be raised directly, so this drives a real cancel
    scope rather than constructing the exception.
    """
    reporter = make_reporter()
    returned_normally = False

    with anyio.move_on_after(0.05) as scope:
        await reporter.on_response(make_response(500, stream=StallingStream()))
        returned_normally = True

    assert scope.cancelled_caught
    assert returned_normally is False
    assert capsys.readouterr().err == ""
    assert reporter.last_message is None


# ---------------------------------------------------------------------------
# 16: the exception-group flattener decides what the backstop may swallow
# ---------------------------------------------------------------------------


def make_status_error() -> httpx.HTTPStatusError:
    request = httpx.Request("POST", URL)
    return httpx.HTTPStatusError(
        "Server error",
        request=request,
        response=httpx.Response(500, request=request),
    )


SWALLOW_CASES = {
    "bare_http_status_error": lambda: make_status_error(),
    "group_of_one": lambda: _BaseExceptionGroup("g", [make_status_error()]),
    "nested_group": lambda: _BaseExceptionGroup("g", [_BaseExceptionGroup("h", [make_status_error()])]),
}

RERAISE_CASES = {
    "bare_cancelled": lambda: asyncio.CancelledError(),
    "bare_keyboard_interrupt": lambda: KeyboardInterrupt(),
    "bare_system_exit": lambda: SystemExit(),
    "group_of_cancelled": lambda: _BaseExceptionGroup("g", [asyncio.CancelledError()]),
    "group_with_value_error": lambda: _BaseExceptionGroup("g", [make_status_error(), ValueError("nope")]),
    "group_with_cancelled": lambda: _BaseExceptionGroup("g", [make_status_error(), asyncio.CancelledError()]),
}


@pytest.mark.parametrize("factory", SWALLOW_CASES.values(), ids=list(SWALLOW_CASES))
def test_is_only_http_status_errors_accepts_pure_status_failures(factory):
    """Groups whose every leaf is an HTTPStatusError may be swallowed."""
    from uc_mcp_proxy.errors import is_only_http_status_errors

    assert is_only_http_status_errors(factory()) is True


@pytest.mark.parametrize("factory", RERAISE_CASES.values(), ids=list(RERAISE_CASES))
def test_is_only_http_status_errors_rejects_anything_else(factory):
    """Cancellation, interrupts and mixed groups must reach the caller."""
    from uc_mcp_proxy.errors import is_only_http_status_errors

    assert is_only_http_status_errors(factory()) is False


def test_leaves_flattens_nested_groups():
    """``_leaves`` returns the non-group leaves in order."""
    from uc_mcp_proxy.errors import _leaves

    inner = make_status_error()
    other = ValueError("nope")
    group = _BaseExceptionGroup("g", [_BaseExceptionGroup("h", [inner]), other])

    assert _leaves(group) == [inner, other]


def test_leaves_returns_a_bare_exception_unchanged():
    """A non-group exception is its own only leaf."""
    from uc_mcp_proxy.errors import _leaves

    exc = make_status_error()

    assert _leaves(exc) == [exc]


@pytest.mark.anyio
@pytest.mark.parametrize("status", [400, 429])
async def test_generic_4xx_is_reported_without_blaming_credentials(status, capsys):
    """A rate limit or a bad request is neither an auth failure nor a server fault."""
    reporter = make_reporter()
    await reporter.on_response(make_response(status))

    err = capsys.readouterr().err
    assert str(status) in err
    assert "not an authentication failure" in err
    assert "rejected your credentials" not in err
    assert "server-side error" not in err
    # Still a request the proxy needed to make, so it is still fatal.
    assert reporter.fatal.is_set()


@pytest.mark.anyio
async def test_control_characters_are_stripped_from_the_body_snippet(capsys):
    """A hostile server must not be able to drive the operator's terminal.

    The snippet is the only remote-controlled text the proxy prints. Collapsing
    whitespace removes newlines and carriage returns but leaves ESC intact, so
    without an explicit strip the server could emit cursor-movement and
    erase-line sequences that overwrite the diagnosis printed above it.
    """
    hostile = "\x1b[1A\x1b[2Kuc-mcp-proxy: credentials accepted\x07\x9b31m"
    reporter = make_reporter()
    await reporter.on_response(make_response(500, text=hostile))

    err = capsys.readouterr().err
    assert "\x1b" not in err
    assert "\x07" not in err
    assert "\x9b" not in err
    # The text itself still shows, so the operator sees what the server claimed.
    assert "credentials accepted" in err
    # ...but only inside the server-echo line, which the real diagnosis frames.
    assert "the remote MCP server failed" in err
    assert err.rstrip().endswith("Exiting.")
