"""Tests for the HTTP error diagnosis hooks in ``uc_mcp_proxy.errors``.

These are hook-level tests: responses are built by hand and handed straight to
``HttpErrorReporter.on_response``, so each test pins one behaviour of the hook
without standing up the MCP SDK. The end-to-end counterparts live in
``test_http_errors_e2e.py``.
"""

from __future__ import annotations

import asyncio
import builtins
import sys
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


def await_sync(coro):
    """Drive a coroutine to completion from a sync test."""
    import asyncio

    return asyncio.run(coro)


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


# ---------------------------------------------------------------------------
# 17: an armed retry suppresses exactly one 401, and only a 401
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_armed_401_is_suppressed_and_leaves_no_trace(capsys):
    """An armed 401 prints a retry notice but must not consume the dedup slot.

    Leaving ``reported`` untouched is load-bearing: a suppressed 401 must not
    consume the ``(role, status)`` dedup slot the real report will need, or a
    second 401 -- the retry's own -- would be silently swallowed by the dedup
    check instead of reported.
    """
    from uc_mcp_proxy.errors import arm_retry

    reporter = make_reporter()
    response = make_response(401)
    arm_retry(response.request, armed=True)

    await reporter.on_response(response)

    err = capsys.readouterr().err
    assert len(err.strip().splitlines()) == 1
    assert "retry" in err.lower()
    assert reporter.reported == set()
    assert reporter.fatal_message is None
    assert not reporter.fatal.is_set()
    assert reporter.diagnosed is False


@pytest.mark.anyio
async def test_unarmed_401_is_reported_normally(capsys):
    """Non-vacuous counterpart to the armed case: an unarmed 401 is reported."""
    reporter = make_reporter()
    response = make_response(401)

    await reporter.on_response(response)

    err = capsys.readouterr().err
    assert "401" in err
    assert reporter.reported == {("request", 401)}
    assert reporter.fatal_message is not None
    assert reporter.fatal.is_set()
    assert reporter.diagnosed is True


@pytest.mark.anyio
async def test_armed_non_401_is_still_reported(capsys):
    """Suppression is 401-only: an armed request still reports a 500."""
    from uc_mcp_proxy.errors import arm_retry

    reporter = make_reporter()
    response = make_response(500)
    arm_retry(response.request, armed=True)

    await reporter.on_response(response)

    err = capsys.readouterr().err
    assert "500" in err
    assert reporter.reported == {("request", 500)}
    assert reporter.fatal.is_set()


@pytest.mark.anyio
async def test_disarmed_retry_401_is_reported(capsys):
    """A retry marker flipped back off no longer suppresses the 401."""
    from uc_mcp_proxy.errors import arm_retry

    reporter = make_reporter()
    response = make_response(401)
    arm_retry(response.request, armed=True)
    arm_retry(response.request, armed=False)

    await reporter.on_response(response)

    err = capsys.readouterr().err
    assert "401" in err
    assert reporter.reported == {("request", 401)}
    assert reporter.fatal.is_set()
    assert reporter.fatal_message is not None


# ---------------------------------------------------------------------------
# 18: proxy-authored remediation replaces the generic 401/403 advice
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_proxy_remediation_replaces_the_generic_401_text(capsys):
    """Proxy-authored remediation substitutes for the default 401 wording."""
    from uc_mcp_proxy.errors import set_remediation

    reporter = make_reporter()
    response = make_response(401)
    set_remediation(response.request, "SENTINEL-REMEDIATION")

    await reporter.on_response(response)

    err = capsys.readouterr().err
    assert "SENTINEL-REMEDIATION" in err
    assert "may have expired" not in err.lower()
    assert "rejected your credentials" in err.lower()


@pytest.mark.anyio
async def test_proxy_remediation_replaces_the_403_oauth_u2m_advice(capsys):
    """Proxy-authored remediation substitutes for the default 403 wording.

    When the proxy has already exchanged the credential for this target,
    advising a browser login is the one remedy it just made unnecessary --
    and this feature's audience has no browser.
    """
    from uc_mcp_proxy.errors import set_remediation

    reporter = make_reporter()
    response = make_response(403)
    set_remediation(response.request, "SENTINEL-REMEDIATION")

    await reporter.on_response(response)

    err = capsys.readouterr().err
    assert "SENTINEL-REMEDIATION" in err
    assert "OAuth U2M" not in err


@pytest.mark.anyio
async def test_403_without_remediation_keeps_the_default_advice(capsys):
    """Non-vacuous counterpart: with no remediation set, the default persists."""
    reporter = make_reporter()

    await reporter.on_response(make_response(403))

    err = capsys.readouterr().err
    assert "OAuth U2M" in err


# ---------------------------------------------------------------------------
# 19: report_fatal is the second entrance, reserved for proxy-side failures
# ---------------------------------------------------------------------------


def test_report_fatal_sets_every_field_the_backstop_reads(capsys):
    """One call to ``report_fatal`` leaves the whole post-state consistent."""
    reporter = make_reporter()
    message = "uc-mcp-proxy: token exchange failed."

    reporter.report_fatal(message)

    err = capsys.readouterr().err
    assert err.count(message) == 1
    assert reporter.fatal_message == message
    assert reporter.last_message == message
    assert reporter.fatal.is_set()
    assert reporter.reported == set()
    assert reporter.diagnosed is True


def test_report_fatal_is_idempotent_and_silent_while_shutting_down(capsys):
    """A second call is silent, and so is any call made during shutdown.

    The SDK's teardown DELETE goes out through the same auth flow, so a
    session that outlived its token can reach this on the way out. A clean
    multi-hour session must not exit non-zero because a refresh failed during
    shutdown.
    """
    reporter = make_reporter()
    first_message = "uc-mcp-proxy: token exchange failed."
    reporter.report_fatal(first_message)
    capsys.readouterr()

    reporter.report_fatal("uc-mcp-proxy: a different message entirely.")

    assert capsys.readouterr().err == ""
    assert reporter.fatal_message == first_message
    assert reporter.last_message == first_message

    shutting_down_reporter = make_reporter()
    shutting_down_reporter.shutting_down = True

    shutting_down_reporter.report_fatal(first_message)

    assert capsys.readouterr().err == ""
    assert shutting_down_reporter.fatal_message is None
    assert shutting_down_reporter.last_message is None
    assert not shutting_down_reporter.fatal.is_set()


# ---------------------------------------------------------------------------
# 20: diagnosed answers "has the user already been told?" from either entrance
# ---------------------------------------------------------------------------


@pytest.mark.anyio
@pytest.mark.parametrize("trigger", ["reported_failure", "report_fatal"])
async def test_diagnosed_is_true_from_either_entrance(trigger):
    """Both ``_report`` and ``report_fatal`` flip ``diagnosed`` to True."""
    reporter = make_reporter()
    assert reporter.diagnosed is False

    if trigger == "reported_failure":
        await reporter.on_response(make_response(500))
    else:
        reporter.report_fatal("uc-mcp-proxy: proxy-side failure.")

    assert reporter.diagnosed is True


# ---------------------------------------------------------------------------
# 21: is_only_diagnosed_errors generalizes the swallow decision to ProxyFatalError
# ---------------------------------------------------------------------------


def make_proxy_fatal_error():
    from uc_mcp_proxy.errors import ProxyFatalError

    return ProxyFatalError("diagnosed already")


class _EmptyGroupStub(BaseException):
    """Duck-types as an exception group with zero leaves.

    The real ``BaseExceptionGroup`` constructor refuses an empty sequence, so
    an actual empty group cannot be built; this stands in for one to exercise
    the ``bool(leaves)`` guard in ``_leaves``' caller.
    """

    exceptions: tuple[BaseException, ...] = ()


DIAGNOSED_SWALLOW_CASES = {
    "bare_http_status_error": lambda: make_status_error(),
    "bare_proxy_fatal_error": lambda: make_proxy_fatal_error(),
    "group_of_status_and_proxy_fatal": lambda: _BaseExceptionGroup(
        "g", [make_status_error(), make_proxy_fatal_error()]
    ),
    "nested_group": lambda: _BaseExceptionGroup("g", [_BaseExceptionGroup("h", [make_status_error()])]),
}

DIAGNOSED_RERAISE_CASES = {
    "bare_cancelled": lambda: asyncio.CancelledError(),
    "bare_keyboard_interrupt": lambda: KeyboardInterrupt(),
    "bare_system_exit": lambda: SystemExit(),
    "bare_runtime_error": lambda: RuntimeError("boom"),
    "group_of_cancelled": lambda: _BaseExceptionGroup("g", [asyncio.CancelledError()]),
    "group_with_runtime_error": lambda: _BaseExceptionGroup("g", [make_status_error(), RuntimeError("boom")]),
    "group_with_cancelled": lambda: _BaseExceptionGroup("g", [make_status_error(), asyncio.CancelledError()]),
    "empty_group": lambda: _EmptyGroupStub(),
}


@pytest.mark.parametrize("factory", DIAGNOSED_SWALLOW_CASES.values(), ids=list(DIAGNOSED_SWALLOW_CASES))
def test_is_only_diagnosed_errors_accepts_status_and_proxy_fatal_leaves(factory):
    """Groups whose every leaf is a status error or a ``ProxyFatalError`` pass."""
    from uc_mcp_proxy.errors import is_only_diagnosed_errors

    assert is_only_diagnosed_errors(factory()) is True


@pytest.mark.parametrize("factory", DIAGNOSED_RERAISE_CASES.values(), ids=list(DIAGNOSED_RERAISE_CASES))
def test_is_only_diagnosed_errors_rejects_anything_else(factory):
    """Cancellation, other exceptions, and an empty group must reach the caller.

    The empty and cancelled cases are why this is a positive, non-empty match
    -- otherwise the backstop would swallow a Ctrl-C and report it as a
    credential rejection.
    """
    from uc_mcp_proxy.errors import is_only_diagnosed_errors

    assert is_only_diagnosed_errors(factory()) is False


# ---------------------------------------------------------------------------
# 22: scrub_body redacts before truncating and strips terminal control codes
# ---------------------------------------------------------------------------


def test_scrub_body_redacts_before_truncating():
    """A secret straddling the 500-char boundary must be fully redacted.

    Truncating first would half-print a credential: the portion of the raw
    secret that falls inside the first 500 characters would survive even
    though the rest was cut off.
    """
    from uc_mcp_proxy.errors import scrub_body

    secret = "SECRET-XYZ-1234567890ABCDEFGH"  # 30 chars
    padding = "a" * 490
    text = padding + secret  # secret spans chars 490-519, straddling char 500

    result = scrub_body(text, [secret])

    assert secret not in result
    assert "<redacted>" in result
    assert len(result) <= 500


def test_scrub_body_strips_control_characters():
    """ESC and CSI sequences are removed from the scrubbed text."""
    from uc_mcp_proxy.errors import scrub_body

    hostile = "before\x1b[31mafter\x9b1mtail\x07end"

    result = scrub_body(hostile, [])

    assert "\x1b" not in result
    assert "\x9b" not in result
    assert "\x07" not in result


def test_scrub_body_collapses_whitespace_and_truncates():
    """Newlines and tabs collapse to single spaces, and output is bounded."""
    from uc_mcp_proxy.errors import scrub_body

    text = "line one\n\t line two\r\n" * 50

    result = scrub_body(text, [])

    assert len(result) <= 500
    assert "\n" not in result
    assert "\t" not in result
    assert "\r" not in result


def test_scrub_body_ignores_empty_secrets():
    """An empty secret must not splice ``<redacted>`` between every character."""
    from uc_mcp_proxy.errors import scrub_body

    text = "real secret embedded here and real again"

    result = scrub_body(text, ["", "real"])

    assert "real" not in result
    assert result.count("<redacted>") == 2


# ---------------------------------------------------------------------------
# 24: the scrubber cannot be walked around by splitting the secret
# ---------------------------------------------------------------------------


SPLIT_SECRET_CASES = {
    "nul_byte": "\x00",
    "escape": "\x1b",
    "newline": "\n",
    "tab": "\t",
    "carriage_return": "\r",
    "c1_control": "\x85",
    "zero_width_space": "​",
    "bidi_override": "‮",
    "single_space": " ",
    "run_of_spaces": "   ",
}


@pytest.mark.parametrize("separator", SPLIT_SECRET_CASES.values(), ids=list(SPLIT_SECRET_CASES))
def test_scrub_body_redacts_a_secret_the_server_split(separator):
    """A credential echoed with a byte inserted into it must still be redacted.

    This is the whole reason normalization runs before redaction. Redacting
    first, the inserted byte defeats ``str.replace``, and the control strip that
    follows then *deletes* it -- reassembling the secret verbatim on its way to
    the terminal. The proxy would have printed the credential itself.
    """
    from uc_mcp_proxy.errors import scrub_body

    secret = "dapiDEADBEEF0123456789abcdef"
    split = secret[:14] + separator + secret[14:]

    result = scrub_body(f'{{"error":"rejected {split}"}}', [secret])

    assert secret not in result, f"credential reassembled: {result!r}"
    assert "<redacted>" in result
    # Non-vacuous: the surrounding text really did survive, so this is not
    # passing because the whole body vanished.
    assert "rejected" in result


def test_scrub_body_still_redacts_an_unsplit_secret():
    """The ordinary case keeps working -- guards against over-fitting to splits."""
    from uc_mcp_proxy.errors import scrub_body

    secret = "dapiDEADBEEF0123456789abcdef"

    assert scrub_body(f"body {secret} end", [secret]) == "body <redacted> end"


def test_scrub_body_strips_bidi_and_zero_width_characters():
    """Characters that reorder the rendered line are removed along with C0/C1.

    They cannot move the cursor, but they can visually detach a ``<redacted>``
    marker from what it redacts, or render a hostname backwards.
    """
    from uc_mcp_proxy.errors import scrub_body

    result = scrub_body("safe ‮ evil ⁦x⁩ ​ ﻿ end", [])

    for char in ("‮", "⁦", "⁩", "​", "﻿"):
        assert char not in result
    assert "safe" in result and "end" in result


# ---------------------------------------------------------------------------
# 25: the reason phrase is server-authored too
# ---------------------------------------------------------------------------


def test_scrub_reason_strips_escapes_and_bounds_length():
    """The status line is as server-controlled as the body, and far longer.

    h11's grammar rejects only NUL and whitespace, so ESC reaches us intact and
    httpx's ASCII decode preserves it -- and h11 allows 16 KiB of it, where
    bodies are capped at 500.
    """
    from uc_mcp_proxy.errors import scrub_reason

    hostile = "Forbidden\x1b[2K\x1b[1Auc-mcp-proxy: connected OK" + "A" * 500

    result = scrub_reason(hostile)

    assert "\x1b" not in result
    assert len(result) <= 80


@pytest.mark.anyio
async def test_reported_message_never_carries_an_escape_from_the_reason_phrase(capsys):
    """End to end: a hostile status line cannot rewrite the terminal.

    Without this the escape defense built for the body snippet is simply walked
    around -- the headline interpolates the reason phrase directly.
    """
    from uc_mcp_proxy.errors import _ROLE_KEY

    reporter = make_reporter()
    request = httpx.Request("POST", URL, extensions={_ROLE_KEY: "request"})
    response = httpx.Response(403, request=request, text="{}")
    # httpx derives reason_phrase from the status code, so set it directly --
    # this is the value a real server puts on the wire.
    response.extensions = dict(response.extensions, reason_phrase=b"Forbidden\x1b[2K\x1b[1AFAKE")

    await reporter.on_response(response)

    err = capsys.readouterr().err
    assert "\x1b" not in err
    assert "403" in err


# ---------------------------------------------------------------------------
# 26: the forwarded-identity header must not outlive its origin
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_guard_keeps_forwarded_token_on_the_original_origin():
    """The header survives the hop the user actually asked for."""
    from uc_mcp_proxy.errors import guard_forwarded_token

    request = httpx.Request("POST", URL, headers={"X-Forwarded-Access-Token": "tok"})
    await guard_forwarded_token(request)

    assert request.headers["X-Forwarded-Access-Token"] == "tok"


@pytest.mark.anyio
async def test_guard_drops_forwarded_token_on_a_cross_origin_hop():
    """A redirect off the target origin must not carry the credential along.

    httpx strips ``Authorization`` on a cross-origin hop but knows nothing about
    this header, so the foreign origin would otherwise learn a live token *and*
    learn it in the one case where the real credential was already removed.
    """
    from uc_mcp_proxy.errors import _ORIGIN_KEY, guard_forwarded_token

    # Extensions are copied per hop, so the rebuilt request carries the origin
    # recorded on the first one -- the same mechanism ``stamp_role`` relies on.
    redirected = httpx.Request(
        "GET",
        "https://elsewhere.example.net/x",
        headers={"X-Forwarded-Access-Token": "tok"},
        extensions={_ORIGIN_KEY: ("https", "example.com", None)},
    )
    await guard_forwarded_token(redirected)

    assert "X-Forwarded-Access-Token" not in redirected.headers


@pytest.mark.anyio
async def test_guard_never_raises_on_a_malformed_request():
    """Request hooks run outside httpx's ``try``; a raise here escapes entirely."""
    from uc_mcp_proxy.errors import guard_forwarded_token

    class _Exploding:
        url = property(lambda self: (_ for _ in ()).throw(RuntimeError("boom")))

    await guard_forwarded_token(_Exploding())  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 27: nothing may truncate before redaction, at any internal boundary
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("offset", [-2, -1, 0, 1, 2], ids=lambda n: f"straddle{n:+d}")
def test_no_internal_boundary_can_sever_a_secret(offset):
    """A secret is redacted wherever it falls, however long the body is.

    An earlier version bounded the redaction window to the first 8 KiB *before*
    stripping control characters. Because stripping deletes, content past that
    cut shifted left into the visible window while the cut itself had already
    severed the secret -- printing half a credential. There is now no truncation
    before redaction anywhere; this pins that for a body far larger than any
    such window.
    """
    from uc_mcp_proxy.errors import scrub_body

    secret = "dapiDEADBEEF0123456789abcdef"
    # Control padding is deleted, so the visible text stays inside the snippet
    # limit while the raw string is long enough to cross any internal bound.
    padding = "\x01" * 8000 + "A" * (192 + offset)
    result = scrub_body(padding + secret + " tail", [secret])

    longest = max((n for n in range(len(secret), 3, -1) if secret[:n] in result), default=0)
    assert longest == 0, f"leaked a {longest}-character prefix: {result[-60:]!r}"
    assert "<redacted>" in result


def test_snippet_read_is_bounded(monkeypatch):
    """A huge error body is not buffered whole to produce a 500-char snippet.

    The 5-second read timeout caps how *long* a server may talk, which on a
    fast link is still hundreds of megabytes.
    """
    from uc_mcp_proxy import errors

    monkeypatch.setattr(errors, "_MAX_SNIPPET_BYTES", 64)
    delivered: list[int] = []

    class _Chunked(httpx.AsyncByteStream):
        async def __aiter__(self):
            for _ in range(1000):
                delivered.append(1)
                yield b"B" * 32

    reporter = make_reporter()
    response = make_response(500, stream=_Chunked())

    snippet = await_sync(reporter._read_snippet(response))

    assert len(snippet) <= 64
    assert len(delivered) <= 3, f"read {len(delivered)} chunks; the cap did not stop it"


# ---------------------------------------------------------------------------
# 28: invisible characters must not be able to hide a credential
# ---------------------------------------------------------------------------


def _invisible_separators():
    """Every non-whitespace Cc/Cf codepoint, sampled across the ranges.

    Generated from Unicode categories rather than listed. A hand-written list
    was wrong twice, and each time the fix was to add the members someone had
    just thought of.
    """
    import unicodedata

    found = [
        cp
        for cp in range(sys.maxunicode + 1)
        if unicodedata.category(chr(cp)) in ("Cc", "Cf") and not chr(cp).isspace()
    ]
    return found[::37]  # a spread across every range, not just the low ones


@pytest.mark.parametrize("codepoint", _invisible_separators(), ids=lambda cp: f"U+{cp:04X}")
def test_invisible_characters_cannot_hide_a_secret(codepoint):
    """A credential interleaved with invisible characters is still redacted.

    These render as nothing, so a log reader sees the bare credential while an
    exact-match redactor -- and a secret scanner grepping the file -- sees
    something that does not match.
    """
    from uc_mcp_proxy.errors import scrub_body

    secret = "dapiDEADBEEF0123456789abcdef"
    hidden = chr(codepoint).join(secret)

    result = scrub_body(f"error {hidden} end", [secret])

    assert secret not in result
    assert chr(codepoint) not in result
    assert "<redacted>" in result


# ---------------------------------------------------------------------------
# 29: the reason phrase is a credential channel, not just an escape channel
# ---------------------------------------------------------------------------


def test_scrub_reason_redacts_secrets_not_only_escapes():
    """Stripping escapes without redacting leaves a full-disclosure channel."""
    from uc_mcp_proxy.errors import scrub_reason

    secret = "dapiDEADBEEF0123456789abcdef"

    result = scrub_reason(f"Forbidden token={secret}", [secret])

    assert secret not in result
    assert "<redacted>" in result


@pytest.mark.anyio
async def test_headline_never_carries_a_credential_from_the_reason_phrase(capsys):
    """End to end: the status line cannot print what the body line redacts.

    A server needs no padding and no positioning for this -- it echoes the
    credential it was just sent, in the one field that was not being scrubbed,
    and it lands on the line directly above a correctly-redacted body.
    """
    from uc_mcp_proxy.errors import _ROLE_KEY

    token = "dapiDEADBEEF0123456789abcdef"
    reporter = make_reporter()
    request = httpx.Request(
        "POST", URL, headers={"Authorization": f"Bearer {token}"}, extensions={_ROLE_KEY: "request"}
    )
    response = httpx.Response(403, request=request, text="{}")
    response.extensions = dict(response.extensions, reason_phrase=f"Forbidden token={token}".encode())

    await reporter.on_response(response)

    err = capsys.readouterr().err
    assert token not in err, "the credential reached stderr via the status line"
    assert "403" in err


# ---------------------------------------------------------------------------
# 30: the redirect guard covers every header this module calls a secret
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_guard_drops_the_session_id_cross_origin():
    """``mcp-session-id`` is declared a secret here, so it must not travel either.

    Unlike the forwarded token, this one discloses to a party that never held
    it: httpx strips ``Authorization`` on the hop, so the foreign origin would
    otherwise receive a live session id and nothing else to explain it.
    """
    from uc_mcp_proxy.errors import _ORIGIN_KEY, guard_forwarded_token

    redirected = httpx.Request(
        "GET",
        "https://elsewhere.example.net/x",
        headers={"mcp-session-id": "SESSION-SECRET", "X-Forwarded-Access-Token": "tok"},
        extensions={_ORIGIN_KEY: ("https", "example.com", None)},
    )
    await guard_forwarded_token(redirected)

    assert "mcp-session-id" not in redirected.headers
    assert "X-Forwarded-Access-Token" not in redirected.headers


@pytest.mark.anyio
async def test_guard_drops_credentials_on_a_scheme_downgrade():
    """Same host over plain http is a different origin, and the riskier one."""
    from uc_mcp_proxy.errors import _ORIGIN_KEY, guard_forwarded_token

    downgraded = httpx.Request(
        "GET",
        "http://example.com/mcp",
        headers={"X-Forwarded-Access-Token": "tok"},
        extensions={_ORIGIN_KEY: ("https", "example.com", None)},
    )
    await guard_forwarded_token(downgraded)

    assert "X-Forwarded-Access-Token" not in downgraded.headers


@pytest.mark.anyio
async def test_guard_fails_closed_when_the_origin_cannot_be_determined():
    """If anything goes wrong mid-check the credentials stay off, not on.

    Failing open here would hand a credential to a foreign origin, which is the
    opposite default from ``stamp_role``, where a failure merely loses a label.
    """
    from uc_mcp_proxy.errors import guard_forwarded_token

    class _BadUrl(httpx.Request):
        @property
        def url(self):
            raise RuntimeError("boom")

    request = httpx.Request("GET", URL, headers={"X-Forwarded-Access-Token": "tok"})
    # Swapped after construction: httpx assigns ``self.url`` in ``__init__``,
    # and a property is a data descriptor, so it wins over the instance dict.
    request.__class__ = _BadUrl

    await guard_forwarded_token(request)  # must not raise

    assert "X-Forwarded-Access-Token" not in request.headers


# ---------------------------------------------------------------------------
# 31: a secret severed by the read cap must not print its surviving half
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("straddle", [4, 6, 20, 27], ids=lambda n: f"cut_leaves_{n}")
def test_secret_severed_by_the_read_cap_is_not_printed(straddle):
    """The read stops at a byte cap, and that cap can land inside a credential.

    Redaction cannot match what was already cut, and reading further does not
    help -- the next cap has the same edge. What survives is a prefix at the end
    of the text, and if the discarded remainder was mostly invisible characters
    it sits well inside the visible window rather than being truncated away.
    """
    from uc_mcp_proxy.errors import scrub_body

    secret = "dapiDEADBEEF0123456789abcdef"
    severed = "\x01" * 300 + "A" * 40 + secret[:straddle]

    result = scrub_body(severed, [secret])

    longest = max((n for n in range(len(secret), 3, -1) if secret[:n] in result), default=0)
    assert longest == 0, f"leaked a {longest}-character prefix: {result[-50:]!r}"


def test_a_short_trailing_coincidence_is_left_alone():
    """The trailing sweep must not mangle ordinary text.

    Counterpart to the test above: a fragment too short to be worth anything is
    not worth false-positives either.
    """
    from uc_mcp_proxy.errors import scrub_body

    assert scrub_body("the value is da", ["dapiDEADBEEF0123456789abcdef"]).endswith("da")


# ---------------------------------------------------------------------------
# 32: the redactor's pattern must stay unambiguous
# ---------------------------------------------------------------------------


def test_stripping_control_characters_cannot_recreate_a_whitespace_run():
    """Collapse runs last, so no run longer than one space can survive.

    Stripping after collapsing looks equivalent and is not: deleting a control
    character re-joins the spaces either side of it, re-creating a run the
    collapse had already flattened. The redactor's pattern is only free of
    ambiguity because such a run cannot exist, so this is load-bearing.
    """
    import re as _re

    from uc_mcp_proxy.errors import scrub_body

    result = scrub_body("A" + " \x00" * 12 + "B", [])

    longest = max((len(run) for run in _re.findall(r" +", result)), default=0)
    assert longest <= 1, f"whitespace run of {longest} survived: {result!r}"


def test_a_server_chosen_secret_cannot_make_redaction_expensive():
    """``mcp-session-id`` is server-authored, unvalidated, and used as a secret.

    A secret carrying whitespace once produced an ambiguous pattern group per
    space; against a whitespace run that is combinatorial, and this runs
    synchronously inside a response hook, so the stdio bridge and the abort
    path both stop with it. Timed rather than asserted structurally because the
    failure mode is latency, not a wrong answer.
    """
    import time

    from uc_mcp_proxy.errors import scrub_body

    body = "S" + " \x00" * 40 + "X"

    started = time.perf_counter()
    for spaces in range(4, 20):
        scrub_body(body, ["S" + " " * spaces + "E"])
    elapsed = time.perf_counter() - started

    assert elapsed < 1.0, f"redaction took {elapsed:.1f}s; the pattern is backtracking"


def test_a_very_long_server_chosen_secret_is_matched_literally():
    """Pattern construction is linear in the needle, and the server picks it."""
    import time

    from uc_mcp_proxy.errors import scrub_body

    secret = "S" * 16384

    started = time.perf_counter()
    # Secret first, so the redaction is inside the visible window rather than
    # being cut away by the snippet limit -- the assertion is about the cost of
    # the match, but it should not pass merely because nothing was printed.
    result = scrub_body(secret + "A" * 60000, [secret])
    elapsed = time.perf_counter() - started

    assert result.startswith("<redacted>")
    assert elapsed < 1.0, f"took {elapsed:.1f}s"


def test_a_secret_that_prefixes_another_does_not_shadow_it():
    """Longest-first, or the short one redacts and leaves the long one's tail."""
    from uc_mcp_proxy.errors import scrub_body

    assert scrub_body("x AAAABBBB y", ["AAAA", "AAAABBBB"]) == "x <redacted> y"
