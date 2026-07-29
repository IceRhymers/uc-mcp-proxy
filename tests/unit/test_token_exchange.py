"""Tests for the RFC 8693 PAT-to-app-token exchange in ``uc_mcp_proxy.token_exchange``.

``exchange_pat`` builds its own *synchronous* ``httpx.Client``, so every test
here is a plain ``def`` driven by ``httpx.MockTransport`` -- there is no event
loop to await. The ``anyio`` mark is kept in ``pytestmark`` for consistency
with the rest of the suite even though it has no effect on sync tests.
"""

from __future__ import annotations

from urllib.parse import parse_qs

import httpx
import pytest

from uc_mcp_proxy.errors import ProxyFatalError
from uc_mcp_proxy.token_exchange import (
    _DEFAULT_EXPIRES_IN,
    _EXPIRY_MARGIN_SECONDS,
    _GRANT_TYPE,
    _REQUESTED_TOKEN_TYPE,
    _SUBJECT_TOKEN_TYPE,
    ExchangeConfig,
    ExchangedToken,
    TokenExchangeError,
    exchange_pat,
)

pytestmark = [pytest.mark.unit, pytest.mark.anyio]

PAT = "dapi-fake-pat-DO-NOT-LOG"
HOST = "https://test-workspace.cloud.databricks.com"
CLIENT_ID = "00000000-1111-2222-3333-444444444444"


def make_config(**overrides: object) -> ExchangeConfig:
    """A baseline ``ExchangeConfig`` with two scopes, overridable per test."""
    fields: dict[str, object] = {
        "host": HOST,
        "client_id": CLIENT_ID,
        "scopes": ("sql", "dashboards.genie"),
        "verify_ssl": True,
    }
    fields.update(overrides)
    return ExchangeConfig(**fields)  # type: ignore[arg-type]


def ok_handler(request: httpx.Request) -> httpx.Response:
    """A handler that always succeeds with a minimal token payload."""
    return httpx.Response(200, json={"access_token": "app-scoped-token"})


# ---------------------------------------------------------------------------
# Request shape
# ---------------------------------------------------------------------------


def test_sends_the_exact_rfc_8693_form_body():
    """The POST body has exactly the five RFC 8693 fields, and the PAT rides
    both as ``subject_token`` and as the bearer header -- not just one."""
    captured: dict[str, httpx.Request] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["request"] = request
        return httpx.Response(200, json={"access_token": "app-scoped-token"})

    config = make_config()
    exchange_pat(PAT, config, transport=httpx.MockTransport(handler))

    request = captured["request"]
    form = parse_qs(request.content.decode())
    assert form == {
        "grant_type": [_GRANT_TYPE],
        "subject_token": [PAT],
        "subject_token_type": [_SUBJECT_TOKEN_TYPE],
        "requested_token_type": [_REQUESTED_TOKEN_TYPE],
        "audience": [CLIENT_ID],
        "scope": ["sql dashboards.genie"],
    }
    assert request.headers["Authorization"] == f"Bearer {PAT}"


def test_endpoint_is_derived_from_the_workspace_host():
    """The token endpoint is the workspace host's ``/oidc/v1/token``, and a
    trailing slash on the configured host must not double up the slash."""
    captured: dict[str, httpx.Request] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["request"] = request
        return httpx.Response(200, json={"access_token": "app-scoped-token"})

    exchange_pat(PAT, make_config(host=HOST), transport=httpx.MockTransport(handler))
    assert str(captured["request"].url) == f"{HOST}/oidc/v1/token"

    exchange_pat(PAT, make_config(host=HOST + "/"), transport=httpx.MockTransport(handler))
    assert str(captured["request"].url) == f"{HOST}/oidc/v1/token"


def test_scope_is_omitted_entirely_when_no_scopes_given():
    """With no scopes configured, ``scope`` must be absent from the form --
    not present with an empty value, which would be a different request."""
    captured: dict[str, httpx.Request] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["request"] = request
        return httpx.Response(200, json={"access_token": "app-scoped-token"})

    exchange_pat(PAT, make_config(scopes=()), transport=httpx.MockTransport(handler))

    form = parse_qs(captured["request"].content.decode())
    assert "scope" not in form


# ---------------------------------------------------------------------------
# Expiry arithmetic
# ---------------------------------------------------------------------------


def test_expiry_margin_is_subtracted_from_the_server_lifetime():
    """``expires_at`` is the issue time plus the server's lifetime, minus the
    renewal margin -- not the raw server-stated deadline."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"access_token": "app-scoped-token", "expires_in": 3600})

    token = exchange_pat(
        PAT,
        make_config(),
        now=lambda: 1000.0,
        transport=httpx.MockTransport(handler),
    )

    assert token.expires_at == 1000.0 + 3600 - 60


def test_expiry_never_goes_negative():
    """A server lifetime shorter than the renewal margin clamps to ``now``,
    the token expires immediately, and it does not report a past deadline."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"access_token": "app-scoped-token", "expires_in": 10})

    token = exchange_pat(
        PAT,
        make_config(),
        now=lambda: 1000.0,
        transport=httpx.MockTransport(handler),
    )

    assert token.expires_at == 1000.0


def test_missing_expires_in_falls_back_to_one_hour():
    """A response with no ``expires_in`` at all uses the one-hour default,
    rather than treating the token as already expired or living forever."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"access_token": "app-scoped-token"})

    token = exchange_pat(
        PAT,
        make_config(),
        now=lambda: 1000.0,
        transport=httpx.MockTransport(handler),
    )

    assert token.expires_at == 1000.0 + _DEFAULT_EXPIRES_IN - _EXPIRY_MARGIN_SECONDS


# ---------------------------------------------------------------------------
# Timeout
# ---------------------------------------------------------------------------


def test_timeout_reaches_the_client(monkeypatch):
    """``ExchangeConfig.timeout`` defaults to 10s -- the proxy's own client
    reads at 300s, and copying that value would freeze the stdio bridge for
    five minutes on every exchange -- and a custom value both survives on the
    config and is the exact value handed to the underlying ``httpx.Client``."""
    assert make_config().timeout == 10.0
    assert make_config(timeout=42.0).timeout == 42.0

    captured: dict[str, object] = {}
    real_client = httpx.Client

    class RecordingClient(real_client):  # type: ignore[misc,valid-type]
        def __init__(self, *args: object, **kwargs: object) -> None:
            captured["timeout"] = kwargs.get("timeout")
            super().__init__(*args, **kwargs)

    monkeypatch.setattr("uc_mcp_proxy.token_exchange.httpx.Client", RecordingClient)

    exchange_pat(
        PAT,
        make_config(timeout=42.0),
        transport=httpx.MockTransport(ok_handler),
    )

    assert captured["timeout"] == 42.0


# ---------------------------------------------------------------------------
# No refresh token
# ---------------------------------------------------------------------------


def test_no_refresh_token_is_stored():
    """Even if the server sends a ``refresh_token``, ``ExchangedToken`` has no
    field for it -- the credential is re-derived from the PAT, never refreshed."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"access_token": "app-scoped-token", "refresh_token": "should-not-be-kept"},
        )

    token = exchange_pat(PAT, make_config(), transport=httpx.MockTransport(handler))

    assert isinstance(token, ExchangedToken)
    assert not hasattr(token, "refresh_token")
    assert "should-not-be-kept" not in vars(token).values()


# ---------------------------------------------------------------------------
# PAT never leaks on failure
# ---------------------------------------------------------------------------


def _handler_400_json(request: httpx.Request) -> httpx.Response:
    return httpx.Response(400, json={"error": "invalid_request"})


def _handler_500(request: httpx.Request) -> httpx.Response:
    return httpx.Response(500, text="internal error")


def _handler_connect_error(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("connection refused", request=request)


def _handler_non_json_200(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, text="<html>not json</html>")


def _handler_200_no_access_token(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={"token_type": "Bearer"})


FAILURE_MODES = {
    "400_json_error_body": _handler_400_json,
    "500": _handler_500,
    "connect_error": _handler_connect_error,
    "200_non_json_body": _handler_non_json_200,
    "200_missing_access_token": _handler_200_no_access_token,
}


@pytest.mark.parametrize("handler", FAILURE_MODES.values(), ids=list(FAILURE_MODES))
def test_pat_never_appears_in_any_failure_message(handler):
    """Across every failure mode -- rejection, server error, transport error,
    a non-JSON body, or a JSON body missing the token -- the raised message
    must never contain the raw PAT, since it was also sent in the header."""
    with pytest.raises(TokenExchangeError) as exc_info:
        exchange_pat(PAT, make_config(), transport=httpx.MockTransport(handler))

    assert PAT not in str(exc_info.value)


# ---------------------------------------------------------------------------
# Failure message shape
# ---------------------------------------------------------------------------


def test_failure_message_names_the_request_shape_but_never_subject_token():
    """The diagnosis names every request parameter except the PAT itself, so
    a platform-side rejection is diagnosable without exposing the credential."""
    with pytest.raises(TokenExchangeError) as exc_info:
        exchange_pat(PAT, make_config(), transport=httpx.MockTransport(_handler_400_json))

    message = str(exc_info.value)
    assert f"{HOST}/oidc/v1/token" in message
    assert "grant_type" in message
    assert "subject_token_type" in message
    assert "requested_token_type" in message
    assert "audience" in message
    assert CLIENT_ID in message
    assert "scope" in message

    # "subject_token_type" legitimately contains "subject_token" as a
    # substring, so a plain `"subject_token" not in message` check would be
    # vacuously true even if the code regressed and printed a
    # "subject_token: <pat>" line, because "subject_token_type" would still
    # be the only match `in` finds first for casual reading. Assert instead,
    # line by line, that no line is a `subject_token:` label -- which is
    # exactly the label the code would emit if it started leaking the PAT --
    # while lines labelled `subject_token_type:` remain untouched.
    for line in message.splitlines():
        assert not line.strip().startswith("subject_token:")


def test_failure_message_notes_the_audience_trap():
    """The message warns that an "invalid audience" error can also mean the
    requested_token_type was rejected, since the platform conflates the two."""
    with pytest.raises(TokenExchangeError) as exc_info:
        exchange_pat(PAT, make_config(), transport=httpx.MockTransport(_handler_400_json))

    message = str(exc_info.value).lower()
    assert "invalid audience" in message
    assert "requested_token_type was rejected" in message


def test_omitted_scope_is_named_in_the_failure_message():
    """With no scopes configured, the failure message spells out that scope
    was omitted rather than showing a blank or absent scope line."""
    with pytest.raises(TokenExchangeError) as exc_info:
        exchange_pat(
            PAT,
            make_config(scopes=()),
            transport=httpx.MockTransport(_handler_400_json),
        )

    assert "(omitted -- no --scope given)" in str(exc_info.value)


def test_echoed_subject_token_is_redacted():
    """If the server's own error body echoes the PAT back, the diagnosis must
    redact it rather than passing the server text through verbatim -- this is
    the case where the PAT could leak via a channel the proxy does not control."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"error": "invalid", "subject_token": PAT})

    with pytest.raises(TokenExchangeError) as exc_info:
        exchange_pat(PAT, make_config(), transport=httpx.MockTransport(handler))

    message = str(exc_info.value)
    # Non-vacuous: proves the (redacted) server body actually reached the
    # diagnostic, rather than the assertion passing because nothing was ever
    # printed at all.
    assert "<redacted>" in message
    assert PAT not in message


# ---------------------------------------------------------------------------
# Exception type
# ---------------------------------------------------------------------------


def test_token_exchange_error_is_an_exception_not_baseexception():
    """``TokenExchangeError`` must subclass ``Exception``, not ``BaseException``.

    The MCP SDK catches ``except Exception`` in both ``_handle_get_stream``
    (to keep the background stream alive across a transient failure) and
    ``terminate_session`` (so teardown cannot crash a clean shutdown). If this
    were a bare ``BaseException`` it would slip past both handlers and turn an
    already-diagnosed background-stream or teardown failure back into an
    unhandled crash -- exactly what ``ProxyFatalError`` exists to prevent.
    """
    assert issubclass(TokenExchangeError, Exception)
    assert issubclass(TokenExchangeError, ProxyFatalError)


# ---------------------------------------------------------------------------
# A 2xx body is the one most likely to hold a credential we did not ask for
# ---------------------------------------------------------------------------


def test_unexpected_2xx_shape_reports_keys_not_values():
    """A successful-but-wrong token response is described, never quoted.

    The `>=400` branches print RFC 6749 error bodies, which is fine. This branch
    is reached with the body of a *successful* token response -- exactly the
    body most likely to carry a credential under another spelling. The module
    guarantees no ``refresh_token`` is stored; printing the body verbatim would
    write one to stderr, and MCP clients capture stderr to disk.
    """
    secrets = {"refresh_token": "RT-SUPER-SECRET", "id_token": "IDT-SECRET", "token_type": "Bearer"}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=secrets)

    with pytest.raises(TokenExchangeError) as excinfo:
        exchange_pat(PAT, make_config(), transport=httpx.MockTransport(handler))

    message = str(excinfo.value)
    for value in secrets.values():
        assert value not in message, f"leaked {value!r}"
    # Non-vacuous: the keys are the diagnostic and must survive.
    assert "refresh_token" in message
    assert "id_token" in message


def test_non_json_2xx_still_reports_the_body():
    """A non-JSON 2xx is usually an HTML login page -- seeing it is the point."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<html>SSO required</html>")

    with pytest.raises(TokenExchangeError) as excinfo:
        exchange_pat(PAT, make_config(), transport=httpx.MockTransport(handler))

    assert "SSO required" in str(excinfo.value)


# ---------------------------------------------------------------------------
# The PAT rides in a request body, so the channel must be encrypted
# ---------------------------------------------------------------------------


def test_refuses_a_non_https_token_endpoint():
    """A cleartext host must never receive the PAT.

    The SDK only prepends ``https://`` when a profile omits the scheme, so a
    profile with an explicit ``http://`` host reaches the exchange untouched --
    and here the credential is in the request *body*, not just a header.
    """
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"access_token": "nope"})

    with pytest.raises(TokenExchangeError) as excinfo:
        exchange_pat(PAT, make_config(host="http://insecure.example.com"), transport=httpx.MockTransport(handler))

    assert "non-HTTPS" in str(excinfo.value)
    assert PAT not in str(excinfo.value)
    assert calls == [], "the request must not be sent at all"


def test_https_host_is_accepted():
    """Non-vacuous counterpart: the guard rejects only cleartext."""
    token = exchange_pat(PAT, make_config(), transport=httpx.MockTransport(ok_handler))

    assert token.access_token == "app-scoped-token"


# ---------------------------------------------------------------------------
# The read happens on the event-loop thread, so it must be bounded
# ---------------------------------------------------------------------------


def test_oversized_response_body_is_bounded(monkeypatch):
    """The reader stops at the cap instead of buffering the whole reply.

    Asserted by shrinking the cap below ``scrub_body``'s own 500-character
    limit. Left at 64 KiB the assertion would be satisfied by that limit alone
    and would pass against a completely unbounded read -- which is exactly what
    an earlier version of this test did.
    """
    from uc_mcp_proxy import token_exchange

    monkeypatch.setattr(token_exchange, "_MAX_RESPONSE_BYTES", 64)
    delivered: list[int] = []

    class _Chunked(httpx.SyncByteStream):
        def __iter__(self):
            for _ in range(1000):
                delivered.append(1)
                yield b"A" * 32

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, stream=_Chunked())

    with pytest.raises(TokenExchangeError):
        exchange_pat(PAT, make_config(), transport=httpx.MockTransport(handler))

    # Counted in chunks, not characters: the cap stops the *read*, and one
    # chunk past it is unavoidable because the check follows the read that
    # produced it. Asserting on the rendered text instead would be satisfied
    # by ``scrub_body``'s own 500-char limit and would pass against a
    # completely unbounded read -- which an earlier version of this test did.
    assert len(delivered) <= 3, f"read {len(delivered)} chunks; the cap did not stop it"


def test_read_stops_at_the_wall_clock_deadline():
    """A slow reply is abandoned even while it stays inside httpx's timeout.

    httpx's timeout is per-operation and resets on every chunk, so a server
    dribbling one byte just inside each window would otherwise hold this
    synchronous call -- and with it the event loop -- open indefinitely.
    """
    from uc_mcp_proxy.token_exchange import _read_bounded

    delivered: list[int] = []

    class _Chunked(httpx.SyncByteStream):
        """A body that arrives in many small pieces, like a dribbling server."""

        def __iter__(self):
            for _ in range(1000):
                delivered.append(1)
                yield b"A" * 10

    response = httpx.Response(400, stream=_Chunked())

    # A deadline already in the past, so it trips as soon as it is consulted.
    body = _read_bounded(response, deadline=0.0)

    # One chunk is unavoidable -- the check runs after the read that produced
    # it. What matters is that the remaining 999 were never pulled.
    assert len(body) == 10, f"deadline ignored, read {len(body)} bytes"
    assert len(delivered) == 1, f"kept reading past the deadline: {len(delivered)} chunks"


def test_hostile_reason_phrase_cannot_rewrite_the_terminal():
    """The status line is server-authored and bypasses the body scrubber."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            400,
            json={"error": "nope"},
            extensions={"reason_phrase": b"Bad Request\x1b[2K\x1b[1AFAKE LINE"},
        )

    with pytest.raises(TokenExchangeError) as excinfo:
        exchange_pat(PAT, make_config(), transport=httpx.MockTransport(handler))

    assert "\x1b" not in str(excinfo.value)


def test_reason_phrase_cannot_carry_the_pat():
    """A server can put the credential it just received into its status line.

    Stripping escapes from the reason phrase is not enough. This needs no
    padding and no positioning: the endpoint echoes the PAT it was sent, and
    without redaction it prints in full on the line directly above a body the
    same function successfully protects.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403,
            json={"error": "see status line"},
            extensions={"reason_phrase": f"Forbidden token={PAT}".encode()},
        )

    with pytest.raises(TokenExchangeError) as excinfo:
        exchange_pat(PAT, make_config(), transport=httpx.MockTransport(handler))

    message = str(excinfo.value)
    assert PAT not in message
    assert "<redacted>" in message


def test_malformed_host_is_diagnosed_not_raised():
    """A host httpx cannot parse must not escape as a traceback.

    This frame holds the PAT in its locals, so an uncaught exception here is
    both a worse message and a disclosure risk under any locals-capturing
    reporter.
    """
    with pytest.raises(TokenExchangeError) as excinfo:
        exchange_pat(PAT, make_config(host="https://ws[bad"), transport=httpx.MockTransport(ok_handler))

    assert PAT not in str(excinfo.value)
    assert "not a usable URL" in str(excinfo.value)


@pytest.mark.parametrize(
    "expires_in",
    ["nan", "inf", "-inf", 0, -1, -3600],
    ids=["nan", "inf", "neg_inf", "zero", "minus_one", "very_negative"],
)
def test_a_degenerate_lifetime_falls_back_to_the_default(expires_in):
    """A lifetime the endpoint made up must not break the cache in either direction.

    ``nan``/``inf`` make the cached token never expire; zero or negative make it
    expire on arrival, and since the exchange is synchronous on the event-loop
    thread that turns one blocking round trip per hour into one per request.
    """
    import math

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"access_token": "t", "expires_in": expires_in})

    token = exchange_pat(PAT, make_config(), now=lambda: 1000.0, transport=httpx.MockTransport(handler))

    assert math.isfinite(token.expires_at)
    assert token.expires_at == 1000.0 + 3600 - 60


def test_a_plausible_short_lifetime_is_still_honoured():
    """The clamp must catch only the degenerate values, not a genuinely short token."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"access_token": "t", "expires_in": 300})

    token = exchange_pat(PAT, make_config(), now=lambda: 1000.0, transport=httpx.MockTransport(handler))

    assert token.expires_at == 1000.0 + 300 - 60


def test_the_exchange_client_never_follows_a_redirect():
    """A 307 would re-send the PAT -- which is in the request body -- elsewhere.

    Cross-origin, httpx strips ``Authorization`` but the body travels intact, so
    following one would hand the credential to whatever host the redirect names.
    """
    hosts: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        hosts.append(request.url.host)
        return httpx.Response(307, headers={"location": "https://evil.example.com/steal"})

    with pytest.raises(TokenExchangeError):
        exchange_pat(PAT, make_config(), transport=httpx.MockTransport(handler))

    assert hosts == ["test-workspace.cloud.databricks.com"], f"the PAT was re-sent to {hosts}"


def test_a_huge_integer_lifetime_does_not_escape_as_a_traceback():
    """``float()`` raises ``OverflowError`` on a big int, which is neither a
    ``TypeError`` nor a ``ValueError``.

    JSON puts no bound on an integer literal. Letting this escape would unwind
    out of a frame whose locals include the PAT and ``form["subject_token"]``,
    producing exactly the traceback this module exists to replace -- and
    bypassing ``report_fatal``, so the backstop would re-raise it.
    """
    import json as _json

    def handler(request: httpx.Request) -> httpx.Response:
        body = _json.dumps({"access_token": "t", "expires_in": 10**400}).encode()
        return httpx.Response(200, content=body)

    token = exchange_pat(PAT, make_config(), now=lambda: 1000.0, transport=httpx.MockTransport(handler))

    assert token.expires_at == 1000.0 + 3600 - 60


def test_an_absurdly_long_lifetime_is_clamped():
    """A ten-year token is a malfunction; believing it pins a stale credential."""
    assert _expires_in_for(315_360_000) == 3600.0
    assert _expires_in_for(86_400) == 86_400.0  # a full day is still plausible


def _expires_in_for(value: object) -> float:
    from uc_mcp_proxy.token_exchange import _expires_in

    return _expires_in({"expires_in": value})


def test_a_compressed_reply_is_refused_rather_than_decoded():
    """``Accept-Encoding: identity`` is a request, not a control.

    httpx decodes on the *response's* ``Content-Encoding`` regardless of what
    was asked, and the whole body can arrive as one chunk -- so it expands past
    the byte cap before any counter in the read loop gets to look at it. 194 KiB
    on the wire measured 200 MB decoded, on the event-loop thread.
    """
    import gzip

    from uc_mcp_proxy.token_exchange import _read_bounded

    payload = gzip.compress(b"A" * 50_000_000)

    class _OneChunk(httpx.SyncByteStream):
        def __iter__(self):
            yield payload

    response = httpx.Response(400, headers={"content-encoding": "gzip"}, stream=_OneChunk())

    body = _read_bounded(response, deadline=1e12)

    assert len(body) < 1000, f"decoded {len(body)} bytes despite the refusal"
    assert "Content-Encoding" in body


def test_a_password_in_the_profile_host_is_not_printed():
    """The SDK preserves userinfo when normalizing the host.

    A ``https://user:password@host`` profile entry would otherwise put that
    password on stderr, which MCP clients capture to disk.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"error": "no"})

    with pytest.raises(TokenExchangeError) as excinfo:
        exchange_pat(
            PAT,
            make_config(host="https://someone:hunter2@test-workspace.cloud.databricks.com"),
            transport=httpx.MockTransport(handler),
        )

    message = str(excinfo.value)
    assert "hunter2" not in message
    assert "someone" not in message
    # Non-vacuous: the endpoint is still reported, just without the credentials.
    assert "test-workspace.cloud.databricks.com/oidc/v1/token" in message
