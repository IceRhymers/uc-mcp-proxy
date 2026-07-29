"""Tests for the PAT-to-app-token exchange integration in ``DatabricksAuth``.

``token_exchange.exchange_pat`` itself is covered by ``test_token_exchange.py``.
This file drives ``DatabricksAuth.sync_auth_flow`` end to end -- the same way
``test_auth.py`` does -- with ``httpx.MockTransport`` standing in for the
workspace's ``/oidc/v1/token`` endpoint, so it exercises the actual caching,
re-entrancy, and 401-retry wiring in ``__main__.py`` rather than re-testing
the exchange request shape.
"""

from __future__ import annotations

import httpx
import pytest

from tests.conftest import FAKE_PAT
from uc_mcp_proxy import DatabricksAuth
from uc_mcp_proxy.errors import remediation, retry_armed
from uc_mcp_proxy.token_exchange import ExchangedToken, TokenExchangeError

pytestmark = [pytest.mark.unit, pytest.mark.anyio]


def _counting_exchange_handler(tokens: list[str] | None = None):
    """A ``MockTransport`` handler that mints tokens and records every call.

    With ``tokens`` given, the Nth call returns the Nth token (the last one
    repeats past the end of the list); with no ``tokens``, calls get
    ``exchanged-token-1``, ``exchanged-token-2``, etc. Returns ``(handler,
    calls)`` where ``calls`` grows by one ``httpx.Request`` per invocation, so
    ``len(calls)`` is the exchange count.
    """
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        token = tokens[min(len(calls) - 1, len(tokens) - 1)] if tokens else f"exchanged-token-{len(calls)}"
        return httpx.Response(200, json={"access_token": token, "expires_in": 3600})

    return handler, calls


def _failing_exchange_handler(status: int = 400):
    """A ``MockTransport`` handler that always refuses the exchange."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json={"error": "invalid_client"})

    return handler


# ---------------------------------------------------------------------------
# Exchange disabled / PAT-without-exchange baseline
# ---------------------------------------------------------------------------


def test_exchange_disabled_path_is_unchanged(mock_workspace_client):
    """``exchange=None`` on an OAuth profile: unchanged legacy behaviour --
    the OAuth bearer goes out as-is, plus ``X-Forwarded-Access-Token``. No
    exchange request must be issued."""
    handler, calls = _counting_exchange_handler()
    auth = DatabricksAuth(mock_workspace_client, transport=httpx.MockTransport(handler))
    request = httpx.Request("POST", "https://example.com/mcp")
    flow = auth.sync_auth_flow(request)

    authed = next(flow)

    assert authed.headers["Authorization"] == "Bearer test-oauth-token"
    assert authed.headers["X-Forwarded-Access-Token"] == "test-oauth-token"
    assert calls == []


def test_pat_without_client_id_omits_forwarded_header(mock_workspace_client_pat):
    """``exchange=None`` on a **PAT** profile: the ``Authorization`` header
    still carries the PAT (legacy behaviour), but ``X-Forwarded-Access-Token``
    must be absent.

    This is the widest-blast-radius branch of the forwarded-header rule --
    it changes the outgoing headers for every existing PAT-profile user, not
    just those who opt into ``--client-id`` -- and until this test it had zero
    positive coverage: nothing asserted the header was actually missing for a
    real PAT profile, only that the OAuth path still set it.
    """
    auth = DatabricksAuth(mock_workspace_client_pat)
    request = httpx.Request("POST", "https://example.com/mcp")
    flow = auth.sync_auth_flow(request)

    authed = next(flow)

    assert authed.headers["Authorization"] == f"Bearer {FAKE_PAT}"
    assert "X-Forwarded-Access-Token" not in authed.headers


# ---------------------------------------------------------------------------
# The PAT must never reach the wire once exchange is active
# ---------------------------------------------------------------------------


def test_first_request_sends_exchanged_token_not_the_pat(mock_workspace_client_pat, exchange_config):
    """With exchange active, the first request carries the exchanged token,
    never the raw PAT."""
    handler, calls = _counting_exchange_handler(["app-scoped-token"])
    auth = DatabricksAuth(
        mock_workspace_client_pat,
        exchange=exchange_config,
        transport=httpx.MockTransport(handler),
    )
    request = httpx.Request("POST", "https://example.com/mcp")
    flow = auth.sync_auth_flow(request)

    authed = next(flow)

    assert authed.headers["Authorization"] == "Bearer app-scoped-token"
    assert FAKE_PAT not in authed.headers["Authorization"]
    assert len(calls) == 1


def test_pat_never_appears_in_any_header_value(mock_workspace_client_pat, exchange_config):
    """Stronger than checking one named header: the PAT must not appear in
    ANY header value on the outgoing request."""
    handler, _calls = _counting_exchange_handler(["app-scoped-token"])
    auth = DatabricksAuth(
        mock_workspace_client_pat,
        exchange=exchange_config,
        transport=httpx.MockTransport(handler),
    )
    request = httpx.Request("POST", "https://example.com/mcp")
    flow = auth.sync_auth_flow(request)

    authed = next(flow)

    for _key, value in authed.headers.items():
        assert FAKE_PAT not in value


def test_failed_exchange_never_writes_the_pat_into_headers(mock_workspace_client_pat, exchange_config):
    """Security: when the exchange itself fails, ``TokenExchangeError`` must
    propagate out of ``_apply_headers`` AND the request's headers must never
    have been touched with the PAT.

    ``_apply_headers`` writes nothing until ``_token`` has already succeeded.
    An update-then-overwrite ordering (write the auth headers first, then
    overwrite ``Authorization`` with the exchanged token) would instead have
    left the raw PAT sitting on the request in the exact window where the
    exchange raises -- and that window is the commonest failure mode of all
    (a wrong ``--client-id``), dispatching the real credential to the app
    host it was never supposed to reach.
    """
    auth = DatabricksAuth(
        mock_workspace_client_pat,
        exchange=exchange_config,
        transport=httpx.MockTransport(_failing_exchange_handler()),
    )
    request = httpx.Request("POST", "https://example.com/mcp")

    with pytest.raises(TokenExchangeError):
        auth._apply_headers(request)

    for _key, value in request.headers.items():
        assert FAKE_PAT not in value
    assert "Authorization" not in request.headers


def test_exchange_failure_calls_on_fatal_then_raises(mock_workspace_client_pat, exchange_config):
    """``on_fatal`` receives the diagnosis, and ``TokenExchangeError`` is
    still raised afterwards -- ``_fail`` must never return."""
    fatal_messages: list[str] = []
    auth = DatabricksAuth(
        mock_workspace_client_pat,
        exchange=exchange_config,
        on_fatal=fatal_messages.append,
        transport=httpx.MockTransport(_failing_exchange_handler()),
    )
    request = httpx.Request("POST", "https://example.com/mcp")

    with pytest.raises(TokenExchangeError):
        auth._apply_headers(request)

    assert len(fatal_messages) == 1
    assert "workspace refused the PAT token exchange" in fatal_messages[0]


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------


def test_cache_hit_makes_no_second_request(mock_workspace_client_pat, exchange_config):
    """Ten requests through one ``DatabricksAuth`` instance must reuse the
    cached token: exactly one exchange for the whole run."""
    handler, calls = _counting_exchange_handler(["app-scoped-token"])
    auth = DatabricksAuth(
        mock_workspace_client_pat,
        exchange=exchange_config,
        transport=httpx.MockTransport(handler),
    )

    for _ in range(10):
        request = httpx.Request("POST", "https://example.com/mcp")
        flow = auth.sync_auth_flow(request)
        authed = next(flow)
        assert authed.headers["Authorization"] == "Bearer app-scoped-token"
        with pytest.raises(StopIteration):
            flow.send(httpx.Response(200))

    assert len(calls) == 1


def test_cache_miss_after_expiry_re_exchanges(mock_workspace_client_pat, exchange_config):
    """A cached token that has already expired triggers a fresh exchange on
    the ordinary (non-retry) path."""
    handler, calls = _counting_exchange_handler(["token-2"])
    auth = DatabricksAuth(
        mock_workspace_client_pat,
        exchange=exchange_config,
        transport=httpx.MockTransport(handler),
    )
    auth._cached = ExchangedToken(access_token="token-1", expires_at=-1e12)

    token = auth._token(FAKE_PAT, exchange_config)

    assert token == "token-2"
    assert len(calls) == 1


def test_stale_branch_ignores_expiry(mock_workspace_client_pat, exchange_config):
    """On the collapse-concurrent-401s branch (``stale`` given and the cache
    already holds something different), expiry must NOT be consulted --
    re-checking it here is exactly how a dedupe silently stops deduping, and
    every concurrent 401 would end up paying for its own exchange again."""
    handler, calls = _counting_exchange_handler()
    auth = DatabricksAuth(
        mock_workspace_client_pat,
        exchange=exchange_config,
        transport=httpx.MockTransport(handler),
    )
    auth._cached = ExchangedToken(access_token="fresher-token", expires_at=-1e12)

    token = auth._token(FAKE_PAT, exchange_config, stale="some-other-token")

    assert token == "fresher-token"
    assert calls == []


def test_concurrent_401s_collapse_to_one_exchange(mock_workspace_client_pat, exchange_config):
    """Two requests dispatched under the same cached token both come back
    401. The first one to retry re-mints the credential; the second retry
    must reuse that freshly minted token rather than minting a third one of
    its own, or every concurrent 401 becomes its own exchange -- the
    thundering herd ``_token``'s docstring warns about.

    Expected total: exactly 2 exchanges for the whole scenario -- one for the
    shared initial token (request B is a cache hit), one for request A's
    retry -- with request B's retry adding zero.
    """
    handler, calls = _counting_exchange_handler(["token-1", "token-2"])
    auth = DatabricksAuth(
        mock_workspace_client_pat,
        exchange=exchange_config,
        transport=httpx.MockTransport(handler),
    )

    request_a = httpx.Request("POST", "https://example.com/mcp")
    flow_a = auth.sync_auth_flow(request_a)
    authed_a = next(flow_a)
    assert authed_a.headers["Authorization"] == "Bearer token-1"

    request_b = httpx.Request("POST", "https://example.com/mcp")
    flow_b = auth.sync_auth_flow(request_b)
    authed_b = next(flow_b)
    assert authed_b.headers["Authorization"] == "Bearer token-1"
    assert len(calls) == 1  # B was a cache hit, not a second exchange.

    # A's request 401s first and drives its retry: this is the one that
    # actually re-mints.
    retried_a = flow_a.send(httpx.Response(401))
    assert retried_a.headers["Authorization"] == "Bearer token-2"
    assert len(calls) == 2
    with pytest.raises(StopIteration):
        flow_a.send(httpx.Response(200))

    # B's request also carried the now-superseded token-1 and 401s too. Its
    # retry must reuse token-2, not mint a third token.
    retried_b = flow_b.send(httpx.Response(401))
    assert retried_b.headers["Authorization"] == "Bearer token-2"
    assert len(calls) == 2
    with pytest.raises(StopIteration):
        flow_b.send(httpx.Response(200))


# ---------------------------------------------------------------------------
# 401 retry behaviour
# ---------------------------------------------------------------------------


def test_401_re_exchanges_and_yields_exactly_once_more(mock_workspace_client_pat, exchange_config):
    """A 401 with exchange active gets exactly one retry, carrying a
    different (freshly minted) token, and then the flow terminates."""
    handler, _calls = _counting_exchange_handler(["token-1", "token-2"])
    auth = DatabricksAuth(
        mock_workspace_client_pat,
        exchange=exchange_config,
        transport=httpx.MockTransport(handler),
    )
    request = httpx.Request("POST", "https://example.com/mcp")
    flow = auth.sync_auth_flow(request)

    first = next(flow)
    first_auth = first.headers["Authorization"]
    assert first_auth == "Bearer token-1"

    second = flow.send(httpx.Response(401))
    # ``first`` and ``second`` are the SAME request object, mutated in place
    # by the retry -- so this must compare against the value captured before
    # the retry, not against ``first.headers[...]`` read now.
    assert second.headers["Authorization"] == "Bearer token-2"
    assert second.headers["Authorization"] != first_auth

    with pytest.raises(StopIteration):
        flow.send(httpx.Response(200))


def test_401_twice_does_not_yield_a_third_time(mock_workspace_client_pat, exchange_config):
    """A second 401 (on the retry itself) is permanent by definition -- the
    flow must terminate rather than yield a third request."""
    handler, _calls = _counting_exchange_handler(["token-1", "token-2"])
    auth = DatabricksAuth(
        mock_workspace_client_pat,
        exchange=exchange_config,
        transport=httpx.MockTransport(handler),
    )
    request = httpx.Request("POST", "https://example.com/mcp")
    flow = auth.sync_auth_flow(request)

    next(flow)
    flow.send(httpx.Response(401))
    with pytest.raises(StopIteration):
        flow.send(httpx.Response(401))


def test_401_without_exchange_does_not_retry(mock_workspace_client):
    """A 401 from a Managed/External MCP URL (no ``--client-id``, so
    ``exchange`` is ``None``) must not cost a pointless second request: there
    is no credential the proxy can refresh on that path, so retrying would
    only double the latency of a failure that is going to be reported either
    way."""
    auth = DatabricksAuth(mock_workspace_client)
    request = httpx.Request("POST", "https://example.com/mcp")
    flow = auth.sync_auth_flow(request)

    next(flow)
    with pytest.raises(StopIteration):
        flow.send(httpx.Response(401))


def test_shutting_down_skips_arming_and_retry(mock_workspace_client_pat, exchange_config):
    """With ``shutting_down`` reporting ``True``, the first request is not
    armed for retry, and a 401 produces no retry at all -- symmetric with
    ``HttpErrorReporter``'s own shutting-down guard, so a credential refresh
    during teardown cannot make a clean shutdown look like a failure."""
    handler, _calls = _counting_exchange_handler(["token-1"])
    auth = DatabricksAuth(
        mock_workspace_client_pat,
        exchange=exchange_config,
        shutting_down=lambda: True,
        transport=httpx.MockTransport(handler),
    )
    request = httpx.Request("POST", "https://example.com/mcp")
    flow = auth.sync_auth_flow(request)

    next(flow)
    assert retry_armed(request) is False

    with pytest.raises(StopIteration):
        flow.send(httpx.Response(401))


# ---------------------------------------------------------------------------
# Retry-armed extension bookkeeping
# ---------------------------------------------------------------------------


def test_arm_and_disarm_marks_the_request(mock_workspace_client_pat, exchange_config):
    """``retry_armed`` is ``True`` on the first attempt (a 401 will be
    retried) and ``False`` on the retry itself (a second 401 is permanent).
    ``remediation`` carries proxy-authored guidance naming the two flags a
    misconfigured exchange most commonly gets wrong."""
    handler, _calls = _counting_exchange_handler(["token-1", "token-2"])
    auth = DatabricksAuth(
        mock_workspace_client_pat,
        exchange=exchange_config,
        transport=httpx.MockTransport(handler),
    )
    request = httpx.Request("POST", "https://example.com/mcp")
    flow = auth.sync_auth_flow(request)

    next(flow)
    assert retry_armed(request) is True
    text = remediation(request)
    assert text
    assert "--client-id" in text
    assert "--scope" in text

    flow.send(httpx.Response(401))
    assert retry_armed(request) is False


# ---------------------------------------------------------------------------
# Re-entrancy tripwire
# ---------------------------------------------------------------------------


def test_reentrancy_assertion_raises_and_resets(mock_workspace_client_pat, exchange_config):
    """Re-entering ``_token`` while an exchange is already in flight raises
    immediately (naming the re-entrancy) rather than dispatching a second,
    concurrent exchange. Separately: a FAILED real exchange must still reset
    ``_exchanging`` to ``False`` in its ``finally``, so a failure does not
    brick auth for the rest of the process."""
    handler, calls = _counting_exchange_handler(["token-1"])
    auth = DatabricksAuth(
        mock_workspace_client_pat,
        exchange=exchange_config,
        transport=httpx.MockTransport(handler),
    )

    auth._exchanging = True
    with pytest.raises(TokenExchangeError, match="re-entered|await"):
        auth._token(FAKE_PAT, exchange_config)
    assert auth._exchanging is True  # untouched -- the tripwire itself never resets it
    assert calls == []

    auth._exchanging = False
    auth._transport = httpx.MockTransport(_failing_exchange_handler())
    with pytest.raises(TokenExchangeError):
        auth._token(FAKE_PAT, exchange_config)

    assert auth._exchanging is False


# ---------------------------------------------------------------------------
# No Authorization header from authenticate()
# ---------------------------------------------------------------------------


def test_no_authorization_from_authenticate_fails_cleanly(mock_workspace_client_pat, exchange_config):
    """When ``authenticate()`` returns no ``Authorization`` header at all,
    ``_apply_headers`` must fail cleanly, naming the profile, and must NOT
    send an exchange request with ``subject_token=""`` -- that would just
    trade one opaque 400 from the token endpoint for another, less legible
    one."""
    mock_workspace_client_pat.config.authenticate.return_value = {}
    handler, calls = _counting_exchange_handler()
    auth = DatabricksAuth(
        mock_workspace_client_pat,
        exchange=exchange_config,
        transport=httpx.MockTransport(handler),
    )
    request = httpx.Request("POST", "https://example.com/mcp")

    with pytest.raises(TokenExchangeError) as exc_info:
        auth._apply_headers(request)

    assert "test-profile" in str(exc_info.value)
    assert calls == []


# ---------------------------------------------------------------------------
# authenticate() call count
# ---------------------------------------------------------------------------


def test_authenticate_called_once_per_request(mock_workspace_client_pat, exchange_config):
    """Guards a double-call regression: one request through the flow must
    call ``authenticate()`` exactly once, not once per internal helper."""
    handler, _calls = _counting_exchange_handler(["token-1"])
    auth = DatabricksAuth(
        mock_workspace_client_pat,
        exchange=exchange_config,
        transport=httpx.MockTransport(handler),
    )
    request = httpx.Request("POST", "https://example.com/mcp")
    flow = auth.sync_auth_flow(request)

    next(flow)
    with pytest.raises(StopIteration):
        flow.send(httpx.Response(200))

    assert mock_workspace_client_pat.config.authenticate.call_count == 1
