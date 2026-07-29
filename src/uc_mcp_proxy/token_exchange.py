"""RFC 8693 exchange of a Databricks PAT for an app-scoped OAuth token.

Databricks Apps do not accept a personal access token. They accept an OAuth
token minted *for that app*, which RFC 8693 calls a token exchange: the PAT is
the ``subject_token``, the app's client id is the ``audience``, and the
workspace's ``/oidc/v1/token`` endpoint hands back a short-lived access token
scoped to the app.

Kept deliberately free of the proxy so it can be tested with nothing but an
``httpx.MockTransport``. Three constraints on this module are not obvious and
are each load-bearing:

* The client is built with **no** ``auth=``. The proxy's own client is
  constructed with ``auth=DatabricksAuth(...)``, so routing this request
  through it would re-enter the auth flow and recurse without bound.
* The call is **synchronous**, made from the event-loop thread. That is what
  makes the read-check-exchange-store sequence in ``DatabricksAuth`` atomic
  without a lock. See ``_token``'s docstring for why a lock would be worse.
* The timeout is **its own** value and must not be copied from the proxy's
  client, whose read timeout is 300s. This call blocks the loop, so that value
  would freeze the stdio bridge -- and the abort path with it -- for five
  minutes.

The response carries no ``refresh_token``, so none is read or stored; the
credential is re-derived from the PAT when it expires.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import urljoin

import httpx

from uc_mcp_proxy.errors import ProxyFatalError, scrub_body

_TOKEN_PATH = "oidc/v1/token"
_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:token-exchange"
_SUBJECT_TOKEN_TYPE = "urn:databricks:params:oauth:token-type:personal-access-token"
#: The only ``requested_token_type`` the platform accepts. Verified against a
#: live workspace: every other value in the RFC's table is refused, and the
#: refusal is reported as an *audience* error rather than a token-type one --
#: which is why ``_format_failure`` says so explicitly.
_REQUESTED_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:access_token"

#: Renew this far before the server's stated expiry, so a token cannot go stale
#: in flight. The retry is what makes expiry survivable; this only avoids the
#: round trip in the common case.
_EXPIRY_MARGIN_SECONDS = 60.0
_DEFAULT_EXPIRES_IN = 3600


class TokenExchangeError(ProxyFatalError):
    """The workspace refused to exchange the PAT for an app token."""


@dataclass(frozen=True)
class ExchangeConfig:
    """Everything the exchange needs, resolved once at startup."""

    #: The *workspace* host, from ``client.config.host``. Never the app host:
    #: the app has no token endpoint, and sending the PAT there is the exact
    #: exposure this feature exists to remove.
    host: str
    #: The app's ``oauth2_app_client_id``, sent as the ``audience``.
    client_id: str
    scopes: tuple[str, ...]
    verify_ssl: bool
    #: Must NOT be copied from ``_build_http_client``'s ``read=300.0``. This
    #: call is synchronous and runs on the event loop, so the timeout is a hard
    #: ceiling on how long the whole proxy stops responding. At 10s the worst
    #: case is a ten-second freeze of the stdio bridge mid-tool-call, which MCP
    #: clients (30-60s timeouts) survive; at 300s it would look like a hang.
    timeout: float = 10.0


@dataclass(frozen=True)
class ExchangedToken:
    """An app-scoped access token and the monotonic deadline to renew it."""

    access_token: str
    #: Monotonic, with ``_EXPIRY_MARGIN_SECONDS`` already subtracted. Monotonic
    #: rather than wall-clock so a clock adjustment cannot make a live token
    #: look expired, or an expired one look live.
    expires_at: float

    def expired(self, now: float) -> bool:
        return now >= self.expires_at


def exchange_pat(
    pat: str,
    config: ExchangeConfig,
    *,
    now: Callable[[], float] = time.monotonic,
    transport: httpx.BaseTransport | None = None,
) -> ExchangedToken:
    """Exchange ``pat`` for a token scoped to ``config.client_id``.

    Raises ``TokenExchangeError`` -- whose message names the exact request that
    was sent, so a platform-side change is diagnosable rather than mysterious --
    on any non-2xx, transport error, unparseable body, or missing token.

    ``now`` and ``transport`` are test seams; ``transport`` mirrors the one in
    ``_build_http_client``, and httpx ignores ``verify`` when it is supplied.
    """
    endpoint = _token_endpoint(config.host)
    form = {
        "grant_type": _GRANT_TYPE,
        "subject_token": pat,
        "subject_token_type": _SUBJECT_TOKEN_TYPE,
        "requested_token_type": _REQUESTED_TOKEN_TYPE,
        "audience": config.client_id,
    }
    # Omitted entirely rather than sent empty: an app with no
    # ``effective_user_api_scopes`` has nothing to ask for, and ``scope=``
    # is not the same request as no scope at all.
    if config.scopes:
        form["scope"] = " ".join(config.scopes)

    # Sampled before the request, not after, so network latency counts against
    # the token's life rather than being silently added to it.
    issued_at = now()

    try:
        with httpx.Client(
            verify=config.verify_ssl,
            timeout=config.timeout,
            transport=transport,
        ) as client:
            response = client.post(endpoint, data=form, headers={"Authorization": f"Bearer {pat}"})
    except httpx.RequestError as exc:
        raise TokenExchangeError(
            _format_failure(
                config,
                endpoint,
                pat,
                headline="could not reach the workspace token endpoint.",
                detail=str(exc),
            )
        ) from exc

    if response.status_code >= 400:
        raise TokenExchangeError(
            _format_failure(
                config,
                endpoint,
                pat,
                headline=(
                    f"the workspace refused the PAT token exchange "
                    f"(HTTP {response.status_code} {response.reason_phrase or ''})."
                ),
                detail=response.text,
            )
        )

    try:
        payload = response.json()
    except ValueError as exc:
        raise TokenExchangeError(
            _format_failure(
                config,
                endpoint,
                pat,
                headline="the workspace token endpoint returned a body that is not JSON.",
                detail=response.text,
            )
        ) from exc

    access_token = payload.get("access_token") if isinstance(payload, dict) else None
    if not isinstance(access_token, str) or not access_token:
        raise TokenExchangeError(
            _format_failure(
                config,
                endpoint,
                pat,
                headline="the workspace token endpoint returned no access_token.",
                detail=response.text,
            )
        )

    return ExchangedToken(
        access_token=access_token,
        expires_at=issued_at + max(_expires_in(payload) - _EXPIRY_MARGIN_SECONDS, 0.0),
    )


def _token_endpoint(host: str) -> str:
    """The workspace's OIDC token endpoint, however ``host`` was written."""
    return urljoin(host.rstrip("/") + "/", _TOKEN_PATH)


def _expires_in(payload: dict[str, object]) -> float:
    """The token's stated lifetime in seconds, defaulting when absent or odd."""
    try:
        return float(payload.get("expires_in", _DEFAULT_EXPIRES_IN))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return float(_DEFAULT_EXPIRES_IN)


def _format_failure(
    config: ExchangeConfig,
    endpoint: str,
    pat: str,
    *,
    headline: str,
    detail: str,
) -> str:
    """Describe the request that was refused, without disclosing the PAT.

    Names every parameter that was sent *except* ``subject_token``, because the
    accepted values are undocumented and a platform-side change is otherwise
    indistinguishable from a user's typo. The server's own text is scrubbed
    with the PAT as a secret: this request carries the credential in the body
    as well as the header, so an endpoint that echoes the offending field back
    would otherwise print it.
    """
    scope = " ".join(config.scopes) if config.scopes else "(omitted -- no --scope given)"
    lines = [
        f"uc-mcp-proxy: {headline}",
        f"  endpoint:             {endpoint}",
        f"  grant_type:           {_GRANT_TYPE}",
        f"  subject_token_type:   {_SUBJECT_TOKEN_TYPE}",
        f"  requested_token_type: {_REQUESTED_TOKEN_TYPE}",
        f"  audience:             {config.client_id}",
        f"  scope:                {scope}",
    ]
    scrubbed = scrub_body(detail, [pat])
    if scrubbed:
        lines.append(f"  server:               {scrubbed}")
    lines.append(
        "Check that --client-id is the app's `oauth2_app_client_id` and that --scope matches "
        "`effective_user_api_scopes` from `databricks apps get <app-name> -o json`."
    )
    # The platform reports a rejected requested_token_type as an audience
    # error, so without this the user re-checks a --client-id that was correct.
    lines.append('Note: an "invalid audience" error can also mean requested_token_type was rejected.')
    lines.append("Exiting.")
    return "\n".join(lines)
