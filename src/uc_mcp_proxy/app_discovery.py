"""Detect a Databricks App target and discover its token-exchange parameters.

A Databricks App is served from its own ``*.databricksapps.com`` hostname and
authenticates with an OAuth token minted *for that app* (see
``token_exchange``). Everything a caller needs to mint one — the app's
``oauth2_app_client_id`` and its ``effective_user_api_scopes`` — is readable
from workspace metadata by any identity that can see the app, so the proxy can
resolve them itself rather than making the user copy them onto the command line.

Kept free of the proxy's HTTP machinery: the only dependency is the already
constructed ``WorkspaceClient``, so this module is exercised with a fake client
in unit tests.

Two facts this module encodes, both established empirically (see
``docs/token-exchange.md``):

* The exchange accepts a ``pat``-typed credential **only** when that credential
  is a Lakebox-generated one; a classic, hand-minted ``dapi…`` token is refused
  as a subject token. ``looks_like_classic_pat`` is the cheap pre-flight signal
  that lets the proxy refuse before sending anything, rather than after a round
  trip that also forwards the credential.
* ``scope`` is required and non-empty for a real App, even though the exchange
  request treats it as optional — so discovering the app's scopes is
  load-bearing, not a convenience.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

#: Databricks serves every App from a subdomain of this host. Managed and
#: External MCP servers live under the workspace host instead, so this suffix is
#: what distinguishes "an App, which needs a token exchange" from "a workspace
#: endpoint, which takes the credential as-is".
_APP_HOST_SUFFIX = ".databricksapps.com"

#: Every classic Databricks personal access token begins with this. A
#: Lakebox-generated credential does not (36 chars, mixed alphanumeric, no
#: prefix), and that is the only observable difference between the two at the
#: point the proxy has to decide whether an exchange can possibly succeed.
_CLASSIC_PAT_PREFIX = "dapi"


class AppDiscoveryError(Exception):
    """Listing Databricks Apps failed while resolving an app's client id.

    Raised only for a failure of the lookup itself (no permission, transport
    error). "No app matched the URL" is not an error here — it returns ``None``
    so the caller can phrase a more specific diagnosis.
    """


@dataclass(frozen=True)
class DiscoveredApp:
    """The token-exchange parameters resolved from an app's workspace metadata."""

    name: str
    client_id: str
    scopes: tuple[str, ...]


def is_app_host(url: str) -> bool:
    """True if ``url``'s host is a Databricks App (``*.databricksapps.com``)."""
    host = urlsplit(url).hostname or ""
    return host.endswith(_APP_HOST_SUFFIX)


def looks_like_classic_pat(token: str) -> bool:
    """True if ``token`` is a classic ``dapi…`` PAT the exchange will refuse.

    Deliberately conservative: it only ever returns ``True`` for the one shape
    that is *known* to be rejected, so a false positive cannot block a
    credential that would actually have worked. An empty token is not classic —
    the "no PAT to exchange" path handles that with a clearer message.
    """
    return token.startswith(_CLASSIC_PAT_PREFIX)


def discover_app(client: WorkspaceClient, url: str) -> DiscoveredApp | None:
    """Resolve the App at ``url`` to its client id and scopes, or ``None``.

    Matches on the app's registered ``url`` host rather than parsing the
    hostname, because the app name is followed by the workspace id and a naive
    split is ambiguous. Returns ``None`` when no visible app has a matching URL
    or the matched app carries no client id; raises ``AppDiscoveryError`` if the
    listing itself fails.
    """
    target = urlsplit(url).hostname
    if not target:
        return None

    try:
        apps = list(client.apps.list())
    except Exception as exc:  # noqa: BLE001 - re-raised as a typed error for the caller to diagnose
        raise AppDiscoveryError(str(exc)) from exc

    for app in apps:
        if urlsplit(app.url or "").hostname != target:
            continue
        client_id = app.oauth2_app_client_id
        if not client_id:
            return None
        scopes = tuple(app.effective_user_api_scopes or ())
        if not scopes:
            # ``apps.list`` may omit ``effective_user_api_scopes`` that
            # ``apps.get`` carries. Best-effort: a lookup failure here leaves
            # scopes empty, which the caller warns about rather than aborting on.
            try:
                detail = client.apps.get(app.name)
            except Exception:  # noqa: BLE001 - scopes are best-effort; the caller warns on empty
                detail = None
            if detail is not None:
                scopes = tuple(detail.effective_user_api_scopes or ())
        return DiscoveredApp(name=app.name or "", client_id=client_id, scopes=scopes)

    return None
