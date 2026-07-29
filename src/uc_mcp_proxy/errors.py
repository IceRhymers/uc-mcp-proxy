"""Diagnosis and reporting of HTTP errors from the remote MCP server.

The proxy owns the ``httpx.AsyncClient`` it hands to the MCP SDK, so that
client's event hooks are the only place that sees every response on both
transport paths. That matters because the two paths fail in opposite ways:
the SDK's GET SSE loop swallows failures into ``logger.debug`` and reconnects,
so no exception ever escapes it, while the POST path raises from inside a
``tg.start_soon`` task, which surfaces as a traceback rather than a diagnosis.

Neither hook may raise. httpx re-raises whatever a response hook throws
directly into the SDK task that made the request -- which is precisely the
traceback path this module exists to remove. Cancellation is the deliberate
exception: it must propagate, so the guards catch ``Exception``, never
``BaseException``.

Failures are classified by *session role*, not by HTTP verb. The proxy sets
``follow_redirects=True``, and httpx rewrites POST to GET on 301/302/303, so
by the time a response hook runs the verb may no longer describe what the
request was for. ``stamp_role`` records the role on the way out instead.
"""

from __future__ import annotations

import contextlib
import functools
import re
import sys
import unicodedata
from collections.abc import Sequence

import anyio
import httpx


class ProxyFatalError(Exception):
    """A proxy-side failure that has already been diagnosed to stderr.

    Raised only after the diagnosis has been emitted, so the backstop in
    ``__main__`` may swallow it: re-raising would print a traceback whose only
    new information is a line number.

    Derives from ``Exception``, never ``BaseException``, and that choice is
    load-bearing in two places inside the MCP SDK. ``_handle_get_stream``
    catches ``except Exception`` and reconnects, so a failure on the background
    stream stays non-fatal; ``terminate_session`` catches ``except Exception``
    so a failure during teardown cannot turn a clean shutdown into a crash. A
    ``BaseException`` here would slip past both and invert the request-vs-stream
    fatality rule this module exists to encode.
    """


_BODY_SNIPPET_LIMIT = 500
#: Reason phrases are a handful of words; h11 would allow 16 KiB of them.
_REASON_LIMIT = 80
_BODY_READ_TIMEOUT = 5.0
_ROLE_KEY = "uc_mcp_role"
#: The origin of the first hop, so a redirect that leaves it can be detected.
_ORIGIN_KEY = "uc_mcp_origin"

#: Set on a request whose 401 the auth layer will retry. Mechanical on purpose:
#: this module never learns *why* a retry is pending, only that one is, so the
#: same machinery serves any future credential-refresh path.
_RETRY_KEY = "uc_mcp_retry_armed"
#: Proxy-authored remediation text for a 401/403 on this request. Authored here
#: in the proxy, never by the server, so it needs no scrubbing.
_REMEDIATION_KEY = "uc_mcp_remediation"

#: JSON-RPC requests and notifications. The channel the user's tool calls ride
#: on -- a refusal here is fatal.
ROLE_REQUEST = "request"
#: The background server-to-client notification stream. The SDK reconnects a
#: bounded number of times, so a refusal here is reported but not fatal.
ROLE_STREAM = "stream"
#: Session termination on close. Never reported: the user did not ask for it,
#: and a server that refuses it has not harmed a session that already ended.
ROLE_TEARDOWN = "teardown"

_METHOD_ROLES = {
    "POST": ROLE_REQUEST,
    "GET": ROLE_STREAM,
    "DELETE": ROLE_TEARDOWN,
}

# Request headers whose values must never reach stderr, even echoed back
# inside a server error body.
_SECRET_HEADERS = ("X-Forwarded-Access-Token", "mcp-session-id")

#: What a redacted secret is replaced with.
_REDACTED = "<redacted>"

#: Cap on how much of a remote body is buffered before it is scrubbed. The
#: snippet the user sees is only ``_BODY_SNIPPET_LIMIT``; this bounds what the
#: proxy holds in memory to get there, so a server cannot make it buffer
#: gigabytes to produce a few hundred characters.
_MAX_SNIPPET_BYTES = 64 * 1024


@functools.lru_cache(maxsize=1)
def _control_chars() -> dict[int, None]:
    """Every character deleted from remote-controlled text before it is printed.

    Derived from Unicode categories rather than enumerated by hand. An
    enumerated list was wrong twice: it is not enough to strip the escapes that
    move a cursor (``Cc``), because the invisible formatting characters
    (``Cf`` -- soft hyphen, word joiner, the bidi controls, the tag block) let a
    server sit one of them between every character of a credential. That
    defeats an exact-match redactor while still *rendering* as the bare secret
    to anyone reading the log. Categories close the class; a list closes
    whichever members somebody thought of.

    Whitespace is deliberately not excluded: this runs after the collapse, so
    the only whitespace left is the single space the collapse produced, and
    ``U+0020`` is ``Zs`` rather than ``Cc``.

    Built lazily and cached -- the scan is ~100ms, and it is only ever needed
    on a path that is already about to print a diagnosis.
    """
    return dict.fromkeys(cp for cp in range(sys.maxunicode + 1) if unicodedata.category(chr(cp)) in ("Cc", "Cf"))


async def stamp_role(request: httpx.Request) -> None:
    """Record the session role of ``request``, first-wins.

    Registered as an httpx ``request`` event hook. Hooks fire once per redirect
    hop, and ``_build_redirect_request`` copies ``extensions`` forward, so the
    first stamp survives a POST-to-GET rewrite: the later hop sees the key
    already present and declines to overwrite it.

    Request hooks are invoked outside httpx's own ``try``, so a raise here
    would escape without even closing the response. ``setdefault`` on a dict
    httpx guarantees exists cannot realistically fail, but Principle 3 covers
    both hooks rather than only the response one.
    """
    # Defensive: setdefault on a dict httpx guarantees exists cannot raise.
    with contextlib.suppress(Exception):
        request.extensions.setdefault(
            _ROLE_KEY,
            _METHOD_ROLES.get(request.method.upper(), ROLE_REQUEST),
        )


async def guard_forwarded_token(request: httpx.Request) -> None:
    """Drop credential headers on any hop that leaves the first origin.

    httpx pops ``Authorization`` when a redirect crosses origins but knows
    nothing about this header, so without this a single ``302`` hands a live
    workspace credential to a host the user never named -- and hands it over
    *with the real ``Authorization`` header already stripped*, so the foreign
    origin learns a token it was never sent directly.

    First-wins on the origin, for the same reason ``stamp_role`` is first-wins
    and by the same mechanism: httpx copies ``extensions`` per hop, so the value
    recorded on the first request is carried into every rebuilt one. The first
    hop is by definition the target the user asked for.

    A request hook rather than a check in ``_apply_headers``, because the auth
    flow runs once per attempt while redirects are rebuilt beneath it -- only a
    hook sees every hop.
    """
    # Two properties at once, and the structure is what gets both.
    #
    # Never raises: a request hook is invoked outside httpx's own ``try``, so
    # an exception here escapes without even closing the response.
    #
    # Fails *closed*: the headers come off first and go back on only once the
    # origin has been confirmed to match, so anything unexpected in between
    # leaves them off. A blanket suppress is right for ``stamp_role``, where
    # failing open merely loses a label; here failing open would hand a live
    # credential to a foreign origin.
    with contextlib.suppress(Exception):
        carried = {name: request.headers.pop(name) for name in _SECRET_HEADERS if name in request.headers}
        if not carried:
            return
        origin = (request.url.scheme, request.url.host, request.url.port)
        if request.extensions.setdefault(_ORIGIN_KEY, origin) == origin:
            request.headers.update(carried)


def arm_retry(request: httpx.Request, *, armed: bool) -> None:
    """Record whether a 401 on ``request`` will be retried by the auth layer.

    Assigns unconditionally, unlike ``stamp_role``'s ``setdefault``. The two
    keys want opposite semantics on the same dict and the difference is easy to
    get backwards, so ``armed`` is keyword-only to make the transition visible
    at every call site: a role is an immutable property of the logical
    operation and must survive a redirect rewrite, while the retry marker is a
    property of the current attempt and must flip ``True`` -> ``False`` when
    the retry itself goes out.

    Callers must pass the *original* request, never ``response.request``. Under
    a redirect the latter is that hop's snapshot -- httpx copies extensions per
    hop rather than sharing them -- so disarming it would write into a dict
    that is then discarded, the rebuilt retry would still read as armed, both
    401s would be suppressed, and the proxy would hang with no diagnosis at all.
    """
    with contextlib.suppress(Exception):
        request.extensions[_RETRY_KEY] = armed


def retry_armed(request: httpx.Request) -> bool:
    """True if a 401 on ``request`` is about to be retried."""
    return bool(request.extensions.get(_RETRY_KEY, False))


def set_remediation(request: httpx.Request, text: str) -> None:
    """Attach proxy-authored remediation text for a 401/403 on ``request``."""
    with contextlib.suppress(Exception):
        request.extensions[_REMEDIATION_KEY] = text


def remediation(request: httpx.Request) -> str | None:
    """Return the proxy-authored remediation for ``request``, if any."""
    text = request.extensions.get(_REMEDIATION_KEY)
    return text if isinstance(text, str) else None


def scrub_body(text: str, secrets: Sequence[str]) -> str:
    """Return ``text`` safe to print: normalized, then secrets removed, then bounded.

    The step order is load-bearing, and it is not the intuitive one.

    Normalization runs **first**. Collapsing and the control strip both *delete*
    characters, so redacting ahead of them lets a server defeat ``str.replace``
    by echoing the credential with one byte inserted into the middle of it --
    and then this function removes that byte and reassembles the secret
    verbatim on its way to the terminal. Redacting first buys nothing: the
    property that matters, that a secret straddling the truncation boundary is
    never half-printed, only requires redaction to precede *truncation*, which
    it still does.

    A separator that survives normalization -- a run of whitespace collapses to
    a single space rather than vanishing -- would still hide the secret from
    exact matching, so a separator-tolerant pass follows.

    Secret-agnostic so the token-exchange path, whose request carries a
    credential in the body as well as the header, shares this one
    implementation of the terminal-escape defense rather than growing a second.
    """
    scrubbed = " ".join(text.split()).translate(_control_chars())
    for secret in secrets:
        if not secret:
            continue
        # One pass, tolerant of the single space a collapsed whitespace run
        # leaves behind. Anchored on the secret's own characters and facing a
        # haystack with no multi-character whitespace run, so each ``\s*`` can
        # match at most one character and it cannot backtrack.
        scrubbed = re.sub(r"\s*".join(map(re.escape, secret)), _REDACTED, scrubbed)
    # Truncation happens last and nothing truncates before it. Any earlier cut
    # -- including a "just to bound the work" one -- can sever a secret and let
    # the surviving half through, which is the whole hazard this ordering
    # exists to prevent. Callers bound the *read* instead.
    return scrubbed[:_BODY_SNIPPET_LIMIT]


def scrub_reason(reason: str, secrets: Sequence[str] = ()) -> str:
    """Return an HTTP reason phrase safe to interpolate into a diagnosis.

    The reason phrase is as server-authored as the body and reaches us intact:
    h11's grammar rejects only NUL and whitespace, so ESC passes validation and
    httpx's ASCII decode preserves it. Every headline in this module
    interpolates it, so without this the escape defense built for the body
    snippet is simply walked around -- and unlike the body, the status line is
    bounded only by h11's 16 KiB header limit.

    ``secrets`` is not optional in spirit. Stripping escapes without redacting
    leaves a channel that prints a credential *in full*, on the line directly
    above a body the same function successfully protected: a server answering
    ``403 Forbidden token=<the token it just received>`` needs no positioning
    and no padding to do it.
    """
    return scrub_body(reason, secrets)[:_REASON_LIMIT]


class HttpErrorReporter:
    """Reports HTTP failures from the remote MCP server and signals shutdown.

    Reporting and fatality are deliberately separate. ``last_message`` records
    every diagnosis so the backstop in ``__main__`` can attribute an exit that
    the SDK unwound on its own; ``fatal_message`` is set only when the proxy
    itself should stop. A background-stream failure that later recovered must
    never poison an otherwise successful exit.
    """

    def __init__(self, url: str, profile: str, auth_type: str) -> None:
        self.url = url
        self.profile = profile
        self.auth_type = auth_type
        self.fatal = anyio.Event()
        self.fatal_message: str | None = None
        self.last_message: str | None = None
        self.reported: set[tuple[str, int]] = set()
        self.shutting_down = False

    @property
    def diagnosed(self) -> bool:
        """True once any diagnosis has reached stderr, from either entrance.

        ``reported`` is keyed on ``(role, status)`` and only ``_report`` can
        populate it, so a proxy-side failure -- which has no response and
        therefore neither a role nor a status -- would leave it empty. The
        backstop in ``__main__`` reads this instead, so both entrances answer
        the one question it actually asks: has the user already been told?
        """
        return bool(self.reported) or self.fatal_message is not None

    def report_fatal(self, message: str) -> None:
        """Report a proxy-side failure that has no server response behind it.

        The second entrance into this class. ``_report`` is driven by httpx's
        response hook and keys everything off an ``httpx.Response``; this one is
        called by the auth layer when the *proxy* failed instead of the server.
        Both must leave this object in the same state and obey the same guards,
        because ``run()`` reads only that state.

        Returns early while shutting down, symmetric with ``_report``. The SDK's
        teardown DELETE goes out through the same auth flow, so on a session
        that outlived its token this can be reached by an exchange the user
        never asked for; a clean multi-hour session must not exit non-zero
        because a credential refresh failed on the way out the door.

        On the background GET stream the SDK swallows the raised failure into
        ``logger.debug`` and reconnects, so this print is the *only* signal the
        user ever gets. It is not redundant with the raise.
        """
        if self.shutting_down:
            return
        # Idempotent: every in-flight request re-runs the failing refresh, and
        # the user needs the diagnosis once, not once per request.
        if self.fatal_message is not None:
            return
        print(message, file=sys.stderr)
        self.last_message = message
        self.fatal_message = message
        self.fatal.set()

    async def on_response(self, response: httpx.Response) -> None:
        """httpx ``response`` event hook. Never raises except on cancellation."""
        # Last resort -- ``_report`` already guards its own formatting.
        with contextlib.suppress(Exception):
            await self._report(response)

    async def _report(self, response: httpx.Response) -> None:
        if self.shutting_down:
            return
        # Must come before anything that touches the body: reading a 2xx here
        # would consume the SSE stream and silently break the proxy.
        if response.status_code < 400:
            return
        role = response.request.extensions.get(_ROLE_KEY, ROLE_REQUEST)
        # Defense in depth, and unreachable today: the SDK only issues DELETE
        # from ``terminate_session`` during teardown, by which point the
        # ``shutting_down`` check above has already returned. Kept so the rule
        # holds even if a future SDK deletes at some other moment.
        if role == ROLE_TEARDOWN:
            return  # pragma: no cover
        if response.status_code == 401 and retry_armed(response.request):
            # The auth layer re-mints the credential and retries this exactly
            # once. Reporting here would set ``fatal_message`` and fire
            # ``fatal``, and ``_watch_fatal`` would ``_abort()`` the process
            # before the retry was ever dispatched -- httpx runs response event
            # hooks strictly before it hands the response back to the auth flow.
            #
            # Returning here, rather than after the dedup check below, is
            # deliberate on both sides: nothing is added to ``reported``, so a
            # suppressed 401 neither consumes the ``(role, status)`` slot the
            # real report will need nor makes ``diagnosed`` true; and the body
            # is left unread, so httpx can still consume it on the way back up.
            print(
                "uc-mcp-proxy: credential rejected (401); refreshing and retrying once.",
                file=sys.stderr,
            )
            return
        key = (role, response.status_code)
        if key in self.reported:
            return

        try:
            snippet = await self._read_snippet(response)
            fatal = role == ROLE_REQUEST
            message = self._format(response, snippet, fatal=fatal)
        except Exception:
            # Fall back to a message built only from values we already hold, so
            # a formatting bug degrades to a terse diagnosis rather than to the
            # traceback this module exists to prevent.
            fatal = role == ROLE_REQUEST
            message = (
                f"uc-mcp-proxy: the remote MCP server returned HTTP "
                f"{response.status_code} for {self.url} "
                f"(profile={self.profile}, auth_type={self.auth_type})."
            )

        print(message, file=sys.stderr)
        # Recorded only after a successful emit, so the backstop can never
        # attribute an exit to a message the user never saw.
        self.reported.add(key)
        self.last_message = message
        if fatal and self.fatal_message is None:
            self.fatal_message = message
            self.fatal.set()

    async def _read_snippet(self, response: httpx.Response) -> str:
        """Return a short, redacted excerpt of the error body, or ``""``.

        The client's read timeout is 300s, so an unguarded read of a stalled
        error body would hang the proxy. After a timeout the response is
        consumed but not closed and ``.text`` raises ``ResponseNotRead``, so
        the flag -- not a try around ``.text`` alone -- is what makes this safe.
        """
        chunks: list[bytes] = []
        read_ok = False
        with anyio.move_on_after(_BODY_READ_TIMEOUT):
            # Bounded, not ``aread()``. The timeout alone caps how *long* a
            # server can talk, which on a fast link is still hundreds of
            # megabytes buffered to produce a few hundred printed characters.
            size = 0
            async for chunk in response.aiter_bytes():
                chunks.append(chunk)
                size += len(chunk)
                if size >= _MAX_SNIPPET_BYTES:
                    break
            read_ok = True
        if not read_ok:
            return ""
        text = b"".join(chunks)[:_MAX_SNIPPET_BYTES].decode("utf-8", errors="replace")
        return scrub_body(text, self._secrets(response.request))

    def _secrets(self, request: httpx.Request) -> list[str]:
        """Credentials carried by ``request`` that a server could echo back."""
        authorization = request.headers.get("Authorization", "")
        return [
            authorization.removeprefix("Bearer ") if authorization.startswith("Bearer ") else "",
            *(request.headers.get(header, "") for header in _SECRET_HEADERS),
        ]

    def _format(self, response: httpx.Response, snippet: str, *, fatal: bool) -> str:
        headline, advice = self._diagnose(response)
        lines = [
            headline,
            f"  url:       {self.url}",
            f"  profile:   {self.profile}",
            f"  auth_type: {self.auth_type}",
        ]
        if snippet:
            lines.append(f"  server:    {snippet}")
        lines.append(advice)
        lines.append(self._disposition(fatal=fatal))
        return "\n".join(lines)

    def _diagnose(self, response: httpx.Response) -> tuple[str, str]:
        """Return ``(headline, remediation)`` for this status.

        Deliberately does not reuse ``auth.py``'s remediation text. By the time
        a remote 401 arrives, preflight has already minted a token, so that
        module's "missing or expired -- generate a new token" wording is a false
        statement about the user's machine: it recasts a remote-authorization
        failure as a local-credential failure.
        """
        status = response.status_code
        reason = scrub_reason(response.reason_phrase or "", self._secrets(response.request))
        # Proxy-authored, never server-authored, so it needs no scrubbing. Only
        # the remediation is substituted; the headline stays as written, which
        # is what keeps this module from having to know what the proxy did to
        # the credential before sending it.
        proxy_remediation = remediation(response.request)

        if status == 401:
            return (
                f"uc-mcp-proxy: the remote MCP server rejected your credentials (HTTP {status} {reason}).",
                proxy_remediation
                or f"The token for profile {self.profile!r} was minted successfully, so the "
                f"server rejected it rather than it being absent locally. The token may "
                f"have expired, or this profile's identity may not be recognized by the "
                f"target.",
            )
        if status == 403:
            return (
                f"uc-mcp-proxy: the remote MCP server refused this request (HTTP {status} {reason}).",
                # The default text advises OAuth U2M -- a browser login. When
                # the proxy has already exchanged the credential for this
                # target, that is the one remedy it just made unnecessary, and
                # the audience this feature exists for has no browser.
                proxy_remediation
                or f"The credential for profile {self.profile!r} authenticated successfully "
                f"but is not authorized for this target. If the target is a Databricks "
                f"App, it may require OAuth U2M rather than a PAT.",
            )
        if status == 404:
            if "mcp-session-id" in response.request.headers:
                return (
                    f"uc-mcp-proxy: the MCP session expired server-side (HTTP {status} {reason}).",
                    "The server no longer recognizes this session. Restart the MCP client "
                    "to establish a new one. This is not an authentication failure.",
                )
            return (
                f"uc-mcp-proxy: no MCP endpoint at this URL (HTTP {status} {reason}).",
                "Check --url. This is not an authentication failure.",
            )
        if status >= 500:
            return (
                f"uc-mcp-proxy: the remote MCP server failed (HTTP {status} {reason}).",
                "This is a server-side error, not an authentication problem.",
            )
        return (
            f"uc-mcp-proxy: the remote MCP server rejected this request (HTTP {status} {reason}).",
            "This is not an authentication failure.",
        )

    def _disposition(self, *, fatal: bool) -> str:
        """Say what happens next.

        Wording for the non-fatal case is channel-neutral on purpose. A
        reconnecting stream is usually the notification channel, but it can
        also be a tool call's SSE response being resumed -- calling it
        "notifications" would misdescribe which channel the user just lost.
        """
        if fatal:
            return "Exiting."
        return (
            "The proxy will keep running, but this background stream from the "
            "server will be retried only a bounded number of times and will then "
            "stop; server-initiated messages may be lost for the rest of this session."
        )


def _leaves(exc: BaseException) -> list[BaseException]:
    """Flatten nested exception groups into their non-group leaves.

    Duck-typed on ``.exceptions`` rather than ``BaseExceptionGroup`` because
    this package supports Python 3.10, where that name does not exist.
    """
    nested = getattr(exc, "exceptions", None)
    if not isinstance(nested, (list, tuple)):
        return [exc]
    leaves: list[BaseException] = []
    for sub in nested:
        leaves.extend(_leaves(sub))
    return leaves


def is_only_diagnosed_errors(exc: BaseException) -> bool:
    """True if every leaf of ``exc`` has already been diagnosed to stderr.

    Generalizes the swallow decision to the second failure shape: a
    ``ProxyFatalError`` the proxy raised *after* reporting it. Keeps the same
    positive, non-empty match, so a bare ``CancelledError`` still answers False
    and Ctrl-C is never reported as a credential rejection.
    """
    leaves = _leaves(exc)
    return bool(leaves) and all(isinstance(leaf, (httpx.HTTPStatusError, ProxyFatalError)) for leaf in leaves)
