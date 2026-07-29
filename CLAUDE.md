# uc-mcp-proxy

MCP stdio-to-Streamable-HTTP proxy with Databricks OAuth.

## Commands

- `uv sync` — install all dependencies (including dev)
- `make test` / `make test-unit` — run unit tests only (default, what CI runs)
- `make test-cov` — unit tests with coverage
- `make test-integration` — run integration tests only (**fires real Databricks auth, can open a browser**)
- `make test-all` — run unit + integration
- `make check` — ruff lint + ruff format-check + mypy
- `make fmt` — auto-format and fix lint
- `uv build` — build sdist + wheel into `dist/`

## Test policy

Integration tests intentionally exercise the real preflight auth flow,
including `databricks auth login` (which pops a browser). They must never
run in CI and should not be the default target. `make test` runs unit
tests only to match CI.

Two things the suite quietly depends on:

- **Every test file must declare `pytestmark`.** `make test` is `pytest -m unit`,
  and an unmarked test is *silently deselected* — no warning, and
  `--strict-markers` only validates markers that are present. A new file with no
  marker makes "tests green" mean nothing. The deselected count is an assertion:
  it should stay at 8.
- **`--tb=short` in `addopts` is load-bearing, not cosmetic.** A long traceback
  from inside `exchange_pat` would dump its locals, which include a live PAT.

## Architecture

Package in `src/uc_mcp_proxy/`:

- `__main__.py` — CLI entry point, `DatabricksAuth` (httpx auth flow), `bridge()` (bidirectional stdio↔HTTP stream copy), `run()` (async main)
- `auth.py` — credential preflight, auto-login, auth-type-specific remediation
- `errors.py` — HTTP error diagnosis and reporting from the remote server
- `token_exchange.py` — RFC 8693 exchange of a PAT for an app-scoped OAuth token
- `__init__.py` — re-exports `DatabricksAuth`

The proxy bridges an MCP stdio transport to a remote Streamable HTTP MCP server, injecting Databricks OAuth tokens on every request via `DatabricksAuth`.

### Error handling

The proxy owns the `httpx.AsyncClient` it hands to the MCP SDK, so that client's
event hooks are the only place that sees every response on both transport
paths. This matters because the paths fail in opposite ways: the SDK's GET SSE
loop swallows failures into `logger.debug` and reconnects, so no exception ever
escapes it, while the POST path raises from inside a `tg.start_soon` task, which
surfaces as a traceback rather than a diagnosis.

Failures are classified by *session role*, not HTTP verb — `follow_redirects=True`
means httpx rewrites POST to GET on 3xx, so the verb no longer describes what the
request was for. `stamp_role` records the role on the way out.

> The proxy exits when the server refuses a request it actually needed to make.
> It warns and keeps going when the server refuses a background stream.

Neither hook may raise: httpx re-raises whatever a response hook throws into the
SDK task that made the request, which is the exact traceback path this design
removes. Cancellation is the deliberate exception and must propagate.

### The armed/disarmed retry contract

`DatabricksAuth` can re-mint a credential and retry a 401 once. That only works
because of an httpx ordering fact: **response event hooks run strictly before
the auth flow is handed the response.** Left alone, the reporter would reach the
first 401 first and `_abort()` the process before the retry was ever dispatched,
so the retry would be present, correct, and dead.

The auth layer therefore marks the request via `arm_retry(request, armed=...)`,
and `_report` suppresses an armed 401 instead of diagnosing it. The keys are
mechanical — a bool plus a proxy-authored remediation string — so `errors.py`
never learns what a token exchange is and the machinery is reusable.

Two rules that are easy to break and hard to notice:

- **Re-yield the original request, never `response.request`.** httpx copies
  `extensions` per redirect hop rather than sharing them, so disarming the hop's
  snapshot writes into a dict that is then discarded. Both 401s get suppressed
  and the proxy hangs with no output at all — strictly worse than the crash.
- **Suppression returns before `reported.add`.** A suppressed 401 must not
  consume the `(role, status)` dedup slot, or the retry's own 401 is silently
  swallowed.

`HttpErrorReporter` has a second entrance, `report_fatal`, for a proxy-side
failure that has no server response behind it. It obeys the same `shutting_down`
guard as `_report` — the teardown DELETE goes out through the auth flow, so a
session that outlived its token can reach it on the way out, and a clean
multi-hour session must not exit non-zero over a refresh it never needed. The
backstop in `run()` reads `reporter.diagnosed`, not `reporter.reported`, because
only the response hook can populate the latter.

> Failures that reach the user through `report_fatal` are the *only* signal on
> the GET SSE path: the SDK swallows the raised exception and reconnects.

### Printing server-controlled text

`scrub_body` normalizes **before** it redacts, and that order is the opposite of
the intuitive one. Normalizing deletes characters, so redacting first lets a
server hide a credential from `str.replace` by echoing it with one byte inserted
— and then the strip removes that byte and reassembles the secret verbatim on
its way to stderr. Both the haystack and each needle go through `_normalize`, so
they cannot drift.

Inside `_normalize` the order matters again, and the other way round: **strip
invisible characters first, collapse whitespace last.** Collapsing first lets a
later deletion re-join the spaces either side of it and re-create a run the
collapse had already flattened. The redactor's pattern is `\s?` rather than
`\s*` precisely because no run longer than one can exist — and when that
stopped being true, a server-chosen `mcp-session-id` containing spaces drove the
match into catastrophic backtracking: 5.8 seconds at nine spaces, growing ~3.5×
per space, on the thread that also runs the abort path.

**Truncation is the recurring hazard.** Nothing may cut between the read and
`scrub_body`, because a cut severs a secret and the surviving half matches
nothing. That bug shipped twice — once as an 8 KiB redaction window, once as a
byte slice after the read loop. The read caps themselves still land wherever
they land, so `scrub_body` finishes by sweeping a trailing partial secret off
the end; reading further is not a fix, since the next cap has the same edge.

The stripped character set is derived from Unicode categories (`Cc` + `Cf`), not
enumerated. An enumerated list was wrong twice: stripping the escapes that move
a cursor is not enough, because the *invisible* formatting characters — soft
hyphen, word joiner, the bidi controls, the tag block — let a server sit one
between every character of a credential, defeating an exact-match redactor while
still rendering as the bare secret to whoever reads the log.

Three more things are easy to miss:

- **The reason phrase needs the secret list, not just the escape strip.** It is
  server-authored, it is interpolated into every headline, and a server that
  answers `403 Forbidden token=<what it just received>` needs no padding and no
  positioning to print a credential in full on the line directly above a body
  that was correctly redacted.

- **The body snippet is not the only remote-controlled text.** The status line's
  reason phrase is equally server-authored — h11's grammar rejects only NUL and
  whitespace, so ESC survives — and it is interpolated into every headline.
  `scrub_reason` exists so the escape defense cannot simply be walked around.
- **Credential headers outlive their origin unless something stops them.**
  httpx pops `Authorization` on a cross-origin redirect but knows nothing about
  `X-Forwarded-Access-Token` or `mcp-session-id`, so a single 302 would hand a
  live credential to a foreign host *with the real credential already stripped*.
  `guard_forwarded_token` is a request hook rather than a check in
  `_apply_headers` because the auth flow runs once per attempt while redirects
  are rebuilt beneath it — only a hook sees every hop.
- **That guard fails closed; `stamp_role` fails open.** Both are wrapped in a
  blanket `suppress`, because neither may raise. But the guard pops the headers
  *first* and puts them back only once the origin is confirmed, so a failure
  mid-check leaves them off. Failing open loses a label in one case and leaks a
  credential in the other.

## Testing

Tests live in `tests/` with two marker categories:

- `unit` — pure unit tests, no external dependencies, fast
- `integration` — full proxy flow tests with mocked transports

All new code must have unit tests. Maintain ≥75% coverage (`fail_under = 75` in pyproject.toml).

## Code Style

- Use `from __future__ import annotations` in all modules
- Type hints on all public functions
- Keep imports sorted: stdlib → third-party → local
