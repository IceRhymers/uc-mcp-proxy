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

## Testing

Tests live in `tests/` with two marker categories:

- `unit` — pure unit tests, no external dependencies, fast
- `integration` — full proxy flow tests with mocked transports

All new code must have unit tests. Maintain ≥75% coverage (`fail_under = 75` in pyproject.toml).

## Code Style

- Use `from __future__ import annotations` in all modules
- Type hints on all public functions
- Keep imports sorted: stdlib → third-party → local
