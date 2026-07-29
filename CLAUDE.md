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

## Architecture

Package in `src/uc_mcp_proxy/`:

- `__main__.py` — CLI entry point, `DatabricksAuth` (httpx auth flow), `bridge()` (bidirectional stdio↔HTTP stream copy), `run()` (async main)
- `auth.py` — credential preflight, auto-login, auth-type-specific remediation
- `errors.py` — HTTP error diagnosis and reporting from the remote server
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

## Testing

Tests live in `tests/` with two marker categories:

- `unit` — pure unit tests, no external dependencies, fast
- `integration` — full proxy flow tests with mocked transports

All new code must have unit tests. Maintain ≥75% coverage (`fail_under = 75` in pyproject.toml).

## Code Style

- Use `from __future__ import annotations` in all modules
- Type hints on all public functions
- Keep imports sorted: stdlib → third-party → local
