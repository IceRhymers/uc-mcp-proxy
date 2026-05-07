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

Single-module package in `src/uc_mcp_proxy/`:

- `__main__.py` — CLI entry point, `DatabricksAuth` (httpx auth flow), `bridge()` (bidirectional stdio↔HTTP stream copy), `run()` (async main)
- `__init__.py` — re-exports `DatabricksAuth`

The proxy bridges an MCP stdio transport to a remote Streamable HTTP MCP server, injecting Databricks OAuth tokens on every request via `DatabricksAuth`.

## Testing

Tests live in `tests/` with two marker categories:

- `unit` — pure unit tests, no external dependencies, fast
- `integration` — full proxy flow tests with mocked transports

All new code must have unit tests. Maintain ≥75% coverage (`fail_under = 75` in pyproject.toml).

## Code Style

- Use `from __future__ import annotations` in all modules
- Type hints on all public functions
- Keep imports sorted: stdlib → third-party → local
