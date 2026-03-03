# uc-mcp-proxy

MCP stdio-to-Streamable-HTTP proxy with Databricks OAuth.

## Commands

- `uv sync` — install all dependencies (including dev)
- `uv run pytest -m unit -v` — run unit tests only
- `uv run pytest -m integration -v` — run integration tests only
- `uv run pytest -v` — run all tests
- `uv run pytest -m unit --cov -v` — unit tests with coverage
- `uv build` — build sdist + wheel into `dist/`

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
