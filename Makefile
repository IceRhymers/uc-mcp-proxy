.PHONY: lint fmt-check typecheck check fmt test test-unit test-integration test-all test-cov

lint:
	uv run ruff check src/ tests/

fmt-check:
	uv run ruff format --check src/ tests/

typecheck:
	uv run mypy

check: lint fmt-check typecheck

fmt:
	uv run ruff format src/ tests/
	uv run ruff check --fix src/ tests/

# Default `test` runs unit tests only — matches CI. Integration tests exercise
# the real Databricks auth flow (including `databricks auth login`, which pops
# a browser) and must be run explicitly via `make test-integration`.
test: test-unit

test-unit:
	uv run pytest -m unit -v

test-cov:
	uv run pytest -m unit --cov -v

test-integration:
	uv run pytest -m integration -v

test-all:
	uv run pytest -v
