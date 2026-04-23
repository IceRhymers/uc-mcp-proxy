.PHONY: lint fmt-check typecheck check fmt

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
