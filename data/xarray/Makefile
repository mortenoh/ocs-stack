.PHONY: help install lint test coverage run run-all docs docs-serve docs-build clean ci

# ==============================================================================
# Venv
# ==============================================================================

UV := $(shell command -v uv 2> /dev/null)

# ==============================================================================
# Targets
# ==============================================================================

help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  install      Install dependencies"
	@echo "  lint         Run formatter, linter, and type checkers"
	@echo "  test         Run tests"
	@echo "  coverage     Run tests with coverage reporting"
	@echo "  run          Run one example (use EXAMPLE=0101_basics)"
	@echo "  run-all      Run every example"
	@echo "  docs-serve   Serve documentation locally with live reload"
	@echo "  docs-build   Build documentation site"
	@echo "  docs         Alias for docs-serve"
	@echo "  ci           lint + test + docs-build"
	@echo "  clean        Clean up temporary files"

install:
	@echo ">>> Installing dependencies"
	@$(UV) sync

lint:
	@echo ">>> Running linter"
	@$(UV) run ruff format .
	@$(UV) run ruff check . --fix
	@echo ">>> Running type checkers"
	@$(UV) run mypy --explicit-package-bases src tests examples
	@$(UV) run pyright

test:
	@echo ">>> Running tests"
	@$(UV) run pytest -q

coverage:
	@echo ">>> Running tests with coverage"
	@$(UV) run coverage run -m pytest -q
	@$(UV) run coverage report
	@$(UV) run coverage xml

run:
	@test -n "$(EXAMPLE)" || { echo "set EXAMPLE=<name>, e.g. make run EXAMPLE=0101_basics"; exit 2; }
	@$(UV) run python examples/$(EXAMPLE).py

run-all:
	@for f in examples/*.py; do \
		echo ">>> Running $$f"; \
		$(UV) run python $$f || exit 1; \
	done

# NO_MKDOCS_2_WARNING silences the Material for MkDocs promotional banner.
docs-serve:
	@echo ">>> Serving documentation at http://127.0.0.1:8000"
	@NO_MKDOCS_2_WARNING=1 $(UV) run mkdocs serve

docs-build:
	@echo ">>> Building documentation site"
	@NO_MKDOCS_2_WARNING=1 $(UV) run mkdocs build

docs: docs-serve

ci: lint test docs-build

clean:
	@echo ">>> Cleaning up"
	@find . -type f -name "*.pyc" -delete
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@rm -rf .pytest_cache .ruff_cache .mypy_cache .pyright
	@rm -rf .coverage htmlcov coverage.xml
	@rm -rf dist build site *.egg-info

# ==============================================================================
# Default
# ==============================================================================

.DEFAULT_GOAL := help
