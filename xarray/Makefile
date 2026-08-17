.PHONY: help install lint test coverage run run-all clean ci

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
	@echo "  ci           lint + test"
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

ci: lint test

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
