.PHONY: help install lint test coverage run run-all docs docs-serve docs-build clean ci up down restart logs ps scale dashboard wait

# ==============================================================================
# Venv
# ==============================================================================

UV := $(shell command -v uv 2> /dev/null)
COMPOSE := docker compose

# Worker count for `make scale N=5`.
N ?= 3

# ==============================================================================
# Targets
# ==============================================================================

help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Cluster:"
	@echo "  up           Build and start scheduler + workers, wait until ready"
	@echo "  down         Stop the cluster and remove its volume"
	@echo "  restart      down then up"
	@echo "  ps           Show running containers"
	@echo "  logs         Follow cluster logs"
	@echo "  scale        Change worker count (use N=5)"
	@echo "  dashboard    Open the scheduler dashboard in a browser"
	@echo ""
	@echo "Project:"
	@echo "  install      Install dependencies"
	@echo "  lint         Run formatter, linter, and type checkers"
	@echo "  test         Run tests"
	@echo "  coverage     Run tests with coverage reporting"
	@echo "  run          Run one example (use EXAMPLE=0101_connect)"
	@echo "  run-all      Run every example"
	@echo "  docs-serve   Serve documentation locally with live reload"
	@echo "  docs-build   Build documentation site"
	@echo "  ci           lint + test + docs-build"
	@echo "  clean        Clean up temporary files"
	@echo ""
	@echo "Examples run against the compose cluster when it is up, and fall back"
	@echo "to an in-process LocalCluster with a printed note when it is not."

install:
	@echo ">>> Installing dependencies"
	@$(UV) sync

up:
	@echo ">>> Building and starting the cluster"
	@$(COMPOSE) up -d --build
	@$(MAKE) --no-print-directory wait

wait:
	@echo ">>> Waiting for the scheduler to accept connections"
	@$(UV) run python -c "from playground_data_dask_distributed import wait_for_scheduler; wait_for_scheduler()"

down:
	@echo ">>> Stopping the cluster and removing its volume"
	@$(COMPOSE) down -v

restart: down up

ps:
	@$(COMPOSE) ps

logs:
	@$(COMPOSE) logs -f

scale:
	@echo ">>> Scaling to $(N) workers"
	@$(COMPOSE) up -d --no-recreate --scale worker=$(N)

dashboard:
	@echo ">>> Opening http://127.0.0.1:8787/status"
	@python3 -c "import webbrowser; webbrowser.open('http://127.0.0.1:8787/status')"

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
	@test -n "$(EXAMPLE)" || { echo "set EXAMPLE=<name>, e.g. make run EXAMPLE=0101_connect"; exit 2; }
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
	@rm -rf reports

# ==============================================================================
# Default
# ==============================================================================

.DEFAULT_GOAL := help
