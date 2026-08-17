# Root Makefile for the tutorial collection.
# Projects live in <group>/<name>/ with their own Makefiles; this one drives
# verification across the whole collection via scripts/verify.sh.

.DEFAULT_GOAL := help
SHELL := /bin/bash

# Override on the command line, e.g. `make verify PROJECT=lang/start`.
PROJECT ?=

.PHONY: help list verify verify-all

help: ## Show this help
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | \
		awk -F':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'
	@echo
	@echo "Examples:"
	@echo "  make verify PROJECT=lang/start   # lint + tests + docs + run examples"
	@echo "  make verify-all                  # the full local sweep"

list: ## List all discoverable projects (group/name)
	@./scripts/verify.sh --list

verify: ## Verify one project (set PROJECT=group/name): lint, tests, docs, examples
	@test -n "$(PROJECT)" || { echo "set PROJECT=group/name or a bare name (see: make list)"; exit 2; }
	@./scripts/verify.sh $(PROJECT)

verify-all: ## Full local sweep over every project
	@./scripts/verify.sh --all
