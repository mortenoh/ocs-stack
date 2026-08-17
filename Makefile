# Root Makefile for the tutorial collection.
# Each project sits at the repository root with its own Makefile; this one
# drives verification across the collection and builds the documentation site.

.DEFAULT_GOAL := help
SHELL := /bin/bash

# Override on the command line, e.g. `make verify PROJECT=xarray`.
PROJECT ?=

# The docs site needs none of the projects' dependencies: mkdocstrings reads
# their source statically, so uvx supplies the toolchain and there is no root
# virtualenv to keep in sync. NO_MKDOCS_2_WARNING silences a promo banner.
MKDOCS := NO_MKDOCS_2_WARNING=1 uvx --quiet \
	--with mkdocs-material \
	--with 'mkdocstrings[python]' \
	mkdocs

# The offline book needs only a markdown converter, so it gets its own, much
# smaller uvx environment rather than borrowing the mkdocs one.
BOOK := uvx --quiet \
	--with markdown \
	--with pymdown-extensions \
	--with pyyaml \
	python

.PHONY: help list verify verify-all docs docs-serve docs-build docs-links \
	book pdf offline share clean

# Port for `make share`; override with `make share PORT=9000`.
PORT ?= 8000

help: ## Show this help
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | \
		awk -F':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'
	@echo
	@echo "Examples:"
	@echo "  make verify PROJECT=xarray   # lint + tests + run every example"
	@echo "  make verify-all              # the full local sweep"
	@echo "  make docs-serve              # the documentation site"
	@echo "  make offline                 # one HTML file and a PDF, for reading away from a desk"
	@echo "  make share                   # serve the site to your own devices over Tailscale"

list: ## List all discoverable projects
	@./scripts/verify.sh --list

verify: ## Verify one project (set PROJECT=<name>): lint, tests, examples
	@test -n "$(PROJECT)" || { echo "set PROJECT=<name> (see: make list)"; exit 2; }
	@./scripts/verify.sh $(PROJECT)

verify-all: ## Full local sweep over every project
	@./scripts/verify.sh --all

docs-serve: ## Serve the documentation site with live reload
	@echo ">>> Serving documentation at http://127.0.0.1:8000"
	@$(MKDOCS) serve

docs-build: ## Build the documentation site into site/
	@echo ">>> Building documentation site"
	@$(MKDOCS) build
	@$(MAKE) --no-print-directory docs-links

docs-links: ## Check that every relative link in docs/ resolves
	@./scripts/check-links.sh

docs: docs-serve

# ==============================================================================
# Reading away from a desk
# ==============================================================================

book: ## Render all docs as one self-contained HTML file in dist/
	@echo ">>> Building the offline book"
	@$(BOOK) scripts/build-book.py

pdf: book ## Print the book to dist/climate-stack.pdf (needs Chrome or Chromium)
	@./scripts/build-pdf.sh

offline: pdf ## Build both the single-file book and the PDF
	@echo
	@echo "For a phone, AirDrop or copy dist/climate-stack.html -- it works offline,"
	@echo "reflows to the screen, and follows the system light/dark setting."
	@echo "dist/climate-stack.pdf is the same content paginated for A4."

share: docs-build ## Serve the site to your own devices over Tailscale (tailnet only)
	@./scripts/serve-tailscale.sh $(PORT)

clean: ## Remove the built documentation site and offline builds
	@rm -rf site dist
