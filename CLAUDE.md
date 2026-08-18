# CLAUDE.md

Guidance for Claude Code when working anywhere in this repository. This is the
only `CLAUDE.md` in the repo.

This is `climate-stack`: self-contained tutorial projects, one per layer of the
climate data stack — xarray, dask, dask.distributed, icechunk, and a pipeline
capstone — sitting directly at the repository root as `<project>/`. There is
no root `pyproject.toml` and no uv workspace — every project has its own
`pyproject.toml`, `.venv`, and `uv.lock`. Run `uv` and per-project `make` from
inside a project directory. `make list` prints every project.

When in doubt, follow conventions from chapkit:
https://github.com/dhis2-chap/chapkit — the per-project template below is
lifted from it.

## Layout

Projects are flat. There are no topic groups: with a handful of projects they
added a category decision per project and expressed nothing that `docs/` does
not say better in prose. Do not reintroduce them without a real reason.

Package names are prefixed but otherwise flat: the `xarray` project is
distribution `climate-stack-xarray`, module `climate_stack_xarray`. The prefix is
required — a project studying `polars` cannot have a module named `polars`
while depending on it.

## Documentation

Documentation is **centralised** in `docs/` at the repository root and built as
one mkdocs site from the root `mkdocs.yml`. Projects do NOT have their own
`docs/` directory or `mkdocs.yml`.

- Long-form pages live in `docs/`: `index`, `stack`, `scaling`, `storage`,
  `open-climate-service`, `conventions`.
- `docs/reference/<name>.md` is an mkdocstrings stub per project.
- Projects keep their own `README.md` and `ROADMAP.md` — working files that
  render fine on their own. Keep the roadmap accurate as examples land.
- Write markdown that reads well as a plain file, not only when rendered.

`make offline` at the root renders the same pages into one self-contained
`dist/climate-stack.html` plus a PDF, and `make share` serves the site over
Tailscale. `scripts/build-book.py` reads its page list from the `nav` in
`mkdocs.yml`, so adding a page to the nav is enough — do not give the book its
own list.

### What a project page must be

`docs/projects/<name>.md` is **the** documentation for that library: a
teaching text someone can read start to finish and come away able to use the
thing. Target 3000-4000 lines. It is NOT a summary, NOT an index, and NOT a
reformatted `ROADMAP.md` — a page that lists example filenames with one-line
descriptions is a failure, however tidy it looks.

Every project page contains, in this order:

1. What the project is, in a paragraph.
2. **A real introduction to the technology itself** — several hundred lines.
   What problem it solves, the mental model, how it relates to what the reader
   already knows, and when not to use it. Assume no prior exposure.
3. Setup: how to install and run it here.
4. Core concepts, each with runnable code, its real output, and its pitfalls.
5. Every example covered in depth — 80-200 lines each — with key code
   extracted from the file, real output, why it matters, and the traps. Link to
   the source file.
6. A consolidated pitfalls section.
7. How it maps to open-climate-service.
8. Where to go next, and **links to the upstream project's own docs**.

Non-negotiable: read every example before writing about it, run the ones you
quote, and never invent output.

`make docs-build` / `make docs-serve` at the root run mkdocs through `uvx`, so
no root virtualenv exists and none is needed: mkdocstrings reads each project's
source statically via griffe, with `paths` in `mkdocs.yml` pointing at the
`src/` directories. Adding a project means adding it to `paths`, `nav`, and a
page in `docs/projects/` and `docs/reference/`.

## Per-project template (from chapkit)

- Python 3.13 (`.python-version`), `uv` for everything, `uv_build` backend,
  src layout: `src/climate_stack_<name>/`.
- ruff: `target-version = "py313"`, `line-length = 120`, select
  `E, W, F, I, D`, google docstring convention, per-file ignores as in the
  starter project.
- mypy (strict flags, `mypy_path = ["src"]`) and pyright
  (`typeCheckingMode = "strict"`) — both must pass. Where they disagree, prefer
  a form both accept (`getattr`/`setattr`, a narrowly-typed `Any` with a
  one-line comment) over duelling suppressions.
- pytest + coverage; tests in `tests/`, excluded from docstring rules.
- Examples in `examples/PPNN_topic_name.py` (`PP` = phase, `NN` = example
  number): each a self-contained lesson with a module docstring header
  (what/why/how to run), `# SECTION` banner comments, printed prose before
  each demo, and a `=== Summary ===` block at the end. Copy the shape of
  `xarray/examples/` rather than reinventing it.
- A `README.md` and a `ROADMAP.md` (phase plan, one line per example) kept
  accurate as examples land.

### Makefile (every project)

`.DEFAULT_GOAL := help`, exposing at least: `install` (uv sync), `lint`
(ruff format + ruff check --fix + mypy + pyright), `test`, `coverage`,
`run EXAMPLE=<name>`, `run-all`, `clean`, and `ci` (lint + test). Copy
`xarray/Makefile` as the baseline. Docs targets belong to the root Makefile,
not to projects.

### Dependencies

Use `uv add` (and `uv add --dev`) so the latest version is picked up, and fix
breaking API changes rather than pinning to an old version. Where a pin is
genuinely forced, record the reason in a comment next to it.

### Projects with infrastructure

A project needing real services adds a `compose.yml` (plus a `Dockerfile` when
the image needs more than an upstream one) and Makefile targets `up`, `down`,
`ps`, `logs`, and any project-specific controls. `up` must block until the
service is actually ready rather than returning on container start.

The examples still have to run without it. `dask-distributed` is the
reference: a `connect()` helper probes the service, falls back to an
in-process substitute, and prints a note naming what was lost and how to start
the real thing. Every example is verified both ways.

Where a container and the host both take part, pin the image to the versions
in `uv.lock` — a mismatch between a client library and its server is a whole
class of confusing failures.

## Rules

- **Never revert, always fix forward.**
- **Always test the unhappy path.** Invalid input must raise a clear error,
  and tests must cover it.
- **Always run examples and verify output.** A clean lint/type-check is not
  enough — `make run-all` and read the output.
- **Examples needing a server/credentials must still run.** Make them
  environment-driven and gracefully skip with an explanatory message when
  unconfigured.
- **Examples must be deterministic.** Seed every generator with something
  stable across processes; `hash()` is randomized per process and is not.
- **Quote only numbers that came from a run.** Mark timing-derived figures as
  machine-dependent — they do not reproduce exactly.

## Verify before claiming done

`make verify PROJECT=<name>` at the root runs the project's `ci` target (lint,
type checks, tests) plus every example; `make verify-all` sweeps the whole
repository; `make docs-build` builds the site under `--strict` (a mkdocs
warning, such as a link to a heading that does not exist, fails it), checks
every relative link, and runs `make docs-check`, which enforces the
per-example rules above mechanically — template conformance, an 80-line floor
per example section, a link to the source, roadmap agreement in both
directions, and that the project is wired into `nav`, `paths`, and
`docs/reference/`.
When a change touches `docs/`, `make offline` too — the book builder resolves
links differently from mkdocs and is the thing that catches a nav entry
pointing at a page that does not exist.

## Conventions

### Git commits

- Conventional commits: `<type>(<scope>): <description>` with types `feat`,
  `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`.
- Scope is the project directory (`xarray`, `icechunk`); omit it for
  repository-wide changes.
- No attribution or co-authored-by lines. A commit should read as ordinary
  work.
- Keep the subject imperative and under ~72 characters.

### No emojis

No emojis ever — not in code, comments, docs, commit messages, or output.
