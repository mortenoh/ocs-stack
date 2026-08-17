# CLAUDE.md

Guidance for Claude Code when working anywhere in this repository. This is the
only `CLAUDE.md` in the repo.

This is a Python playground: self-contained tutorial projects, one per library
or subject, living in topic groups as `<group>/<project>/`. There is no root
`pyproject.toml` and no uv workspace — every project has its own
`pyproject.toml`, `.venv`, and `uv.lock`. Run `uv` and `make` from inside a
project directory, never from the repository root. `make list` prints every
project.

When in doubt, follow conventions from chapkit:
https://github.com/dhis2-chap/chapkit — the per-project template below is
lifted from it.

## Repository layout

Groups are created as projects appear. The intended set mirrors
playground-rust: `lang/` (language and tooling), `data/` (serialization and
data processing), `db/`, `web/`, `net/`, `async/`, `ml/`, `ui/`, `tools/`,
`domain/`.

## Naming

Directories are bare (`lang/start`); package names carry the full path:
distribution `playground-<group>-<name>`, module `playground_<group>_<name>`
(`lang/start` is `playground-lang-start`, module `playground_lang_start`).
This keeps names unique across groups and means a `data/polars` project never
shadows the `polars` module it depends on. Moving a project to another group
renames its module — update the project's own examples/tests/docs when that
happens.

## Per-project template (from chapkit)

- Python 3.13 (`.python-version`), `uv` for everything, `uv_build` backend,
  src layout: `src/playground_<group>_<name>/`.
- ruff: `target-version = "py313"`, `line-length = 120`, select
  `E, W, F, I, D`, google docstring convention, per-file ignores as in the
  starter project.
- mypy (strict flags, `mypy_path = ["src"]`) and pyright
  (`typeCheckingMode = "strict"` with chapkit's relaxations) — both must pass.
- pytest + coverage; tests in `tests/`, excluded from docstring rules.
- mkdocs-material + mkdocstrings: `mkdocs.yml`, `docs/index.md`,
  `docs/api-reference.md`. `site/` is git-ignored.
- Examples in `examples/PPNN_topic_name.py` (`PP` = phase, `NN` = example
  number): each a self-contained lesson with a module docstring header
  (what/why/how to run), `# SECTION` banner comments, printed prose before
  each demo, and a `=== Summary ===` block at the end. Copy the shape of
  `lang/start/examples/` rather than reinventing it.
- A `README.md` and, once a project grows phases, a `ROADMAP.md` (phase plan,
  one line per example) kept accurate as examples land.

### Makefile (every project)

`.DEFAULT_GOAL := help`, exposing at least: `install` (uv sync), `lint`
(ruff format + ruff check --fix + mypy + pyright), `test`, `coverage`,
`run EXAMPLE=<name>`, `run-all`, `docs-serve`, `docs-build`, `clean`, and
`ci` (lint + test + docs-build). Copy `lang/start/Makefile` as the baseline.

### Dependencies

Use `uv add` (and `uv add --dev`) so the latest version is picked up, and fix
breaking API changes rather than pinning to an old version. Where a pin is
genuinely forced, record the reason in a comment next to it.

### Projects with infrastructure

A project needing real services adds a `compose.yml` (plus a `Dockerfile` when
the image needs more than an upstream one) and Makefile targets `up`, `down`,
`ps`, `logs`, and any project-specific controls. `up` must block until the
service is actually ready rather than returning on container start.

The examples still have to run without it. `data/dask-distributed` is the
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

## Verify before claiming done

`make verify PROJECT=group/name` at the root runs the project's `ci` target
(lint, tests, docs build) plus every example; `make verify-all` sweeps the
whole repository.

## Conventions

### Git commits

- Conventional commits: `<type>(<scope>): <description>` with types `feat`,
  `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`.
- Scope is the project directory without the group (`start`, `polars`); omit
  it for repository-wide changes.
- No attribution or co-authored-by lines. A commit should read as ordinary
  work.
- Keep the subject imperative and under ~72 characters.

### No emojis

No emojis ever — not in code, comments, docs, commit messages, or output.
