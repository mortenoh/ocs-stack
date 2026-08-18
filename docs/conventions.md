# Conventions

## Layout

Projects sit directly at the repository root, one directory each. There is no
root package, no uv workspace, and no topic grouping — with a handful of
projects, groups added a category decision per project and bought nothing that
this documentation does not express better in prose.

Package names are prefixed but otherwise flat: the `xarray` project is
distribution `ocs-stack-xarray`, module `ocs_stack_xarray`. The prefix is
load-bearing — a project studying `polars` cannot have a module named `polars`
while depending on it.

## Per-project template

Projects follow the [chapkit](https://github.com/dhis2-chap/chapkit) template:

- Python 3.13, `uv` with the `uv_build` backend, src layout
- ruff at 120 columns, Google docstring convention
- mypy and pyright, both in strict mode, both required to pass
- pytest with coverage; tests exempt from docstring rules
- a `Makefile` with `install`, `lint`, `test`, `coverage`, `run EXAMPLE=<name>`,
  `run-all`, `clean`, and `ci` (lint + test)
- `README.md` and `ROADMAP.md` — the roadmap is the syllabus and is kept
  accurate as examples land

Dependencies are added with `uv add`, never hand-pinned, and breaking API
changes get fixed rather than pinned around. Where a pin is genuinely forced,
the reason goes in a comment beside it.

## Examples

Examples live in `examples/PPNN_topic_name.py`, where `PP` is the phase and
`NN` the number within it. Each is a self-contained lesson:

- a module docstring with **What**, **Why** (tied to what OCS does with it),
  and **Run** lines
- `# SECTION:` banner comments
- printed prose before each demonstration, and printed evidence after it —
  shapes, timings, task counts, error messages
- a `=== Summary ===` block at the end

They must be deterministic, offline, and non-interactive. Anything needing a
real service still has to run without it: `connect()` in `dask-distributed`
probes the cluster, falls back to an in-process substitute, and prints what the
fallback cannot show.

## Projects with infrastructure

A project needing real services adds a `compose.yml` (plus a `Dockerfile` when
the image needs more than an upstream one) and Makefile targets `up`, `down`,
`ps`, `logs`. `up` blocks until the service is genuinely ready rather than
returning on container start.

Where a container and the host both take part, the image is pinned to the
versions in `uv.lock`. A mismatch between a client library and its server is a
whole class of confusing failures.

## Verification

Verification is not a clean type-check:

```bash
make verify PROJECT=xarray   # lint, type-check, test, run every example
make verify-all              # the whole repository
make docs-build              # this site, its links, and the docs standard
make docs-check              # just the docs standard
```

`make docs-check` enforces the rules this page states in prose, because a
documentation lapse is invisible in a way a broken build is not — a page is
never "broken", only quietly thinner than it promises to be. It checks that
every example carries a What/Why/Run docstring, `# SECTION` banners and a
summary; that every example has its own section in the project page of at
least 80 lines; that the page links the source; that the roadmap and the
examples directory agree in both directions; and that the project is wired
into the site at all — a reference stub, both nav entries, and a `paths` entry
for mkdocstrings, since a project can satisfy every other rule and still not
appear. It runs as part of `make docs-build`.

`make docs-build` passes `--strict` to mkdocs, so a warning fails the build.
That matters for one case `check-links.sh` cannot cover: it strips the anchor
before testing a path, so a link to a heading that does not exist resolves as
far as it is concerned. mkdocs validates those anchors, and without `--strict`
it would say so and exit 0 anyway. `make docs-serve` stays lenient, so a
half-written page still reloads.

`make verify` runs every example and reads its output, because compiling proves
it builds, not that it works. Every number quoted in these pages came out of an
example that actually ran; timing-derived figures are marked as
machine-dependent because they do not reproduce exactly.

### Links to source

The project pages link to example and library source with paths relative to
the repository root, because that is how these files are usually read: in an
editor, or as markdown on GitHub. The published site is the one place those
paths cannot work — it contains `docs/` and nothing else, so a relative source
link 404s there.

`scripts/mkdocs_hooks.py` rewrites them to absolute GitHub URLs at build time.
The markdown on disk stays relative, so `scripts/check-links.sh` can keep
checking it against the filesystem, and the built site gets a URL that
resolves. Links that stay inside `docs/` are left alone.

### Continuous integration

Two workflows in `.github/workflows/`:

- `ci.yml` runs on every push and pull request. A `discover` job asks
  `scripts/verify.sh --list` what the projects are and feeds that into a
  matrix, so adding a project cannot leave it silently untested — the same
  reasoning as the site-wiring check above. Each project then runs
  `scripts/verify.sh <project>`: lint, both type checkers, tests, and every
  example. A separate job runs `make docs-build`.
- `docs.yml` publishes the site to GitHub Pages on every push to `main`. It
  builds with `make docs-build`, the same target CI runs, so a page that fails
  the documentation standard is never published.

The runners have no Docker daemon, which is a supported way to run this: every
example that wants a real service falls back and prints what the fallback
cannot show.

## Documentation

Documentation is centralised in `docs/` at the repository root and built as one
mkdocs site. Projects keep their own `README.md` and `ROADMAP.md` — those are
working files and render fine on their own — but there is a single site, a
single navigation, and a single search.

The API reference is generated by mkdocstrings, which reads the source
statically through griffe. That means the docs build needs none of the
projects' dependencies installed: `mkdocs.yml` simply points `paths` at each
project's `src/` directory, and `make docs-build` runs the whole thing through
`uvx` without a root virtualenv.

### Reading it offline

The site is the right shape at a desk and the wrong shape on a phone with no
signal. `make offline` produces the other shape:

- `dist/ocs-stack.html` — every page in nav order in one self-contained
  file. No assets, no network, no sidebar; cross-page links become anchors
  within the document, and links to source outside `docs/` become the path in
  monospace, since a standalone file cannot follow them.
- `dist/ocs-stack.pdf` — the same file printed by headless Chrome, about
  450 A4 pages.

`scripts/build-book.py` reads the nav out of `mkdocs.yml` rather than keeping
its own list, so a page added to the site is in the book without a second edit.
It uses the same markdown extensions, minus the mermaid fence, which has no
renderer in a standalone file.

`make share` serves the built site over Tailscale for reading on another
device with search intact. It binds to this machine's tailnet address rather
than `0.0.0.0`, so the local network cannot reach it, and it does not use
`tailscale funnel`, which would publish it to the internet.

## Working rules

- **Never revert, always fix forward.** Fix the root cause rather than removing
  functionality.
- **Always test the unhappy path.** Invalid input must raise a clear error, and
  tests must cover it.
- **No emojis** anywhere: code, comments, docs, commit messages, or output.
- **Conventional commits**, scoped to the project directory, with no
  attribution or co-authored-by lines.
