# Roadmap

Each project has its own `ROADMAP.md`, which is the syllabus for that library
and is kept accurate as examples land. This file is the repository-level one:
what is open across the collection rather than inside one project.

## Open from the review of 2026-08-17

One item needs a decision that is not mine to make. The three documentation
inconsistencies the review found are fixed, and the directory has been renamed.

- [x] **The directory was still named after the old repository on disk.** It
      is now `ocs-stack`, matching the contents. One thing does record the
      absolute path and has to be rebuilt after any such move: the shebang of
      every console script in a `.venv` (`mypy`, `pyright`, `ruff`, `pytest`).
      `pyvenv.cfg` and `uv.lock` do not, and `uv sync` considers a moved
      environment up to date, so the failure surfaces as
      `Failed to spawn: mypy` rather than as anything about paths. The fix is
      `rm -rf */.venv && uv sync` per project. Note that `uv run python` keeps
      working throughout, so an import check does not catch this and
      `make verify-all` is the test that does.
- [ ] **No git remote is configured.** Every project page links to source with
      repository-relative paths, which resolve in an editor and would resolve
      on GitHub. They have nowhere to resolve to yet.
- [x] **`dask-distributed` had no `Setup` section**, the only project page
      missing one. It now has a real one in the position `CLAUDE.md` requires,
      covering the two-step start, the fallback with the cluster down, and the
      lockfile pin — with the deep Compose walkthrough left where it was.
- [x] **`climate-pipeline` wrote its phase headings as `Phase 1 -- Ingest`.**
      All 26 headings now use the em dash the other four pages use. Prose was
      left alone.
- [x] **`dask` and `icechunk` opened with a hand-maintained contents list.**
      Removed: the site has a sidebar and the offline book builds its own table
      of contents, so both lists were maintained by hand for no reader.

## Known limit of the documentation check

`make docs-check` enforces structure: template conformance, section depth,
source links, roadmap agreement, and that a project reaches the site at all.
`make docs-build` runs mkdocs under `--strict`, so a link to a heading that
does not exist fails the build rather than warning into a passing one. None of
that can tell whether a quoted output block is still true.

Output quoted from examples is covered indirectly — `make verify-all` runs all
86 and their output is read. Output belonging to the pages themselves, in the
introduction and core-concepts sections, is not: those snippets are runnable
but nothing runs them.

- [ ] A harness that extracts every self-contained snippet from the project
      pages, executes it in that project's environment, and diffs the result
      against the quoted block. The August 2026 review verified this surface by
      sample only — one xarray snippet, reproduced byte for byte including
      float values and `nbytes` — which is evidence, not coverage.

## Groundwork laid, work not started

Both extensions are planned for open-climate-service, and the projects here
exist to make them routine rather than exploratory. See
[`docs/open-climate-service.md`](docs/open-climate-service.md) for the full
argument.

### Icechunk on S3

OCS calls `icechunk.local_filesystem_storage` only. A commit is a
compare-and-swap on a branch pointer, so one committer at a time is correct on
a local filesystem — and object storage becomes necessary the moment compute
spans machines or a second writer appears.

- [ ] Add an S3-backed storage example to the `icechunk` project, against
      MinIO in a `compose.yml`, following the `dask-distributed` pattern: the
      example still runs without the service and says what the fallback cannot
      show.
- [ ] Demonstrate the conditional write that a local filesystem cannot
      provide, as the concrete argument for migrating.
- [ ] Measure what an append costs on object storage versus locally, since
      icechunk shares chunks by reference and a rewrite is a full copy.

### Distributed dask against a real store

The cluster exists; what is missing is the two halves meeting.

- [ ] An example where the graph carries an `s3://` URL that resolves
      identically on the client and on every worker — the problem
      `0302_shared_storage.py` demonstrates and no amount of volume-mounting
      solves cleanly.
- [ ] icechunk's fork/merge distributed write: the coordinator forks a session
      per worker, workers write chunks in parallel, the coordinator merges and
      commits once. Many writers, one committer.

## Conventions to hold

- Every example must run without the service it teaches about, and say what
  the fallback cannot show.
- `make verify-all` before claiming anything is done, and `make offline` too
  when the change touched `docs/`.
- Never quote a number that did not come out of a run.
