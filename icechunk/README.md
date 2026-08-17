# icechunk

Learning [icechunk](https://icechunk.io): versioned, transactional storage for
Zarr v3 -- the layer under every open-climate-service dataset.

Writes are transactions that commit or do not; history is a chain of immutable
snapshots you can read at any point; readers never see a half-written store.
That last property is what makes it safe to append to a dataset that is
simultaneously being served.

## Usage

```bash
make install                       # uv sync
make run EXAMPLE=0101_repo_basics
make run-all                       # run every example
make lint test                     # ruff + mypy + pyright, pytest
```

See `ROADMAP.md` for the syllabus.

Full documentation: [`docs/projects/icechunk.md`](../docs/projects/icechunk.md)
(`make docs-serve` at the repository root renders the whole site).
