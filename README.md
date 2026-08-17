# playground-python

Self-contained tutorial projects, one per library. Every project builds and
runs on its own — its own `pyproject.toml`, `.venv`, `uv.lock`, Makefile, and
examples. There is no root package and no uv workspace.

The current set exists to learn the stack behind
[open-climate-service](https://github.com/dhis2/open-climate-service) from the
bottom up, so that extending it — S3-backed icechunk, a distributed dask
deployment — means applying things already understood rather than learning
them under pressure.

| Project | Examples | Upstream | What it covers |
|---|---:|---|---|
| [`xarray`](xarray/) | 25 | [xarray](https://docs.xarray.dev/) | Labeled N-dimensional arrays: the data model through to lazy evaluation |
| [`dask`](dask/) | 22 | [dask](https://docs.dask.org/) | Task graphs, blocked algorithms, schedulers, and chunking in practice |
| [`dask-distributed`](dask-distributed/) | 15 | [dask.distributed](https://distributed.dask.org/) | A real cluster: scheduler and workers in containers, driven from the host |
| [`icechunk`](icechunk/) | 14 | [icechunk](https://icechunk.io/) | Versioned, transactional Zarr v3 storage |
| [`climate-pipeline`](climate-pipeline/) | 10 | [open-climate-service](https://github.com/dhis2/open-climate-service) | The capstone: a miniature climate service, end to end |

86 examples in total, each a self-contained lesson that prints its own
explanation as it runs.

```bash
make list                     # every project
make verify PROJECT=xarray    # lint, type-check, test, run every example
make verify-all               # the whole repository
make docs-serve               # the documentation site
```

Inside a project: `make install`, `make lint`, `make test`, `make run-all`,
`make run EXAMPLE=<name>`.

## Documentation

The written documentation lives in [`docs/`](docs/) and reads fine as plain
markdown; `make docs-serve` renders it as one searchable site with the API
reference.

- **[Overview](docs/index.md)** — what is here and where to start
- **[The stack](docs/stack.md)** — what each layer does, and where it stops
- **[Scaling](docs/scaling.md)** — the ceilings, and which one you are hitting
- **[Storage](docs/storage.md)** — local filesystem versus object storage, in depth
- **[Open Climate Service](docs/open-climate-service.md)** — how this maps onto
  OCS, and the groundwork for the planned work
- **[Conventions](docs/conventions.md)** — how projects are built and verified

New here? Read [the stack](docs/stack.md), then run
`make run EXAMPLE=0401_full_pipeline` in [`climate-pipeline/`](climate-pipeline/) to see every
layer working together in about a second.
