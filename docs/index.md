# playground-python

Self-contained tutorial projects, one per library. Every project builds and
runs on its own: its own `pyproject.toml`, `.venv`, `uv.lock`, Makefile, and
examples. There is no root package and no uv workspace.

The current set exists for one reason: to learn the stack behind
[open-climate-service](https://github.com/dhis2/open-climate-service) (OCS)
from the bottom up, so that extending it — S3-backed icechunk, a distributed
dask deployment — is a matter of applying things already understood rather
than learning them under pressure.

```bash
make list                     # every project
make verify PROJECT=xarray    # lint, type-check, test, run every example
make verify-all               # the whole repository
make docs-serve               # this site, at http://127.0.0.1:8000
```

Inside a project: `make install`, `make lint`, `make test`, `make run-all`,
`make run EXAMPLE=<name>`.

## The projects

| Project | Examples | What it covers |
|---|---:|---|
| [start](projects/start.md) | 1 | The template itself: uv, src layout, ruff, mypy, pyright, pytest |
| [xarray](projects/xarray.md) | 25 | Labeled N-dimensional arrays: the data model through to lazy evaluation |
| [dask](projects/dask.md) | 22 | Task graphs, blocked algorithms, schedulers, and chunking in practice |
| [dask-distributed](projects/dask-distributed.md) | 15 | A real cluster: scheduler and workers in containers, driven from the host |
| [icechunk](projects/icechunk.md) | 14 | Versioned, transactional Zarr v3 storage |
| [climate](projects/climate.md) | 10 | The capstone: a miniature climate service, end to end |

Each project's `ROADMAP.md` is the syllabus; each example is a self-contained
lesson that prints its own explanation as it runs. 87 examples in total, and
`make verify-all` runs every one of them.

## Where to start

Reading order depends on what you came for.

**To understand the stack from the ground up**, take the layers in order:
xarray → dask → dask-distributed → icechunk, then climate to see them
combined. [The stack](stack.md) walks through exactly that, and within a
project the phases in `ROADMAP.md` build on each other — examples are numbered
`PPNN`, where `PP` is the phase.

**To see the whole thing working first**, run
`make run EXAMPLE=0401_full_pipeline` in `climate/`. It takes about a second
and prints six labelled stages from raw source to published STAC collection.
Then read backwards into whichever stage you want to understand.

**To answer a specific question**, the sharpest examples are:

| Question | Example |
|---|---|
| What does "lazy" actually mean? | `xarray/examples/0602_lazy_graphs.py` |
| How big should chunks be? | `dask/examples/0602_chunk_sizing.py` |
| Why is my cluster not faster? | `dask-distributed/examples/0503_task_stream.py` |
| What breaks when I deploy to a cluster? | `dask-distributed/examples/0302_shared_storage.py` |
| What does a commit protect me from? | `icechunk/examples/0301_atomicity.py` |
| What does history cost to keep? | `icechunk/examples/0501_storage_growth.py` |
| Do I need S3? | [Storage](storage.md) |

## Reading order for the written docs

- **[The stack](stack.md)** — what each layer does, and where it stops.
- **[Scaling](scaling.md)** — the ceilings, and which one you are actually hitting.
- **[Storage](storage.md)** — local filesystem versus object storage, in depth.
- **[Open Climate Service](open-climate-service.md)** — how this maps onto OCS,
  and the groundwork for the planned work.
- **[Conventions](conventions.md)** — how the projects are built and verified.
