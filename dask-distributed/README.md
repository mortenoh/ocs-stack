# dask-distributed

Learning `dask.distributed` against a **real cluster** — a scheduler and three
worker containers run with Docker Compose, driven by examples from the host.

This is the project where dask stops being a library and becomes a system.
Everything the local schedulers hide shows up here: work crosses process and
network boundaries, so arguments must serialize, file paths must be valid on
the worker rather than the client, workers die and tasks get retried, and the
cluster's environment must match the client's. See `ROADMAP.md` for the
syllabus.

## Cluster

```bash
make up          # build images, start scheduler + 3 workers, wait for ready
make ps          # container status
make dashboard   # open http://127.0.0.1:8787/status
make scale N=5   # change worker count while running
make logs        # follow scheduler and worker logs
make down        # stop and remove the volume
```

Examples connect to `tcp://127.0.0.1:8786` (override with
`DASK_SCHEDULER_ADDRESS`). **They run without Docker too**: when no scheduler
answers, each example falls back to an in-process `LocalCluster` and prints a
note saying so, so `make run-all` works on any machine. The distributed
lessons are far more vivid against the real cluster.

## Usage

```bash
make install                  # uv sync
make run EXAMPLE=0101_connect
make run-all                  # run every example
make lint test                # ruff + mypy + pyright, pytest
```

## Version matching

`Dockerfile` pins the same dask, distributed, and Python versions the client
uses. Mismatched versions between client and workers are the most common cause
of confusing distributed failures — bump the image tag and the `pyproject.toml`
floors together.

Full documentation: [`docs/projects/dask-distributed.md`](../docs/projects/dask-distributed.md)
(`make docs-serve` at the repository root renders the whole site).
