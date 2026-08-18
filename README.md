# ocs-stack

**Documentation: <https://mortenoh.github.io/ocs-stack/>**

A companion piece to
[open-climate-service](https://github.com/dhis2/open-climate-service), and a
testing ground for it. OCS is the real service; this is where its stack gets
taken apart layer by layer, and where a change can be tried on something small
before it is tried on something that matters.

The layers are the ones a climate data service is built from — labeled arrays,
then parallel execution, then a cluster, then versioned storage — one
self-contained tutorial project per layer, with a working pipeline on top.
Every project builds and runs on its own: its own `pyproject.toml`, `.venv`,
`uv.lock`, Makefile, and examples. There is no root package and no uv
workspace.

Learning the stack from the bottom up is the point: extending OCS — S3-backed
icechunk, a distributed dask deployment — should mean applying things already
understood rather than learning them under pressure. The two planned
extensions each have groundwork here and nothing in OCS yet, which is the
shape this repository is meant to have.

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
make offline                  # one HTML file and a PDF, for reading away from a desk
make share                    # serve the site to your own devices over Tailscale
```

Inside a project: `make install`, `make lint`, `make test`, `make run-all`,
`make run EXAMPLE=<name>`.

## Documentation

The written documentation lives in [`docs/`](docs/) and reads fine as plain
markdown. It is published as one searchable site with the API reference at
**<https://mortenoh.github.io/ocs-stack/>**, rebuilt on every push to `main`;
`make docs-serve` renders the same thing locally.

- **[Overview](docs/index.md)** — what is here and where to start
- **[The stack](docs/stack.md)** — what each layer does, and where it stops
- **[Scaling](docs/scaling.md)** — the ceilings, and which one you are hitting
- **[Storage](docs/storage.md)** — local filesystem versus object storage, in depth
- **[Open Climate Service](docs/open-climate-service.md)** — how this maps onto
  OCS, and the groundwork for the planned work
- **[Conventions](docs/conventions.md)** — how projects are built and verified

### Reading it away from a desk

`make offline` renders every page above into `dist/ocs-stack.html` — one
file, no assets, no network, roughly 450 printed pages — and prints it to
`dist/ocs-stack.pdf` with headless Chrome. Copy the HTML to a phone and it
reflows to the screen, keeps its table of contents, and follows the system
light/dark setting. It works with no connection at all, which the site does
not.

`make share` is the other half: it builds the site and serves it bound to this
machine's Tailscale address, so a phone on the same tailnet can read it with
search intact. It binds to the tailnet address rather than `0.0.0.0`, so
nothing on the local network can reach it, and it deliberately does not use
`tailscale funnel`, which would publish the site to the public internet.

New here? Read [the stack](docs/stack.md), then run
`make run EXAMPLE=0401_full_pipeline` in [`climate-pipeline/`](climate-pipeline/) to see every
layer working together in about a second.
