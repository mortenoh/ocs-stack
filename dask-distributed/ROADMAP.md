# dask-distributed roadmap

Learning `dask.distributed` against a real multi-container cluster. The
previous project (`data/dask`) covered graphs and the local schedulers; this
one covers what only appears once work crosses a network: serialization,
data locality, worker memory, failure, and observability.

Why it matters for [open-climate-service](https://github.com/dhis2/open-climate-service):
an openEO process graph submitted to a deployed instance runs on workers that
do not share the API process's memory or filesystem. Every lesson here is a
constraint that shapes how such a service is deployed.

The cluster is defined in `compose.yml` (scheduler + 3 workers + shared
volume) and built from `Dockerfile`. Helpers live in
`src/playground_dask_distributed/`: `connect()` returns a session against the
Compose cluster, or an in-process fallback with a printed note.

## Phase 1 — Connecting to a real cluster

- [x] `0101_connect` — connect over TCP; scheduler_info, worker inventory, dashboard URL
- [x] `0102_versions` — `client.get_versions()`; why client and workers must match
- [x] `0103_submit_across_workers` — submit tasks, see which container ran each one

## Phase 2 — Moving data across the wire

Everything sent to a worker is serialized. This phase makes the cost visible.

- [x] `0201_serialization` — what crosses the wire; cheap vs expensive payloads
- [x] `0202_scatter_gather` — `client.scatter` to publish data once instead of per task
- [x] `0203_locality` — `who_has`/`has_what`; moving compute to data, not data to compute

## Phase 3 — Worker memory and shared storage

- [x] `0301_worker_memory` — memory limits, the spill-to-disk thresholds, watching usage
- [x] `0302_shared_storage` — the container path is not the client path; writing zarr through workers
- [x] `0303_distributed_xarray` — why a lazy zarr graph needs one resolvable path, and the two shapes that work

## Phase 4 — Failure and elasticity

The half a LocalCluster cannot teach.

- [x] `0401_worker_failure` — kill a worker mid-computation; watch the work get reassigned
- [x] `0402_errors_and_retries` — exceptions on workers, tracebacks on the client, `retries=`
- [x] `0403_scaling` — what capacity buys, measured; graceful retirement versus killing

## Phase 5 — Observability

- [x] `0501_dashboard_tour` — what each dashboard panel means, read programmatically
- [x] `0502_performance_report` — `performance_report()` to a shareable HTML file
- [x] `0503_task_stream` — task stream and per-worker profiles; finding the bottleneck
