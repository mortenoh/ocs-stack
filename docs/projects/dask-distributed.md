# dask-distributed

**A real cluster.** A scheduler and three worker containers run with Docker Compose, driven by examples from the host. This is where dask stops being a library and becomes a system: serialization, locality, worker memory, failure, and observability all become visible.

```bash
cd dask-distributed
make install
make run-all
```

15 examples, in phases:

### Phase 1 — Connecting to a real cluster

- `0101_connect` — connect over TCP; scheduler_info, worker inventory, dashboard URL
- `0102_versions` — `client.get_versions()`; why client and workers must match
- `0103_submit_across_workers` — submit tasks, see which container ran each one

### Phase 2 — Moving data across the wire

- `0201_serialization` — what crosses the wire; cheap vs expensive payloads
- `0202_scatter_gather` — `client.scatter` to publish data once instead of per task
- `0203_locality` — `who_has`/`has_what`; moving compute to data, not data to compute

### Phase 3 — Worker memory and shared storage

- `0301_worker_memory` — memory limits, the spill-to-disk thresholds, watching usage
- `0302_shared_storage` — the container path is not the client path; writing zarr through workers
- `0303_distributed_xarray` — why a lazy zarr graph needs one resolvable path, and the two shapes that work

### Phase 4 — Failure and elasticity

- `0401_worker_failure` — kill a worker mid-computation; watch the work get reassigned
- `0402_errors_and_retries` — exceptions on workers, tracebacks on the client, `retries=`
- `0403_scaling` — what capacity buys, measured; graceful retirement versus killing

### Phase 5 — Observability

- `0501_dashboard_tour` — what each dashboard panel means, read programmatically
- `0502_performance_report` — `performance_report()` to a shareable HTML file
- `0503_task_stream` — task stream and per-worker profiles; finding the bottleneck
