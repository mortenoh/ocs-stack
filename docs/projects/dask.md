# dask

**Task graphs and chunks.** Task graphs, blocked algorithms, schedulers, and the chunking decisions that decide whether a pipeline is fast or hopeless. Covers the local schedulers; the cluster comes next.

```bash
cd dask
make install
make run-all
```

22 examples, in phases:

### Phase 1 — Delayed and task graphs

- `0101_delayed_basics` — `dask.delayed` wraps functions; nothing runs until compute
- `0102_task_graphs` — inspect the graph: keys, dependencies, `__dask_graph__`
- `0103_sharing_intermediates` — one compute for many outputs; common subgraphs run once

### Phase 2 — dask.array

- `0201_chunked_arrays` — chunks, `.chunks`, `.blocks`; chunk count vs chunk size
- `0202_blocked_algorithms` — how reductions split into per-chunk + combine tasks
- `0203_lazy_pipelines` — chained elementwise/reduction ops stay lazy; graph growth
- `0204_rechunking` — `rechunk` cost model; when layouts must change
- `0205_map_blocks_overlap` — `map_blocks`, `map_overlap` for stencil-style work

### Phase 3 — Schedulers and distributed

- `0301_schedulers` — threads vs processes vs sync; GIL and serialization trade-offs
- `0302_local_cluster` — `LocalCluster` + `Client`; the dashboard; workers and memory
- `0303_futures` — `client.submit`/`gather`; eager tasks vs lazy collections
- `0304_diagnostics` — `ProgressBar`, `ResourceProfiler`, `performance_report`

### Phase 4 — dask.dataframe

- `0401_partitions` — partitions are pandas DataFrames; divisions and indexes
- `0402_groupby_shuffle` — cheap map-like ops vs shuffle-requiring ops
- `0403_pandas_boundary` — when to `compute()` down to pandas; small-data anti-pattern

### Phase 5 — Dask-backed xarray

- `0501_chunked_xarray` — `chunks=` at open/creation; the dask repr; `.data` is a dask array
- `0502_graphs_through_xarray` — groupby/resample/coarsen build dask graphs underneath
- `0503_compute_persist` — `.compute()` vs `.persist()` vs `.load()`; memory consequences
- `0504_lazy_zarr_pipeline` — open zarr lazily, transform, write zarr; end-to-end laziness

### Phase 6 — Patterns and pitfalls

- `0601_zarr_legal_chunks` — zarr requires uniform chunks, dask does not; OCS `_uniform_chunks` re-implemented
- `0602_chunk_sizing` — too many tiny chunks vs too few huge ones; the ~100 MB rule of thumb
- `0603_graph_hygiene` — avoid rebuilding graphs in loops; slicing before computing; fuse-friendly code
