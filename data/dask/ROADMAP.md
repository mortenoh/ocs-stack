# dask roadmap

Learning dask from first principles to the patterns
[open-climate-service](https://github.com/dhis2/open-climate-service) (OCS)
depends on. The through-line: dask is a task graph plus a scheduler; arrays,
dataframes, and dask-backed xarray are all front-ends that build graphs.
OCS runs its openEO process graphs on dask (`openeo-processes-dask`) and
writes dask-chunked xarray to zarr — phases 5 and 6 land exactly there.

Shared helpers live in `src/playground_data_dask/`: synthetic chunked arrays
and a chunk-layout reporter used across the examples.

## Phase 1 — Delayed and task graphs

The core mental model: build a graph lazily, execute it later.

- [x] `0101_delayed_basics` — `dask.delayed` wraps functions; nothing runs until compute
- [x] `0102_task_graphs` — inspect the graph: keys, dependencies, `__dask_graph__`
- [x] `0103_sharing_intermediates` — one compute for many outputs; common subgraphs run once

## Phase 2 — dask.array

Blocked numpy: one array, many chunks, one task per chunk operation.

- [x] `0201_chunked_arrays` — chunks, `.chunks`, `.blocks`; chunk count vs chunk size
- [x] `0202_blocked_algorithms` — how reductions split into per-chunk + combine tasks
- [x] `0203_lazy_pipelines` — chained elementwise/reduction ops stay lazy; graph growth
- [x] `0204_rechunking` — `rechunk` cost model; when layouts must change
- [x] `0205_map_blocks_overlap` — `map_blocks`, `map_overlap` for stencil-style work

## Phase 3 — Schedulers and distributed

Same graph, different executors.

- [x] `0301_schedulers` — threads vs processes vs sync; GIL and serialization trade-offs
- [x] `0302_local_cluster` — `LocalCluster` + `Client`; the dashboard; workers and memory
- [x] `0303_futures` — `client.submit`/`gather`; eager tasks vs lazy collections
- [x] `0304_diagnostics` — `ProgressBar`, `ResourceProfiler`, `performance_report`

## Phase 4 — dask.dataframe

Partitioned pandas — the tabular sibling, and where shuffles enter.

- [x] `0401_partitions` — partitions are pandas DataFrames; divisions and indexes
- [x] `0402_groupby_shuffle` — cheap map-like ops vs shuffle-requiring ops
- [x] `0403_pandas_boundary` — when to `compute()` down to pandas; small-data anti-pattern

## Phase 5 — Dask-backed xarray

The bridge to the previous project and to OCS: xarray as a graph builder.

- [x] `0501_chunked_xarray` — `chunks=` at open/creation; the dask repr; `.data` is a dask array
- [x] `0502_graphs_through_xarray` — groupby/resample/coarsen build dask graphs underneath
- [x] `0503_compute_persist` — `.compute()` vs `.persist()` vs `.load()`; memory consequences
- [x] `0504_lazy_zarr_pipeline` — open zarr lazily, transform, write zarr; end-to-end laziness

## Phase 6 — Patterns and pitfalls

The production wisdom OCS encodes.

- [x] `0601_zarr_legal_chunks` — zarr requires uniform chunks, dask does not; OCS `_uniform_chunks` re-implemented
- [x] `0602_chunk_sizing` — too many tiny chunks vs too few huge ones; the ~100 MB rule of thumb
- [x] `0603_graph_hygiene` — avoid rebuilding graphs in loops; slicing before computing; fuse-friendly code
