"""Lazy graphs: chained operations build task graphs; compute vs load vs persist.

What: chains operations on a chunked dataset (anomaly, then monthly means) and
watches the dask task graph grow while wall time stays near zero, then triggers
computation and contrasts .compute(), .load(), and .persist().

Why: open-climate-service composes whole pipelines this way — anomalies,
climatologies, resampling — as pure graph building. Nothing runs until a write
or an explicit compute, so intermediate datasets are free to create and pass
around. Knowing which trigger materializes what (and where) is the core skill.

Run: make run EXAMPLE=0602_lazy_graphs
"""

import time

import xarray as xr

from playground_data_xarray import temperature_dataset


def task_count(obj: xr.DataArray | xr.Dataset) -> int:
    """Return the number of tasks in the object's dask graph (0 if numpy-backed)."""
    graph = obj.__dask_graph__()
    return 0 if graph is None else len(dict(graph))


def backing(obj: xr.DataArray) -> str:
    """Return the fully qualified type name of the array backing a DataArray."""
    return f"{type(obj.data).__module__}.{type(obj.data).__name__}"


def main() -> None:
    """Build a lazy pipeline, inspect its graph, then materialize it three ways."""
    # SECTION: a chunked starting point
    print("One year of daily temperature, chunked into (30, 64, 64) blocks:")
    ds = temperature_dataset(days=365, ny=128, nx=128).chunk({"time": 30, "y": 64, "x": 64})
    t2m = ds.t2m
    print(f"  chunks: {dict(ds.chunks)}")
    print(f"  tasks in the graph so far: {task_count(t2m)} (one task per block)")

    # SECTION: chaining operations grows the graph, not the memory
    print("\nEach operation appends tasks to the graph; wall time stays near zero:")
    t0 = time.perf_counter()
    climatology = t2m.mean("time")
    anomaly = t2m - climatology
    built_anomaly = time.perf_counter() - t0
    print(f"  anomaly = t2m - t2m.mean('time')      -> {task_count(anomaly):4d} tasks, {built_anomaly * 1000:.1f} ms")

    t0 = time.perf_counter()
    monthly = anomaly.resample(time="MS").mean()
    built_monthly = time.perf_counter() - t0
    print(f"  monthly = anomaly.resample('MS').mean -> {task_count(monthly):4d} tasks, {built_monthly * 1000:.1f} ms")
    print(f"  monthly is still lazy: backing = {backing(monthly)}, sizes = {dict(monthly.sizes)}")

    # SECTION: .compute() returns an in-memory copy
    print("\n.compute() runs the graph and returns a NEW numpy-backed object:")
    t0 = time.perf_counter()
    result = monthly.compute()
    compute_s = time.perf_counter() - t0
    print(f"  ran {task_count(monthly)} tasks in {compute_s:.3f} s on the threaded scheduler")
    print(f"  result backing:  {backing(result)}")
    print(f"  monthly backing: {backing(monthly)}  (the original is STILL lazy)")
    print(f"  January mean anomaly at cell (0, 0): {float(result[0, 0, 0]):+.3f} degC")

    # SECTION: .load() fills the same object in place
    print("\n.load() computes too, but mutates the object it is called on:")
    obj = monthly.copy()
    returned = obj.load()
    print(f"  returned is obj: {returned is obj}  (load returns self)")
    print(f"  obj backing after load: {backing(obj)}  (dask replaced by numpy in place)")

    # SECTION: .persist() computes but stays dask-backed
    print("\n.persist() runs the graph and keeps results in memory AS dask chunks:")
    persisted = monthly.persist()
    print(f"  persisted backing: {backing(persisted)}  (still dask)")
    print(f"  tasks: {task_count(monthly)} before persist -> {task_count(persisted)} after")
    print("  (only the materialized blocks remain; the whole recipe collapsed)")
    t0 = time.perf_counter()
    persisted.compute()
    recompute_s = time.perf_counter() - t0
    print(f"  computing the persisted object: {recompute_s * 1000:.1f} ms vs {compute_s * 1000:.1f} ms from scratch")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- operations on dask-backed xarray append tasks to a graph; nothing runs")
    print("- graph size grows with each step while wall time stays near zero")
    print("- .compute() -> new in-memory copy; the lazy original is untouched")
    print("- .load()    -> same object, filled in place (returns self)")
    print("- .persist() -> results held in memory, but the object stays dask-backed")
    print("- OCS builds whole pipelines lazily and pays only at write/compute time")


if __name__ == "__main__":
    main()
