"""compute vs load vs persist: three ways to turn lazy graphs into real numbers.

What: takes one lazy dask-backed pipeline and materializes it three ways —
.compute() returns an in-memory copy while the original stays lazy, .load()
fills the same object in place, and .persist() keeps the result as concrete
chunks still wrapped in dask (the task count collapses to one task per
chunk). Timing the same reduction twice shows what re-computation costs.

Why: this phase IS the open-climate-service execution model — lazy
zarr-backed xarray on dask. Choosing when to materialize is the memory/CPU
trade-off of that model: compute copies results into RAM, persist trades RAM
for never re-running a graph, and staying lazy re-pays the full compute cost
on every use.

Run: make run EXAMPLE=0503_compute_persist
"""

import time

import numpy as np
import pandas as pd
import xarray as xr

from playground_dask import random_field, task_count


def make_field(days: int = 365) -> xr.DataArray:
    """Return a dask-backed (time, y, x) DataArray shaped like an OCS store.

    Args:
        days: Number of daily time steps.

    Returns:
        A lazy DataArray named ``t2m`` with a datetime time coordinate.
    """
    arr = random_field(days=days)
    return xr.DataArray(
        arr,
        dims=("time", "y", "x"),
        coords={
            "time": pd.date_range("2023-01-01", periods=days, freq="D"),
            "y": np.arange(256, dtype=np.float64),
            "x": np.arange(256, dtype=np.float64),
        },
        name="t2m",
        attrs={"units": "degC", "long_name": "2 metre temperature"},
    )


def time_reduction(da: xr.DataArray) -> float:
    """Compute the grand mean of a DataArray and return the elapsed seconds.

    Args:
        da: Any DataArray; lazy inputs pay their full graph cost here.

    Returns:
        Wall-clock seconds spent inside .compute().
    """
    t0 = time.perf_counter()
    da.mean().compute()
    return time.perf_counter() - t0


def main() -> None:
    """Materialize one lazy pipeline via compute, load, and persist, and compare costs."""
    # SECTION: a lazy pipeline to materialize
    print("One lazy pipeline, three ways to materialize it. The pipeline: a standardized")
    print("daily anomaly ((field - time mean) / time std), never computed so far:")
    da = make_field()
    anomaly = ((da - da.mean(dim="time")) / da.std(dim="time")).rename("t2m_anomaly")
    kind = type(anomaly.data).__name__
    print(f"  anomaly: {task_count(anomaly)} tasks, .data is {kind}, {anomaly.nbytes / 1e6:.0f} MB when real")

    # SECTION: .compute() returns a copy
    print("\n.compute() -- returns a NEW in-memory object; the original stays lazy:")
    t0 = time.perf_counter()
    computed = anomaly.compute()
    print(f"  compute took {time.perf_counter() - t0:.2f} s")
    print(f"  computed.data is {type(computed.data).__name__}  (numpy, {computed.nbytes / 1e6:.0f} MB now in RAM)")
    print(f"  anomaly.data is still {type(anomaly.data).__name__} with {task_count(anomaly)} tasks -- untouched")
    del computed  # release the 191 MB copy before the next demos

    # SECTION: .load() fills in place
    print("\n.load() -- same materialization, but it REPLACES the data on the object itself:")
    loaded = (make_field() - 0.5).rename("t2m_shifted")
    print(f"  before: loaded.data is {type(loaded.data).__name__}")
    loaded.load()
    print(f"  after:  loaded.data is {type(loaded.data).__name__}  (no new object; the graph is gone for good)")
    del loaded

    # SECTION: .persist() keeps dask, drops the graph
    print("\n.persist() -- runs the graph, keeps results as concrete chunks STILL wrapped in dask:")
    persisted = anomaly.persist()
    print(f"  persisted.data is {type(persisted.data).__name__}  (still dask: chunked API intact)")
    print(f"  tasks: {task_count(anomaly)} -> {task_count(persisted)}  (collapsed to one task per finished chunk)")
    print(f"  chunk layout preserved: time chunks = {persisted.chunksizes['time']}")

    # SECTION: memory consequences
    print("\nMemory consequences -- laziness is free, materialization is not:")
    print(f"  lazy anomaly:  ~0 MB held (only a {task_count(anomaly)}-task recipe)")
    print(f"  persisted:     {persisted.nbytes / 1e6:.0f} MB of real chunks pinned in RAM until it is dropped")
    print("  compute/load: same footprint, but as one numpy block instead of dask chunks")

    # SECTION: the cost of re-computation
    print("\nRe-computation cost -- the same reduction, twice, off lazy vs persisted:")
    lazy_first = time_reduction(anomaly)
    lazy_second = time_reduction(anomaly)
    print(f"  lazy:      1st {lazy_first * 1e3:7.1f} ms, 2nd {lazy_second * 1e3:7.1f} ms  (graph re-runs each time)")
    persisted_first = time_reduction(persisted)
    persisted_second = time_reduction(persisted)
    print(f"  persisted: 1st {persisted_first * 1e3:7.1f} ms, 2nd {persisted_second * 1e3:7.1f} ms  (already real)")
    speedup = lazy_second / max(persisted_second, 1e-9)
    print(f"  persisted repeat is ~{speedup:.0f}x faster: you paid once in RAM to stop paying in CPU")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- .compute(): new numpy-backed copy; the lazy original is untouched")
    print("- .load(): materializes in place; the object itself stops being lazy")
    print("- .persist(): concrete chunks behind a dask facade; task count collapses to n_chunks")
    print("- lazy = zero memory but full re-compute per use; persist = RAM traded for reuse")
    print("- persist when a result is reused many times and fits in memory -- else stay lazy")


if __name__ == "__main__":
    main()
