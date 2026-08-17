"""Xarray API calls as graph builders: groupby, resample, and coarsen on dask.

What: applies groupby("time.month").mean(), resample(time="MS").mean(), and
coarsen(y=2, x=2).mean() to a dask-backed DataArray and watches the task
graph grow after each call — proving that xarray's high-level verbs are
front-ends that compile down to dask tasks, executed only at .compute().

Why: this phase IS the open-climate-service execution model — lazy
zarr-backed xarray on dask. When OCS builds monthly aggregates or downsamples
a pyramid level, it calls exactly these xarray methods; dask receives the
compiled graph and runs it chunk by chunk.

Run: make run EXAMPLE=0502_graphs_through_xarray
"""

import time
from typing import Any

import numpy as np
import pandas as pd
import xarray as xr

from playground_data_dask import random_field, task_count


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


def main() -> None:
    """Show that groupby/resample/coarsen each compile into a bigger dask graph."""
    # SECTION: the lazy starting point
    print("Start from a lazy daily field; its graph is one task per chunk:")
    da = make_field()
    base_tasks = task_count(da)
    print(f"  shape={da.shape}, chunks(time)={da.chunksizes['time']}")
    print(f"  base graph: {base_tasks} tasks")

    # SECTION: groupby builds a graph
    print("\ngroupby('time.month').mean() -- a monthly climatology, still lazy:")
    t0 = time.perf_counter()
    climatology = da.groupby("time.month").mean()
    ms = (time.perf_counter() - t0) * 1e3
    print(f"  result: dims={climatology.dims}, shape={climatology.shape}")
    print(f"  tasks: {base_tasks} -> {task_count(climatology)}  (built in {ms:.1f} ms, zero chunks computed)")

    # SECTION: resample builds a graph
    print("\nresample(time='MS').mean() -- monthly means along the real calendar:")
    t0 = time.perf_counter()
    monthly = da.resample(time="MS").mean()
    ms = (time.perf_counter() - t0) * 1e3
    print(f"  result: dims={monthly.dims}, shape={monthly.shape}")
    print(f"  tasks: {base_tasks} -> {task_count(monthly)}  (built in {ms:.1f} ms)")

    # SECTION: coarsen builds a graph (the OCS pyramid op)
    print("\ncoarsen(y=2, x=2).mean() -- one zoom level up; this is the open-climate-service")
    print("pyramid-level operation, and it too just adds tasks to the dask graph:")
    t0 = time.perf_counter()
    # Coarsen.mean is injected at runtime; mypy's view of DataArrayCoarsen misses it.
    coarsened: Any = da.coarsen(y=2, x=2)
    level_up: xr.DataArray = coarsened.mean()
    ms = (time.perf_counter() - t0) * 1e3
    print(f"  result: shape {da.shape} -> {level_up.shape}  (each output cell averages a 2x2 block)")
    print(f"  tasks: {base_tasks} -> {task_count(level_up)}  (built in {ms:.1f} ms)")

    # SECTION: graphs compose before anything runs
    print("\nGraphs compose -- coarsen the climatology and the graph keeps growing:")
    clim_coarsened: Any = climatology.coarsen(y=2, x=2)  # same runtime-injected .mean as above
    pyramid_clim: xr.DataArray = clim_coarsened.mean()
    print(f"  climatology {task_count(climatology)} tasks -> coarsened climatology {task_count(pyramid_clim)} tasks")
    print("  three API calls deep and still not one chunk of data has been touched")

    # SECTION: compute is the trigger
    print("\n.compute() is the only step that executes:")
    t0 = time.perf_counter()
    result = pyramid_clim.compute()
    s = time.perf_counter() - t0
    print(f"  compute took {s:.2f} s -- far above the ms-scale graph builds, because chunks finally ran")
    print(f"  result: {type(result.data).__name__}-backed, shape={result.shape}, mean={float(result.mean()):.4f}")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- groupby/resample/coarsen on a dask-backed DataArray return lazy results")
    print("- each call grows the task graph; task_count is the visible evidence")
    print("- building a graph costs milliseconds; only .compute() touches actual data")
    print("- coarsen(y=2, x=2).mean() is the OCS pyramid-level downsample, run on dask")
    print("- lazy results chain: xarray compiles the recipe, dask executes it once")


if __name__ == "__main__":
    main()
