"""Dask-backed xarray: labeled arrays whose data is a lazy chunked dask array.

What: builds an xr.DataArray and Dataset on top of a chunked dask array,
shows that .data is a dask array, that the repr and ds.chunks expose the
chunk layout, that operations stay lazy, and contrasts all of this with a
numpy-backed equivalent where .values and every operation are eager.

Why: this phase IS the open-climate-service execution model — lazy
zarr-backed xarray on dask. OCS never holds a full (time, y, x) cube in
memory; every variable is a dask array wearing xarray labels, and nothing
touches real bytes until something forces a compute.

Run: make run EXAMPLE=0501_chunked_xarray
"""

import time

import numpy as np
import pandas as pd
import xarray as xr
from dask.array import Array as DaskArray

from playground_dask import chunk_report, random_field, task_count


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
    """Build a dask-backed DataArray/Dataset and contrast it with a numpy-backed twin."""
    # SECTION: a DataArray wrapping a dask array
    print("A DataArray built from a dask array keeps the dask array as its .data:")
    da_lazy = make_field()
    print(f"  underlying array: {chunk_report(da_lazy.data)}")
    print(f"  type(da_lazy.data) = {type(da_lazy.data).__name__}  (dask, not numpy)")
    print(f"  is a dask Array?  {isinstance(da_lazy.data, DaskArray)}")
    print(f"  graph size: {task_count(da_lazy)} tasks -- one per chunk, no values exist yet")

    # SECTION: the repr shows chunks
    print("\nThe repr advertises the laziness -- 'dask.array<...>' plus the chunk layout:")
    repr_lines = repr(da_lazy).splitlines()
    for line in repr_lines[:3]:
        print(f"  | {line}")
    print("  | ...")

    # SECTION: Dataset-level chunk view
    print("\nAs a Dataset, ds.chunks maps each dim to its chunk sizes:")
    ds = da_lazy.to_dataset()
    for dim, sizes in dict(ds.chunks).items():
        print(f"  {dim}: {sizes}")
    print("  (time splits 365 days into 12 x 30 + 5; y and x split 256 into 2 x 128)")

    # SECTION: operations stay lazy
    print("\nOperations return new lazy objects -- graphs grow, nothing computes:")
    t0 = time.perf_counter()
    monthly_map = da_lazy.mean(dim="time")
    anomaly = da_lazy - monthly_map
    built_ms = (time.perf_counter() - t0) * 1e3
    print(f"  da.mean(dim='time')  -> {task_count(monthly_map)} tasks, .data is {type(monthly_map.data).__name__}")
    print(f"  da - mean            -> {task_count(anomaly)} tasks, .data is {type(anomaly.data).__name__}")
    print(f"  building both graphs took {built_ms:.1f} ms -- graph construction, not number crunching")

    # SECTION: the numpy-backed contrast
    print("\nContrast: .values forces a compute, and the result is a numpy-backed twin:")
    t0 = time.perf_counter()
    values = da_lazy.values  # materializes every chunk
    compute_s = time.perf_counter() - t0
    mb = values.nbytes / 1e6
    print(f"  da_lazy.values ran the whole graph in {compute_s:.2f} s -> {type(values).__name__}, {mb:.0f} MB")
    da_eager = xr.DataArray(values, dims=da_lazy.dims, coords=da_lazy.coords, name="t2m")
    print(f"  type(da_eager.data) = {type(da_eager.data).__name__}  (numpy: values already in memory)")
    t0 = time.perf_counter()
    eager_mean = da_eager.mean(dim="time")
    eager_ms = (time.perf_counter() - t0) * 1e3
    print(f"  da_eager.mean(dim='time') executed immediately ({eager_ms:.1f} ms) -> {type(eager_mean.data).__name__}")
    print("  same xarray API, opposite execution model: numpy runs now, dask runs later")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- xarray wraps whatever array you give it; give it dask and every op is lazy")
    print("- the repr shows 'dask.array<...>' and the chunk layout instead of values")
    print("- ds.chunks reports per-dimension chunk sizes across the whole Dataset")
    print("- mean/arithmetic on a dask-backed object just grow the task graph")
    print("- .values (or .compute()) is the moment real work happens")
    print("- a numpy-backed twin has the same labels but eager execution")


if __name__ == "__main__":
    main()
