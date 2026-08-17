"""End-to-end lazy zarr pipeline: write, open lazily, subset, derive, write again.

What: writes a chunked (time, y, x) dataset to a zarr store, reopens it
lazily, selects a spatial window and a time slice, derives a monthly anomaly,
and writes the result to a second zarr store — with task counts, timings, and
on-disk sizes as evidence that only the needed chunks ever materialize.

Why: this phase IS the open-climate-service execution model — lazy
zarr-backed xarray on dask. An OCS request is exactly this pipeline: open a
normalized (time, y, x) zarr store, slice to the requested window, run the
process graph, write the result — and the whole point is that a small request
never pays for the whole store.

Run: make run EXAMPLE=0504_lazy_zarr_pipeline
"""

import tempfile
import time
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from playground_data_dask import random_field, task_count


def make_dataset(days: int = 365) -> xr.Dataset:
    """Return a dask-backed (time, y, x) Dataset shaped like an OCS store.

    Args:
        days: Number of daily time steps.

    Returns:
        A lazy Dataset with one variable ``t2m`` and a datetime time coordinate.
    """
    arr = random_field(days=days)
    da = xr.DataArray(
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
    return da.to_dataset()


def store_size_mb(store: Path) -> float:
    """Return the total size of all files in a zarr store directory, in MB.

    Args:
        store: Path to the root of a zarr store on disk.

    Returns:
        Sum of file sizes under the store, in megabytes.
    """
    return sum(p.stat().st_size for p in store.rglob("*") if p.is_file()) / 1e6


def monthly_anomaly(da: xr.DataArray) -> xr.DataArray:
    """Return each day's deviation from its month's mean, as a lazy graph.

    Args:
        da: A daily (time, y, x) DataArray, dask-backed.

    Returns:
        A lazy DataArray of daily anomalies relative to the monthly climatology.
    """
    climatology = da.groupby("time.month").mean()
    anom = da.groupby("time.month") - climatology
    return anom.rename("t2m_anomaly")


def main() -> None:
    """Run the write -> open lazily -> subset -> derive -> write pipeline with evidence."""
    with tempfile.TemporaryDirectory() as tmp:
        source_store = Path(tmp) / "source.zarr"
        anomaly_store = Path(tmp) / "anomaly.zarr"

        # SECTION: write the source store
        print("Step 1 -- write a chunked year of daily data to zarr:")
        print("  (consolidated=False on every zarr call: consolidated metadata is not part of")
        print("   the zarr v3 spec yet, and writing it triggers a ZarrUserWarning)")
        ds = make_dataset()
        t0 = time.perf_counter()
        ds.to_zarr(source_store, consolidated=False)
        write_s = time.perf_counter() - t0
        print(f"  wrote {ds.t2m.shape} in chunks of {tuple(c[0] for c in ds.t2m.chunks)} in {write_s:.2f} s")

        # SECTION: open lazily
        print("\nStep 2 -- open_zarr is lazy: metadata only, one task per stored chunk:")
        opened = xr.open_zarr(source_store, consolidated=False)
        t2m = opened.t2m
        print(f"  chunks on disk -> chunks in dask: time={opened.chunks['time']}")
        print(f"  graph: {task_count(t2m)} tasks, .data is {type(t2m.data).__name__}, nothing read yet")

        # SECTION: subset before computing
        print("\nStep 3 -- select a spatial window and a time slice (still lazy):")
        window = t2m.sel(y=slice(0.0, 127.0), x=slice(0.0, 127.0), time=slice("2023-06-01", "2023-08-31"))
        print(f"  window: shape={window.shape}  (92 of 365 days, 1 of 4 spatial blocks)")
        print(f"  time chunks inside the window: {window.chunksizes['time']}")

        # SECTION: derive a monthly anomaly
        print("\nStep 4 -- derive the monthly anomaly; compare the graph against the full domain:")
        window_anom = monthly_anomaly(window)
        full_anom = monthly_anomaly(t2m)
        print(f"  windowed anomaly graph: {task_count(window_anom)} tasks")
        print(f"  full-domain equivalent: {task_count(full_anom)} tasks")
        t0 = time.perf_counter()
        window_result = window_anom.compute()
        window_s = time.perf_counter() - t0
        t0 = time.perf_counter()
        full_anom.compute()
        full_s = time.perf_counter() - t0
        ratio = full_s / max(window_s, 1e-9)
        print(f"  compute windowed: {window_s:.2f} s   compute full domain: {full_s:.2f} s  (~{ratio:.0f}x)")
        print("  the lazy pipeline reads only the stored chunks the window overlaps -- the")
        print("  rest of the store is never touched (full domain shown only for contrast)")

        # SECTION: write the result store
        print("\nStep 5 -- write the derived result to a second zarr store:")
        print("  (groupby arithmetic fragmented time into 92 single-day chunks; rechunk to one")
        print("   clean block so the output store is not 92 tiny objects)")
        t0 = time.perf_counter()
        window_anom.chunk({"time": 92}).to_dataset().to_zarr(anomaly_store, consolidated=False)
        anom_write_s = time.perf_counter() - t0
        print(f"  wrote {window_anom.shape} in {anom_write_s:.2f} s")

        # SECTION: bytes on disk
        print("\nEvidence on disk -- the result store holds only the derived window:")
        src_mb = store_size_mb(source_store)
        anom_mb = store_size_mb(anomaly_store)
        print(f"  source store:  {src_mb:7.1f} MB  {ds.t2m.shape}")
        print(f"  anomaly store: {anom_mb:7.1f} MB  {window_anom.shape}")
        mean_val = float(window_result.mean())
        print(f"  sanity check: window anomaly mean = {mean_val:+.6f} (deviations cancel by construction)")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- to_zarr writes each dask chunk as a zarr chunk; open_zarr rebuilds the lazy view")
    print("- sel() on the lazy view narrows the graph before any byte is read")
    print("- the derived graph over a window is far smaller and faster than the full domain")
    print("- only chunks the window overlaps are ever read; disk sizes show the result is tiny")
    print("- this write -> open -> slice -> derive -> write chain is the OCS request path")


if __name__ == "__main__":
    main()
