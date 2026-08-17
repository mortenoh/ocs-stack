"""A climate pipeline on the cluster, and where the store has to live.

What: first shows why a lazy zarr pipeline cannot be driven from the client
when the store lives on a volume only the containers can see, then runs the
pipeline the two ways that do work -- pushing the whole job to a worker, and
computing a cluster-wide graph over data that needs no shared path.

Why: this is the design decision behind open-climate-service's storage layer.
A dask graph carries ONE path string, used by client and workers alike, so the
store must resolve identically everywhere. That is why production deployments
put zarr in object storage with a URL both sides share, rather than on a disk
that happens to be mounted somewhere.

Run: make run EXAMPLE=0303_distributed_xarray
"""

import os
import shutil
import socket
import tempfile
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import xarray as xr

from playground_data_dask_distributed import connect

SHARED_DIR = "/data"
DAYS = 365
GRID = 256
TIME_CHUNK = 30
SPACE_CHUNK = 128


def build_dataset(days: int, grid: int, seed: int = 0) -> xr.Dataset:
    """Build a climate-shaped (time, y, x) dataset in memory.

    Args:
        days: Number of daily time steps.
        grid: Grid height and width.
        seed: Seed for the noise term.

    Returns:
        A dataset with one variable, ``t2m``, in degrees Celsius.
    """
    rng = np.random.default_rng(seed)
    gradient = 26.0 + np.linspace(2.0, -2.0, grid).reshape(1, grid, 1)
    season = 3.0 * np.sin(2 * np.pi * np.arange(days) / 365.25).reshape(days, 1, 1)
    values = gradient + season + rng.normal(0.0, 0.8, size=(days, grid, grid))
    return xr.DataArray(
        values,
        dims=("time", "y", "x"),
        coords={"time": pd.date_range("2024-01-01", periods=days, freq="D")},
        name="t2m",
        attrs={"units": "degC", "long_name": "2 metre temperature"},
    ).to_dataset()


def run_pipeline_on_worker(base: str, days: int, grid: int, time_chunk: int, space_chunk: int) -> dict[str, Any]:
    """Run the whole ingest-and-derive pipeline inside one worker.

    Everything -- writing the source store, reopening it lazily, deriving the
    climatology, writing the result -- happens on the worker's own filesystem,
    so no path ever has to be valid anywhere else.

    Args:
        base: Directory the worker can write to.
        days: Number of daily time steps.
        grid: Grid height and width.
        time_chunk: Chunk length along time.
        space_chunk: Chunk length along y and x.

    Returns:
        A dict of measurements for the client to print.
    """
    source = f"{base}/source.zarr"
    result = f"{base}/anomaly.zarr"
    shutil.rmtree(source, ignore_errors=True)
    shutil.rmtree(result, ignore_errors=True)

    ds = build_dataset(days, grid).chunk({"time": time_chunk, "y": space_chunk, "x": space_chunk})
    # consolidated=False: consolidated metadata is not part of the zarr v3 spec
    # and writing it emits a ZarrUserWarning.
    ds.to_zarr(source, mode="w", consolidated=False)

    opened = xr.open_zarr(source, consolidated=False, chunks={})  # pyright: ignore[reportArgumentType]
    climatology = opened["t2m"].groupby("time.month").mean()
    anomaly = (opened["t2m"].groupby("time.month") - climatology).chunk({"time": time_chunk})
    anomaly.to_dataset(name="t2m_anomaly").to_zarr(result, mode="w", consolidated=False)

    source_mb = sum(f.stat().st_size for f in Path(source).rglob("*") if f.is_file()) / 1e6
    result_mb = sum(f.stat().st_size for f in Path(result).rglob("*") if f.is_file()) / 1e6
    measurements = {
        "host": socket.gethostname(),
        "source_mb": source_mb,
        "result_mb": result_mb,
        "anomaly_mean": float(anomaly.mean().compute()),
        "climatology_shape": tuple(climatology.shape),
    }
    shutil.rmtree(source, ignore_errors=True)
    shutil.rmtree(result, ignore_errors=True)
    return measurements


def graph_size(obj: Any) -> int:
    """Return the number of tasks in a dask-backed object's graph.

    Args:
        obj: A dask-backed xarray object.

    Returns:
        The task count.
    """
    return len(dict(obj.__dask_graph__()))


def main() -> None:
    """Show the storage constraint, then run the pipeline two ways that work."""
    with connect() as session:
        client = session.client
        print(session.banner())

        # SECTION: the constraint
        print("\nA dask graph carries ONE path string. Client and workers both use it,")
        print("so it has to mean the same thing on both sides. Watch it fail:")
        if session.is_compose:
            print(f"  the workers can write {SHARED_DIR} (a docker volume), but the client cannot:")
            print(f"    client sees {SHARED_DIR}: {os.path.exists(SHARED_DIR)}")
            print("  so xr.open_zarr('/data/source.zarr') on the CLIENT raises FileNotFoundError,")
            print("  even though every worker could open it happily.")
            print("\n  In production the answer is a URL both sides resolve -- s3://bucket/store.zarr")
            print("  with each side pointing at its own endpoint. That is what open-climate-service")
            print("  does, and why its stores live in object storage rather than on a mounted disk.")
        else:
            print("  (The fallback shares the client's filesystem, so this constraint is invisible.")
            print("   Run 'make up' to see it properly.)")

        # SECTION: option one, push the job to the data
        base = SHARED_DIR if session.is_compose else tempfile.mkdtemp(prefix="dask-fallback-")
        print(f"\nOption 1 -- send the whole pipeline to a worker, using {base}:")
        started = time.perf_counter()
        stats = client.submit(run_pipeline_on_worker, base, DAYS, GRID, TIME_CHUNK, SPACE_CHUNK).result()
        elapsed = time.perf_counter() - started
        print(f"  worker {stats['host']} ran write -> open -> climatology -> write in {elapsed:.2f}s")
        print(f"  source {stats['source_mb']:.1f} MB, anomaly {stats['result_mb']:.1f} MB")
        print(f"  climatology shape {stats['climatology_shape']}, anomaly mean {stats['anomaly_mean']:+.6f}")
        print("  One worker did everything, so nothing needed a shared path -- but only")
        print("  one worker's cores were used. Fine for a per-dataset ingest job.")

        # SECTION: option two, one graph across the whole cluster
        print("\nOption 2 -- build the graph on the client and let the CLUSTER run it.")
        print("Data generated in the graph itself needs no storage at all:")
        ds = build_dataset(DAYS, GRID).chunk({"time": TIME_CHUNK, "y": SPACE_CHUNK, "x": SPACE_CHUNK})
        climatology = ds["t2m"].groupby("time.month").mean()
        anomaly = ds["t2m"].groupby("time.month") - climatology
        print(f"  the array is {ds['t2m'].nbytes / 1e6:.0f} MB in {ds['t2m'].data.npartitions} chunks")
        print(f"  climatology graph: {graph_size(climatology):>5} tasks")
        print(f"  anomaly graph:     {graph_size(anomaly):>5} tasks")
        print("  Nothing has run yet -- these are just graphs.")

        started = time.perf_counter()
        computed = climatology.compute()
        compute_s = time.perf_counter() - started
        print(f"\n  compute() executed across the cluster in {compute_s:.2f}s")
        print(f"  climatology shape {computed.shape}")
        monthly = " ".join(f"{float(computed.sel(month=m).mean()):.2f}" for m in range(1, 13))
        print(f"  monthly means (degC): {monthly}")
        print(f"  anomaly mean, should be ~0: {float(anomaly.mean().compute()):+.6f}")

        if session.is_compose:
            print("\n  Every chunk of that computation ran inside a worker container, in")
            print("  parallel across all three. The client held only the graph and the result.")

        # SECTION: summary
        print("\n=== Summary ===")
        print("- a dask graph carries one path; it must resolve the same on client and workers")
        print("- a docker volume the host cannot see is not a shared store for lazy pipelines")
        print("- production answer: object storage with a URL both sides resolve")
        print("- pushing a whole job to one worker avoids the problem, at the cost of parallelism")
        print("- a graph over generated or already-distributed data runs cluster-wide with no store")


if __name__ == "__main__":
    main()
