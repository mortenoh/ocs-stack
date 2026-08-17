"""Helpers for building chunked demo arrays and describing their graphs.

Used across the examples so every lesson starts from the same OCS-shaped
data: a daily climate field with dims ``(time, y, x)``, chunked the way a
zarr store would be.
"""

from typing import Any

import dask.array as da
import numpy as np


def random_field(
    days: int = 365,
    ny: int = 256,
    nx: int = 256,
    time_chunk: int = 30,
    spatial_chunk: int = 128,
    seed: int = 0,
) -> da.Array:
    """Return a lazy random (time, y, x) field chunked like a climate store.

    Args:
        days: Number of time steps; must be at least 1.
        ny: Grid height; must be at least 1.
        nx: Grid width; must be at least 1.
        time_chunk: Chunk length along time.
        spatial_chunk: Chunk length along y and x.
        seed: Seed for the random state.

    Returns:
        A lazy dask array of float64 values in [0, 1).

    Raises:
        ValueError: If days, ny, or nx is less than 1.
    """
    for name, value in (("days", days), ("ny", ny), ("nx", nx)):
        if value < 1:
            raise ValueError(f"{name} must be at least 1, got {value}")
    rng = da.random.default_rng(np.random.default_rng(seed))
    # dask's stub for random() declares chunks as str only; tuples are valid at runtime.
    chunks: Any = (time_chunk, spatial_chunk, spatial_chunk)
    arr: da.Array = rng.random((days, ny, nx), chunks=chunks)
    return arr


def chunk_report(arr: da.Array) -> str:
    """Return a one-line human-readable description of an array's chunk layout.

    Args:
        arr: Any dask array.

    Returns:
        A summary like ``"shape=(365, 256, 256), chunks=(30, 128, 128),
        n_chunks=52, ~15.0 MB/chunk"`` using the first chunk's size.
    """
    first = tuple(c[0] for c in arr.chunks)
    n_chunks = int(np.prod([len(c) for c in arr.chunks]))
    mb = float(np.prod(first)) * arr.dtype.itemsize / 1e6
    return f"shape={arr.shape}, chunks={first}, n_chunks={n_chunks}, ~{mb:.1f} MB/chunk"


def task_count(obj: Any) -> int:
    """Return the number of tasks in a dask collection's graph.

    Args:
        obj: Any dask collection (array, delayed, dataframe, or xarray object
            backed by dask).

    Returns:
        The number of keys in the object's task graph.
    """
    graph = obj.__dask_graph__()
    return len(dict(graph))
