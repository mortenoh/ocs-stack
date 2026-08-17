"""Escape hatches: apply_ufunc with dask="parallelized" and DataArray.map_blocks.

What: runs a numpy function (per-cell 90th percentile over time) across dask
chunks with xr.apply_ufunc(dask="parallelized"), then uses DataArray.map_blocks
for block-wise logic that needs the full labeled xarray API, and spells out
when each escape hatch fits.

Why: not every operation exists as a lazy xarray method. When
open-climate-service needs custom per-pixel science (indices, percentiles,
model code), these two hatches run plain numpy/xarray code per chunk while
keeping the whole pipeline lazy and parallel.

Run: make run EXAMPLE=0604_map_blocks_apply_ufunc
"""

import time

import numpy as np
import xarray as xr

from climate_stack_xarray import temperature_dataset


def p90_along_last_axis(values: np.ndarray) -> np.ndarray:
    """Return the 90th percentile along the trailing axis of a numpy array."""
    return np.asarray(np.percentile(values, 90.0, axis=-1))


def standardize_block(block: xr.DataArray) -> xr.DataArray:
    """Standardize one block per cell over its own time axis.

    Safe here only because time is a single chunk, so each block sees the
    full time series for its cells.
    """
    return (block - block.mean("time")) / block.std("time")


def backing(obj: xr.DataArray) -> str:
    """Return the fully qualified type name of the array backing a DataArray."""
    return f"{type(obj.data).__module__}.{type(obj.data).__name__}"


def main() -> None:
    """Apply custom numpy and xarray code per chunk while staying lazy."""
    # SECTION: setup -- core dim in one chunk
    print("A year of daily temperature; time in ONE chunk, space in 64x64 tiles:")
    ds = temperature_dataset(days=365, ny=128, nx=128).chunk({"time": -1, "y": 64, "x": 64})
    t2m = ds.t2m
    print(f"  chunks: {dict(ds.chunks)}")
    print("  (a reduced ('core') dim must live in a single chunk for apply_ufunc)")

    # SECTION: apply_ufunc runs a numpy function per chunk
    print("\nxr.apply_ufunc: wrap a plain numpy function, parallelized over chunks:")
    t0 = time.perf_counter()
    warm = xr.apply_ufunc(
        p90_along_last_axis,
        t2m,
        input_core_dims=[["time"]],
        dask="parallelized",
        output_dtypes=[np.float64],
    )
    built_s = time.perf_counter() - t0
    print(f"  built lazily in {built_s * 1000:.1f} ms: backing = {backing(warm)}, sizes = {dict(warm.sizes)}")
    print("  input_core_dims=[['time']] moved time to the last axis and consumed it")

    t0 = time.perf_counter()
    warm_np = warm.compute()
    compute_s = time.perf_counter() - t0
    ref = np.percentile(temperature_dataset(days=365, ny=128, nx=128).t2m.values, 90.0, axis=0)
    max_diff = float(np.abs(warm_np.values - ref).max())
    print(f"  computed in {compute_s:.3f} s; max |diff| vs eager numpy: {max_diff:.2e}")
    print(f"  sample: 90th-percentile temperature at cell (0, 0) = {float(warm_np[0, 0]):.2f} degC")

    # SECTION: map_blocks hands each block over as a real DataArray
    print("\nDataArray.map_blocks: your function receives a labeled DataArray per block,")
    print("so it can use coords, dim names, the whole xarray API:")
    t0 = time.perf_counter()
    zscore = t2m.map_blocks(standardize_block, template=t2m)
    built_s = time.perf_counter() - t0
    print(f"  built lazily in {built_s * 1000:.1f} ms: backing = {backing(zscore)}")
    print("  (template=t2m declares the output layout, skipping trial inference)")

    t0 = time.perf_counter()
    zscore_np = zscore.compute()
    compute_s = time.perf_counter() - t0
    cell = zscore_np.isel(y=0, x=0)
    print(f"  computed in {compute_s:.3f} s")
    print(f"  per-cell check at (0, 0): mean = {float(cell.mean()):+.2e}, std = {float(cell.std()):.3f}")

    # SECTION: the blockwise pitfall
    print("\nPitfall: each block sees ONLY its block. A reduction over a chunked dim")
    print("inside map_blocks silently computes per-block answers, not global ones.")
    print("Here time was one chunk, so per-block mean/std over time was the real thing.")

    # SECTION: choosing a hatch
    print("\nWhen each escape hatch fits:")
    print("  apply_ufunc + dask='parallelized' -- you have a numpy-signature function")
    print("    (ufunc-like, axis-based); core dims are consumed; fastest, least overhead")
    print("  map_blocks -- your logic needs labels: coords, .sel, groupby, resample")
    print("    per block; xarray-in, xarray-out; slightly more overhead per block")
    print("  neither -- if a native lazy xarray method exists, always prefer it")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- apply_ufunc(dask='parallelized') runs numpy code per chunk, lazily")
    print("- input_core_dims declares which dims the function consumes (single-chunk)")
    print("- map_blocks passes labeled DataArray blocks to arbitrary xarray code")
    print("- both keep the pipeline lazy; results match the eager computation")
    print("- blocks are independent: never reduce over a chunked dim inside them")


if __name__ == "__main__":
    main()
