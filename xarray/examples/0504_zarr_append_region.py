"""Zarr writes over time: append_dim to extend, region to update in place.

What: writes an initial 30-day period to a zarr store, appends the next 30
days with to_zarr(append_dim="time"), then rewrites one slice in place with a
region= write -- verifying the time axis and values after each step.

Why: this is exactly the streaming-ingestion write pattern in
open-climate-service: each new period is appended to the store's time axis as
it arrives, and corrected or reprocessed periods are rewritten in place with
region writes. The store grows and heals without ever being rewritten whole.

Run: make run EXAMPLE=0504_zarr_append_region
"""

import os
import tempfile

import numpy as np
import xarray as xr

from climate_stack_xarray import temperature_dataset


def count_chunk_files(store: str, var: str) -> int:
    """Count the chunk files written for one variable in a zarr store.

    Args:
        store: Path to the store's root directory.
        var: Name of the array whose chunks are counted.

    Returns:
        Number of files under the variable's c/ directory.
    """
    chunk_dir = os.path.join(store, var, "c")
    return sum(len(files) for _, _, files in os.walk(chunk_dir))


def describe_time_axis(store: str) -> str:
    """Summarize a store's time axis as "length: first .. last".

    Args:
        store: Path to the store's root directory.

    Returns:
        A one-line description of the time coordinate.
    """
    ds = xr.open_zarr(store, consolidated=False)
    first = str(ds.time.values[0])[:10]
    last = str(ds.time.values[-1])[:10]
    return f"{ds.sizes['time']} days: {first} .. {last}"


def main() -> None:
    """Grow a zarr store with append_dim, then repair a slice with a region write."""
    print("Sixty days of data arriving as two 30-day periods, OCS-style.")
    print("(all writes use consolidated=False: consolidated metadata is not in the zarr v3 spec)")
    full = temperature_dataset(days=60, ny=16, nx=16)
    period_1 = full.isel(time=slice(0, 30))
    period_2 = full.isel(time=slice(30, 60))

    with tempfile.TemporaryDirectory() as tmp:
        store = os.path.join(tmp, "climate.zarr")

        # SECTION: the initial write fixes the chunk grid
        print("\nStep 1 -- initial period, written with time chunks of 30 (one period per chunk):")
        period_1.to_zarr(store, consolidated=False, encoding={"t2m": {"chunks": (30, 16, 16)}})
        print(f"  time axis:   {describe_time_axis(store)}")
        print(f"  chunk files: {count_chunk_files(store, 't2m')} (30 days / 30-day chunks = 1)")

        # SECTION: append_dim extends the time axis
        print("\nStep 2 -- next period arrives; to_zarr(append_dim='time') extends the store:")
        period_2.to_zarr(store, append_dim="time", consolidated=False)
        print(f"  time axis:   {describe_time_axis(store)}")
        print(f"  chunk files: {count_chunk_files(store, 't2m')} (append added exactly one new chunk file)")
        print("  the period length matches the time chunk, so old chunk files were never touched")

        # SECTION: verify the appended data
        print("\nReopening sees one continuous dataset -- readers never know it arrived in parts:")
        back = xr.open_zarr(store, consolidated=False)
        joined_ok = bool(np.allclose(back.t2m.values, full.t2m.values))
        print(f"  values match the 60-day source exactly: {joined_ok}")

        # SECTION: region writes update a slice in place
        print("\nStep 3 -- days 10..19 turn out to be wrong; rewrite just that slice with region=:")
        corrected = (full.isel(time=slice(10, 20)) + 5.0).drop_vars(["y", "x"])
        print("  (drop_vars(['y', 'x']): a region write may only carry variables that overlap the region,")
        print("   so coords without a 'time' dim must be dropped)")
        corrected.to_zarr(store, region={"time": slice(10, 20)}, consolidated=False)
        after = xr.open_zarr(store, consolidated=False)
        day_09 = float(after.t2m.isel(time=9).mean() - full.t2m.isel(time=9).mean())
        day_15 = float(after.t2m.isel(time=15).mean() - full.t2m.isel(time=15).mean())
        day_25 = float(after.t2m.isel(time=25).mean() - full.t2m.isel(time=25).mean())
        print(f"  mean shift vs original -- day 9: {day_09:+.2f}, day 15: {day_15:+.2f}, day 25: {day_25:+.2f}")
        print(f"  time axis unchanged: {describe_time_axis(store)}")
        print("  only the targeted slice changed; the store's shape and chunk grid did not move")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- append_dim='time' extends an existing store; readers see one continuous axis")
    print("- aligning period length with the time chunk makes appends pure additions of new files")
    print("- region={'time': slice(...)} rewrites a slice in place without touching the rest")
    print("- drop coords that lack the region dim before a region write")
    print("- append for new periods + region for corrections = OCS streaming ingestion")


if __name__ == "__main__":
    main()
