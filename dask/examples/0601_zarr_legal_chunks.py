"""Zarr-legal chunk layouts: uniform sizes except the final chunk.

What: builds a dataset from appended time pieces, reverses the time axis the
way a north-up flip does, shows that ``to_zarr`` rejects the resulting chunk
layout, and fixes it with a small ``uniform_chunks`` helper.

Why: these are the production patterns open-climate-service encodes. Dask
accepts any chunk tuple, but zarr requires every chunk of a dimension to be
equal-sized except the last. OCS hits exactly this in its downloader: pieces
are appended along time, then ``ensure_north_up`` flips a south-up source with
``isel(slice(None, None, -1))``, which reverses the chunk tuple too — a legal
trailing remainder becomes an illegal leading one. Its ``_uniform_chunks``
helper (re-implemented here) rechunks only the offending dims before writing.

Run: make run EXAMPLE=0601_zarr_legal_chunks
"""

import tempfile
import warnings
from collections.abc import Hashable
from pathlib import Path

import pandas as pd
import xarray as xr

from climate_stack_dask.helpers import random_field


def uniform_chunks(ds: xr.Dataset) -> xr.Dataset:
    """Rechunk any dim whose dask chunks are non-uniform except for the final chunk.

    Re-implementation of open-climate-service's ``_uniform_chunks``: a dim is
    zarr-legal when every chunk but the last is the same size and the last is
    no larger. Anything else is rechunked to the largest existing chunk, which
    restores the pre-reversal layout because dask fills uniformly and leaves
    the remainder last. Legal dims are left alone — rechunking a full grid
    costs a graph rewrite for nothing.

    Args:
        ds: Dataset whose data variables may carry zarr-illegal dask chunks.

    Returns:
        The dataset unchanged if every dim is already legal, otherwise a copy
        rechunked to uniform sizes on the offending dims.
    """
    targets: dict[Hashable, int] = {}
    for var in ds.data_vars.values():
        for dim, chunks in zip(var.dims, var.chunks or (), strict=False):
            if len(set(chunks[:-1])) <= 1 and chunks[-1] <= chunks[0]:
                continue  # already legal: uniform except a smaller final chunk
            targets[dim] = max(targets.get(dim, 0), max(chunks))
    if not targets:
        return ds
    return ds.chunk(targets)


def _time_chunks(field: xr.DataArray) -> tuple[int, ...]:
    """Return the chunk tuple along the first (time) dimension of a dask-backed DataArray.

    Args:
        field: A dask-backed DataArray with time as its first dimension.

    Returns:
        The chunk sizes along time.

    Raises:
        ValueError: If the DataArray is not dask-backed.
    """
    chunks = field.chunks
    if chunks is None:
        raise ValueError("expected a dask-backed DataArray")
    return chunks[0]


def _piece(days: int, start: str, seed: int) -> xr.DataArray:
    """Return a lazy (time, y, x) piece with daily time labels starting at ``start``.

    Args:
        days: Number of daily time steps in the piece.
        start: ISO date of the first time step.
        seed: Seed for the synthetic field.

    Returns:
        A dask-backed DataArray named ``t2m`` with one time chunk.
    """
    arr = random_field(days=days, ny=64, nx=64, time_chunk=days, spatial_chunk=64, seed=seed)
    return xr.DataArray(
        arr,
        dims=("time", "y", "x"),
        coords={"time": pd.date_range(start, periods=days, freq="D")},
        name="t2m",
    )


def main() -> None:
    """Produce a zarr-illegal chunk layout by reversal, then fix it the OCS way."""
    # Consolidated metadata is not yet in the zarr v3 spec; the advisory warning is noise here.
    warnings.filterwarnings("ignore", message="Consolidated metadata is currently not part in the Zarr format 3")

    # SECTION: appending pieces along time
    print("A store grows by appending downloads along time: 30 + 30 + 5 days.")
    pieces = [
        _piece(30, "2024-01-01", seed=1),
        _piece(30, "2024-01-31", seed=2),
        _piece(5, "2024-03-01", seed=3),
    ]
    field = xr.concat(pieces, dim="time")
    ds = field.to_dataset()
    print(f"  time chunks after concat: {_time_chunks(ds['t2m'])}")
    print("  Legal for zarr: uniform 30s with the remainder (5) LAST.")

    # SECTION: the reversal that breaks it
    print("\nA south-up source gets flipped north-up: isel(time=slice(None, None, -1)).")
    print("(OCS does the same flip on y; the chunk mechanics are identical.)")
    reversed_ds = ds.isel(time=slice(None, None, -1))
    print(f"  time chunks after reversal: {_time_chunks(reversed_ds['t2m'])}")
    print("  Reversing the axis reverses the chunk TUPLE too: the remainder now leads.")
    print("  Dask is fine with (5, 30, 30). Zarr is not.")

    # SECTION: to_zarr refuses the write
    print("\nAttempting to_zarr with the illegal layout:")
    with tempfile.TemporaryDirectory() as tmp:
        store = Path(tmp) / "field.zarr"
        try:
            reversed_ds.to_zarr(store, mode="w")
            print("  UNEXPECTED: write succeeded")
        except ValueError as err:
            first_line = str(err).splitlines()[0]
            print(f"  ValueError: {first_line}")

        # SECTION: the fix — rechunk offending dims to the max existing chunk
        print("\nFix: uniform_chunks(ds) -- detect dims that are non-uniform except the")
        print("final chunk, rechunk them to the max existing chunk (here 30).")
        print("Pattern credit: open-climate-service's _uniform_chunks in its downloader.")
        fixed = uniform_chunks(reversed_ds)
        print(f"  time chunks after fix: {_time_chunks(fixed['t2m'])}")
        fixed.to_zarr(store, mode="w")
        print("  to_zarr succeeded.")

        # SECTION: proof from the store
        roundtrip = xr.open_zarr(store)
        print("\nReading the store back:")
        print(f"  shape={roundtrip['t2m'].shape}, time chunks={_time_chunks(roundtrip['t2m'])}")
        print(f"  first time label: {str(roundtrip['time'].values[0])[:10]} (reversed order preserved)")
        roundtrip.close()

    # SECTION: summary
    print("\n=== Summary ===")
    print("- dask allows any chunk tuple; zarr requires uniform chunks except the LAST")
    print("- reversing an axis reverses its chunk tuple: (30, 30, 5) -> (5, 30, 30), now illegal")
    print("- to_zarr fails loudly with a ValueError rather than writing a broken store")
    print("- fix: rechunk only offending dims to the max existing chunk (OCS _uniform_chunks)")
    print("- check before rechunking: blanket rechunks of legal layouts cost a graph rewrite")


if __name__ == "__main__":
    main()
