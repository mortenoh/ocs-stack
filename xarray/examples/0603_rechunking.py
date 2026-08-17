"""Rechunking: new chunk layouts, non-uniform chunks, and why zarr writes fail.

What: rechunks a dask-backed dataset to a new layout, then reproduces the trap
where concat of unequal pieces plus a y-axis reversal yields NON-UNIFORM chunks,
makes to_zarr fail on them (error caught and printed), and fixes it by
rechunking to a uniform layout.

Why: open-climate-service hits exactly this — tiles get concatenated, latitude
gets flipped north-up, and the resulting irregular chunks are illegal to write:
zarr requires every chunk along a dim to be equal except the last. OCS carries
a _uniform_chunks helper purely to repair this before writing.

Run: make run EXAMPLE=0603_rechunking
"""

import tempfile
import time
from pathlib import Path

import xarray as xr

from climate_stack_xarray import temperature_dataset


def y_chunks(ds: xr.Dataset) -> tuple[int, ...]:
    """Return the chunk sizes along the y dimension of a chunked dataset."""
    chunks = ds.t2m.chunks
    assert chunks is not None
    return tuple(chunks[ds.t2m.dims.index("y")])


def main() -> None:
    """Rechunk layouts, break a zarr write with non-uniform chunks, then fix it."""
    # SECTION: .chunk() moves to a new layout
    print("Rechunking is just .chunk() with a new layout -- lazily, like everything else:")
    ds = temperature_dataset(days=60, ny=64, nx=64).chunk({"time": 30, "y": 64, "x": 64})
    print(f"  original chunks:  {dict(ds.chunks)}")
    rechunked = ds.chunk({"time": 15})
    print(f"  after .chunk({{'time': 15}}): {dict(rechunked.chunks)}")
    print("  (cheap here; on real data rechunking shuffles bytes between blocks)")

    # SECTION: how non-uniform chunks happen in practice
    print("\nThe trap: concat unequal pieces, then flip the axis. Two tiles along y:")
    north = ds.isel(y=slice(0, 40)).chunk({"y": 32})
    south = ds.isel(y=slice(40, 64)).chunk({"y": 32})
    print(f"  north tile (40 rows, y chunked 32): y chunks {y_chunks(north)}")
    print(f"  south tile (24 rows, y chunked 32): y chunks {y_chunks(south)}")

    combined = xr.concat([north, south], dim="y")
    print(f"  concat along y:                     y chunks {y_chunks(combined)}  <- NON-UNIFORM interior")

    flipped = combined.isel(y=slice(None, None, -1))
    print(f"  north-up flip isel(y=::-1):         y chunks {y_chunks(flipped)}")
    print("  Reversal keeps block boundaries but reverses their order -- the odd-sized")
    print("  chunk is now FIRST, so not even the 'last chunk may differ' rule saves us.")

    with tempfile.TemporaryDirectory() as tmp:
        # SECTION: to_zarr rejects non-uniform chunks
        print("\nZarr chunks must be uniform (only the final chunk may be smaller):")
        bad_store = Path(tmp) / "bad.zarr"
        try:
            flipped.to_zarr(bad_store, consolidated=False)
            print("  UNEXPECTED: write succeeded")
        except ValueError as exc:
            first_line = str(exc).splitlines()[0]
            print(f"  to_zarr raised {type(exc).__name__}:")
            print(f"    {first_line}")

        # SECTION: the fix is an explicit uniform rechunk
        print("\nThe fix: rechunk to a uniform layout before writing (OCS: _uniform_chunks):")
        uniform = flipped.chunk({"y": 32})
        print(f"  after .chunk({{'y': 32}}): y chunks {y_chunks(uniform)}")
        good_store = Path(tmp) / "good.zarr"
        t0 = time.perf_counter()
        uniform.to_zarr(good_store, consolidated=False)
        write_s = time.perf_counter() - t0
        print(f"  to_zarr wrote {good_store.name} in {write_s:.3f} s")

        reopened = xr.open_zarr(good_store, consolidated=False)
        print(f"  reopened store chunks: {dict(reopened.chunks)}")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- .chunk() rechunks lazily; the cost is paid at compute time")
    print("- concat of unequal pieces produces non-uniform chunks along that dim")
    print("- axis reversal reverses chunk order, moving odd chunks away from the end")
    print("- to_zarr refuses non-uniform chunks: zarr stores one fixed chunk shape")
    print("- rechunk to a uniform layout right before the write -- exactly what")
    print("  open-climate-service's _uniform_chunks helper exists to do")


if __name__ == "__main__":
    main()
