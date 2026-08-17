"""Chunked opening: turning numpy-backed data into dask-backed, and lazy zarr stores.

What: converts an in-memory dataset to dask-backed with .chunk(), inspects the
repr / .data type / .chunks differences, writes it to a zarr store, and reopens
it with open_zarr to show that opening reads only metadata, never the data.

Why: this is how open-climate-service touches its stores — every store is
opened chunked, so a dataset of any size costs almost nothing to open and all
computation stays deferred. Understanding "dask-backed" starts here.

Run: make run EXAMPLE=0601_chunked_open
"""

import tempfile
import time
from pathlib import Path

import xarray as xr

from climate_stack_xarray import temperature_dataset


def backing(ds: xr.Dataset, var: str) -> str:
    """Return the fully qualified type name of the array backing a data variable."""
    data = ds[var].data
    return f"{type(data).__module__}.{type(data).__name__}"


def repr_line(ds: xr.Dataset, needle: str) -> str:
    """Return the first line of the Dataset repr containing the given substring."""
    return next(line.strip() for line in repr(ds).splitlines() if needle in line)


def count_files(root: Path) -> int:
    """Return the number of regular files under a directory tree."""
    return sum(1 for p in root.rglob("*") if p.is_file())


def main() -> None:
    """Chunk a dataset into dask, write it to zarr, and reopen it lazily."""
    # SECTION: numpy-backed baseline
    print("A synthetic year of daily temperature, fully in memory (numpy-backed):")
    ds = temperature_dataset(days=365, ny=128, nx=128)
    print(f"  sizes:        {dict(ds.sizes)}")
    print(f"  t2m backing:  {backing(ds, 't2m')}")
    print(f"  t2m .chunks:  {ds.t2m.chunks}  (None means: not chunked, all in memory)")
    print(f"  repr line:    {repr_line(ds, 't2m')}")

    # SECTION: .chunk() converts to dask
    print("\n.chunk() re-backs every variable with a dask array -- same values, new engine:")
    chunked = ds.chunk({"time": 30, "y": 64, "x": 64})
    n_blocks = len(chunked.t2m.data.blocks.ravel())
    print(f"  t2m backing:  {backing(chunked, 't2m')}")
    print(f"  t2m .chunks:  {chunked.t2m.chunks}")
    print(f"  block count:  {n_blocks} blocks of at most (30, 64, 64)")
    print(f"  repr line:    {repr_line(chunked, 't2m')}")
    print("  The repr now shows a dask.array recipe instead of loaded values.")

    with tempfile.TemporaryDirectory() as tmp:
        store = Path(tmp) / "t2m.zarr"

        # SECTION: writing the chunked dataset to zarr
        print("\nto_zarr() computes each dask block and writes it as one zarr chunk:")
        t0 = time.perf_counter()
        chunked.to_zarr(store, consolidated=False)
        write_s = time.perf_counter() - t0
        print(f"  wrote {store.name} in {write_s:.3f} s, {count_files(store)} files on disk")
        print("  (one file per chunk per variable, plus small metadata files)")

        # SECTION: reopening is metadata-only
        print("\nopen_zarr() is lazy by default -- it reads metadata files, zero data chunks:")
        t0 = time.perf_counter()
        reopened = xr.open_zarr(store, consolidated=False)
        open_s = time.perf_counter() - t0
        print(f"  opened in {open_s * 1000:.1f} ms")
        print(f"  t2m backing:  {backing(reopened, 't2m')}")
        print(f"  t2m .chunks:  {reopened.t2m.chunks}  (store chunking becomes dask chunking)")
        print(f"  repr line:    {repr_line(reopened, 't2m')}")

        # SECTION: data is only read on compute
        print("\nThe data itself is read only when something forces computation:")
        t0 = time.perf_counter()
        loaded = reopened.t2m.compute()
        load_s = time.perf_counter() - t0
        print(f"  .compute() read + decompressed everything in {load_s * 1000:.1f} ms")
        print(f"  open was {load_s / open_s:.0f}x faster than load: opening never touched the data")
        print(f"  loaded backing: {type(loaded.data).__module__}.{type(loaded.data).__name__}")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- .chunk() swaps the numpy backing for dask; values, dims, coords are unchanged")
    print("- dask-backed variables report .chunks; numpy-backed ones report None")
    print("- the repr shows a dask recipe (chunksize, dtype) instead of loaded values")
    print("- to_zarr writes one file per chunk; open_zarr reads only metadata")
    print("- this is why OCS can open any store instantly: cost is deferred to compute")


if __name__ == "__main__":
    main()
