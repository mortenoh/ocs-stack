"""Zarr chunk encoding: choosing chunk shapes at write time, and compressors.

What: writes the same (120, 64, 64) dataset with several chunk shapes via
encoding={"var": {"chunks": ...}}, counts the chunk files each layout creates,
and shows the compressor that reopening reveals in .encoding.

Why: chunk shape is *the* performance decision for a zarr store. In
open-climate-service every store is written with an explicit OCS-style choice:
time chunks of about 30 steps (one period) and spatial dims capped, so that
both "one month, whole country" and "long time series, one place" reads touch
few files. Encoding at write time is where that choice is made.

Run: make run EXAMPLE=0503_zarr_chunks_encoding
"""

import os
import tempfile

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


def write_and_report(ds: xr.Dataset, store: str, chunks: tuple[int, int, int] | None, label: str) -> None:
    """Write a dataset with the given t2m chunk shape and print the resulting file count.

    Args:
        ds: Dataset with a t2m variable of dims (time, y, x).
        store: Path to write the store to.
        chunks: Chunk shape for t2m, or None to let zarr decide.
        label: Short description printed next to the numbers.
    """
    encoding = {} if chunks is None else {"t2m": {"chunks": chunks}}
    ds.to_zarr(store, consolidated=False, encoding=encoding)
    back = xr.open_zarr(store, consolidated=False)
    written = back.t2m.encoding["chunks"]
    n_files = count_chunk_files(store, "t2m")
    print(f"  chunks={str(written):>14}  -> {n_files:3d} chunk files   ({label})")


def main() -> None:
    """Compare chunk layouts for one dataset and inspect encoding after reopen."""
    print("One dataset, dims (time=120, y=64, x=64), float64 -> 3.75 MiB of data.")
    print("(all stores written with consolidated=False: consolidated metadata is not in the zarr v3 spec)")
    ds = temperature_dataset(days=120, ny=64, nx=64)

    with tempfile.TemporaryDirectory() as tmp:
        # SECTION: chunk shape decides the file layout
        print("\nSame data, four chunk choices -- encoding={'t2m': {'chunks': ...}} at write time:")
        print("  number of files = ceil(120/t) * ceil(64/y) * ceil(64/x):")
        write_and_report(ds, os.path.join(tmp, "default.zarr"), None, "no encoding: zarr picks automatically")
        write_and_report(
            ds, os.path.join(tmp, "daily.zarr"), (1, 64, 64), "one file per day: append-friendly, tiny files"
        )
        write_and_report(ds, os.path.join(tmp, "spatial.zarr"), (120, 16, 16), "spatial tiles: full history per tile")
        write_and_report(ds, os.path.join(tmp, "ocs.zarr"), (30, 64, 64), "OCS-style: ~30 time steps, spatial capped")

        # SECTION: why the OCS-style shape
        print("\nThe OCS-style choice, time chunk ~30 and spatial capped (here 64 fits in one tile):")
        print("  - a monthly ingestion period lands as whole chunks (see 0504)")
        print("  - 'map for one month' reads 1 file; 'series at one point' reads 4 files")
        print("  - chunks stay ~1 MiB: big enough to compress well, small enough to fetch fast")
        ocs_store = os.path.join(tmp, "ocs.zarr")
        chunk_path = os.path.join(ocs_store, "t2m", "c", "0", "0", "0")
        print(f"  one OCS-style chunk file on disk: {os.path.getsize(chunk_path)} B (compressed 30x64x64 block)")

        # SECTION: the compressor lives in encoding
        print("\nReopening reveals the full write-time encoding, compressor included:")
        back = xr.open_zarr(ocs_store, consolidated=False)
        enc = back.t2m.encoding
        print(f"  chunks:      {enc['chunks']}")
        print(f"  compressors: {enc['compressors']}")
        print(f"  serializer:  {enc['serializer']}")
        print("  zarr v3 defaults to zstd; each chunk file holds one compressed block")

        # SECTION: encoding is per write, not per dataset
        print("\nThe in-memory dataset never changed -- chunking is purely a storage decision:")
        print(f"  ds.t2m.encoding before any write: {temperature_dataset(days=3, ny=2, nx=2).t2m.encoding}")
        print("  encoding is attached when writing and reported when reopening")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- encoding={'var': {'chunks': ...}} at write time controls the on-disk chunk grid")
    print("- chunk shape directly sets the number of chunk files: ceil(shape/chunk) per dim")
    print("- too-small chunks mean thousands of files; too-big chunks mean over-fetching")
    print("- OCS picks ~30 time steps per chunk with capped spatial tiles -- period-aligned reads")
    print("- .encoding after reopen shows chunks, compressor (zstd), and serializer")


if __name__ == "__main__":
    main()
