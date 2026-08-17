"""Zarr basics: to_zarr, open_zarr, and what the store looks like on disk.

What: writes a Dataset to a zarr v3 store, reads it back, then walks the store
directory to show its anatomy — zarr.json metadata documents plus one plain
file per array chunk — and connects the files on disk to the array chunks.

Why: open-climate-service serves zarr stores over plain HTTP precisely because
of this layout: a store is just files at predictable paths, so any client can
fetch the small zarr.json metadata and then only the chunk files it needs.
No server-side compute, no range requests, no special protocol.

Run: make run EXAMPLE=0502_zarr_basics
"""

import json
import os
import tempfile

import numpy as np
import xarray as xr

from climate_stack_xarray import temperature_dataset


def print_tree(root: str) -> int:
    """Print the directory tree under a zarr store and return the file count.

    Args:
        root: Path to the store's root directory.

    Returns:
        Total number of files in the tree.
    """
    count = 0
    print(f"  {os.path.basename(root)}/")
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        rel = os.path.relpath(dirpath, root)
        depth = 0 if rel == "." else rel.count(os.sep) + 1
        indent = "  " + "    " * (depth + 1)
        if rel != ".":
            print(f"{'  ' + '    ' * depth}  {os.path.basename(dirpath)}/")
        for name in sorted(filenames):
            size = os.path.getsize(os.path.join(dirpath, name))
            print(f"{indent}  {name}  ({size} B)")
            count += 1
    return count


def main() -> None:
    """Round-trip a Dataset through zarr and dissect the store on disk."""
    ds = temperature_dataset(days=10, ny=8, nx=8)

    with tempfile.TemporaryDirectory() as tmp:
        # SECTION: writing a zarr store
        print("to_zarr() writes a *directory*, not a file; two time chunks of 5 days each:")
        print("  (consolidated=False: consolidated metadata is not part of the zarr v3 spec, and")
        print("   skipping it keeps the store standard and this output warning-free)")
        store = os.path.join(tmp, "climate.zarr")
        ds.to_zarr(store, consolidated=False, encoding={"t2m": {"chunks": (5, 8, 8)}})
        print(f"  wrote {os.path.basename(store)}: t2m {dict(ds.sizes)} as chunks (5, 8, 8)")

        # SECTION: the round-trip
        print("\nopen_zarr() reads it back -- attrs, coords, and values all survive:")
        back = xr.open_zarr(store, consolidated=False)
        print(f"  data_vars: {list(back.data_vars)}, coords: {list(back.coords)}")
        print(f"  t2m attrs: {back.t2m.attrs}")
        same = bool(np.allclose(ds.t2m.values, back.t2m.values))
        print(f"  values identical: {same}, time axis length: {back.sizes['time']}")

        # SECTION: the store on disk
        print("\nThe store is a plain directory tree -- every piece is an ordinary file:")
        total = print_tree(store)
        print(f"  {total} files total")

        # SECTION: reading the metadata documents
        print("\nzarr.json documents describe the hierarchy; the root one is the group:")
        with open(os.path.join(store, "zarr.json")) as f:
            root_meta = json.load(f)
        print(f"  root zarr.json:     node_type={root_meta['node_type']!r}, zarr_format={root_meta['zarr_format']}")
        with open(os.path.join(store, "t2m", "zarr.json")) as f:
            arr_meta = json.load(f)
        chunk_shape = arr_meta["chunk_grid"]["configuration"]["chunk_shape"]
        dtype = arr_meta["data_type"]
        print(f"  t2m/zarr.json:      shape={arr_meta['shape']}, chunk_shape={chunk_shape}, dtype={dtype!r}")
        print("  a client needs only these small JSON files to know exactly which chunk paths exist")

        # SECTION: chunk files map one-to-one to array chunks
        print("\nChunks live under c/ with one path component per dimension (time/y/x):")
        for key in ("t2m/c/0/0/0", "t2m/c/1/0/0", "t2m/c/2/0/0"):
            exists = os.path.exists(os.path.join(store, key))
            print(f"  {key}  exists={exists}")
        print("  10 days / 5-day chunks = 2 time chunks -> c/0/0/0 and c/1/0/0; there is no chunk 2")
        print("  fetching days 0-4 touches only t2m/c/0/0/0 -- one HTTP GET against a static file server")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- a zarr store is a directory: zarr.json metadata + one file per chunk under c/")
    print("- to_zarr()/open_zarr() round-trip the full data model, same as netCDF")
    print("- chunk paths are predictable (c/<t>/<y>/<x>), so clients read only what they need")
    print("- this is why open-climate-service can serve stores over plain HTTP")


if __name__ == "__main__":
    main()
