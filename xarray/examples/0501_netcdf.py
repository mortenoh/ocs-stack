"""netCDF round-trip: to_netcdf, open_dataset, engines, and file size.

What: writes a two-variable Dataset to a netCDF file, reopens it, and verifies
that attrs and coords survive the round-trip; picks engines explicitly and
compares on-disk sizes with and without compression.

Why: open-climate-service ingests source data that arrives as netCDF (and
GRIB) files before it is normalized and rewritten as zarr. Knowing what a
netCDF file preserves — and which engine reads it — is the entry point of
every ingestion pipeline.

Run: make run EXAMPLE=0501_netcdf
"""

import os
import tempfile

import numpy as np
import xarray as xr
from xarray.backends import list_engines

from ocs_stack_xarray import precipitation_dataset, temperature_dataset


def human_size(num_bytes: int) -> str:
    """Format a byte count as a short human-readable string.

    Args:
        num_bytes: Size in bytes.

    Returns:
        The size rendered as bytes or kibibytes, e.g. "512 B" or "12.3 KiB".
    """
    if num_bytes < 1024:
        return f"{num_bytes} B"
    return f"{num_bytes / 1024:.1f} KiB"


def main() -> None:
    """Round-trip a Dataset through netCDF and inspect engines and file sizes."""
    ds = xr.merge([temperature_dataset(days=31, ny=16, nx=16), precipitation_dataset(days=31, ny=16, nx=16)])
    ds.attrs["title"] = "synthetic climate cube"

    with tempfile.TemporaryDirectory() as tmp:
        # SECTION: writing a netCDF file
        print("to_netcdf() serializes the whole Dataset -- variables, coords, attrs -- into one file:")
        path = os.path.join(tmp, "climate.nc")
        ds.to_netcdf(path)
        print(f"  wrote {os.path.basename(path)}: {human_size(os.path.getsize(path))}")
        print(f"  variables: {list(ds.data_vars)}, sizes: {dict(ds.sizes)}")

        # SECTION: the round-trip preserves the data model
        print("\nopen_dataset() reads it back; attrs and coords survive intact:")
        with xr.open_dataset(path) as back:
            back = back.load()  # pull data into memory so the file can close
        print(f"  dataset attrs:    {back.attrs}")
        print(f"  t2m attrs:        {back.t2m.attrs}")
        print(f"  coords preserved: {list(back.coords)}")
        same_time = bool(np.array_equal(ds.time.values, back.time.values))
        same_vals = bool(np.allclose(ds.t2m.values, back.t2m.values))
        print(f"  time coord identical: {same_time}, t2m values identical: {same_vals}")
        first, last = str(back.time.values[0])[:10], str(back.time.values[-1])[:10]
        print(f"  time axis: {back.sizes['time']} days, {first} .. {last}")

        # SECTION: engine choice
        print("\nEngines: xarray delegates file I/O to a backend; installed backends here:")
        engines = [name for name in list_engines() if name != "store"]
        print(f"  available: {engines}")
        print("  'netcdf4' (the netCDF-C/HDF5 library) is the default for .nc when installed;")
        print("  alternatives are 'h5netcdf' (pure-HDF5) and 'scipy' (netCDF3 only).")
        with xr.open_dataset(path, engine="netcdf4") as explicit:
            print(f"  open_dataset(engine='netcdf4') -> {len(explicit.data_vars)} vars, sizes {dict(explicit.sizes)}")

        # SECTION: file size and compression
        print("\nDefault netCDF4 output is uncompressed; per-variable zlib encoding shrinks it:")
        packed = os.path.join(tmp, "climate_zlib.nc")
        comp = {"zlib": True, "complevel": 4}
        ds.to_netcdf(packed, encoding={name: comp for name in ds.data_vars})
        raw_size = os.path.getsize(path)
        packed_size = os.path.getsize(packed)
        in_memory = int(sum(int(v.nbytes) for v in ds.data_vars.values()))
        print(f"  in-memory data:   {human_size(in_memory)}")
        print(f"  uncompressed .nc: {human_size(raw_size)}")
        print(f"  zlib level 4 .nc: {human_size(packed_size)}  ({packed_size / raw_size:.0%} of uncompressed)")
        with xr.open_dataset(packed) as pback:
            ok = bool(np.allclose(ds.t2m.values, pback.t2m.values))
        print(f"  compressed round-trip lossless: {ok}")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- to_netcdf()/open_dataset() round-trip variables, coords, and attrs losslessly")
    print("- engines are pluggable backends; 'netcdf4' is the workhorse for .nc files")
    print("- compression is opt-in via per-variable encoding (zlib/complevel)")
    print("- netCDF is one opaque file -- the contrast with zarr's chunk-per-file layout is next")


if __name__ == "__main__":
    main()
