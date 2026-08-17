"""Dataset construction: multiple variables sharing dims and coords.

What: builds an xr.Dataset holding temperature and precipitation on one shared
grid, then inspects data_vars, coords, and per-variable attrs.

Why: a Dataset is a dict of DataArrays aligned on shared dimensions — the
shape of every real-world climate store. An open-climate-service zarr store
opens as exactly this: variables over (time, y, x) with CF-style attrs.

Run: make run EXAMPLE=0102_dataset_construction
"""

import xarray as xr

from climate_stack_xarray import precipitation_dataset, temperature_dataset


def main() -> None:
    """Combine variables into a Dataset and inspect its structure."""
    # SECTION: merging variables onto one grid
    print("Two single-variable datasets on the same (time, y, x) grid, merged:")
    ds = xr.merge([temperature_dataset(days=31), precipitation_dataset(days=31)])
    print(f"  data_vars: {list(ds.data_vars)}")
    print(f"  coords:    {list(ds.coords)}")
    print(f"  sizes:     {dict(ds.sizes)}")

    # SECTION: the repr is the first debugging tool
    print("\nThe Dataset repr summarizes everything (variables, dtypes, coords):")
    print("  " + "\n  ".join(repr(ds).splitlines()[:12]))

    # SECTION: variables are DataArrays
    print("\nEach data_var is a DataArray with its own attrs:")
    for name, var in ds.data_vars.items():
        print(f"  {name}: {var.attrs['long_name']} [{var.attrs['units']}]")

    # SECTION: dataset-level operations hit every variable
    print("\nOperations on the Dataset apply to every variable at once:")
    daily = ds.mean(dim=["y", "x"])
    print(f"  ds.mean(dim=['y', 'x']) -> sizes {dict(daily.sizes)}")
    print(f"  first-day spatial means: t2m={float(daily.t2m[0]):.2f} degC, tp={float(daily.tp[0]):.2f} mm/day")

    # SECTION: selection works identically
    print("\nSelection is shared across variables (one call, all variables):")
    week = ds.sel(time=slice("2024-01-08", "2024-01-14"))
    print(f"  ds.sel(time=slice('2024-01-08', '2024-01-14')) -> {week.sizes['time']} days")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- Dataset = dict of DataArrays aligned on shared dims and coords")
    print("- merge() combines variables that share a grid")
    print("- reductions and selection apply across all variables in one call")
    print("- this is exactly what opening a climate zarr store gives you")


if __name__ == "__main__":
    main()
