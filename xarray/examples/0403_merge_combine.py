"""Merge and combine: assembling datasets from variables and tiles.

What: merges single-variable datasets that share a grid into one Dataset,
provokes a merge conflict and resolves it with compat=, then rebuilds a
dataset from a shuffled pile of spatial/temporal tiles with combine_by_coords.

Why: an open-climate-service store is one Dataset of many variables on a
shared (time, y, x) grid -- merge is how variables from separate sources land
on it. Sources also arrive tiled (per-region downloads, per-period files);
combine_by_coords reads the coordinates and reassembles the hypercube without
anyone tracking tile order by hand.

Run: make run EXAMPLE=0403_merge_combine
"""

import xarray as xr

from climate_stack_xarray import precipitation_dataset, temperature_dataset


def main() -> None:
    """Merge variables onto one grid, resolve conflicts, and combine tiles by coords."""
    # SECTION: merge -- variables sharing a grid
    print("xr.merge combines datasets holding DIFFERENT variables on the same grid:")
    temp = temperature_dataset(days=10)
    precip = precipitation_dataset(days=10)
    ds = xr.merge([temp, precip])
    print(f"  merge([t2m dataset, tp dataset]) -> data_vars={list(ds.data_vars)}, sizes={dict(ds.sizes)}")
    print("  merge also aligns indexes (outer join by default), so grids must agree or NaNs appear")

    # SECTION: merge conflicts
    print("\nWhen the SAME variable appears twice with different values, merge refuses:")
    temp_alt = temperature_dataset(days=10, seed=1)  # same name 't2m', different noise
    try:
        xr.merge([temp, temp_alt])
    except xr.MergeError as err:
        print(f"  xr.MergeError: {str(err).splitlines()[0]}")
    print("  default compat='no_conflicts': values may only disagree where one side is NaN")

    # SECTION: resolving conflicts with compat=
    print("\ncompat= chooses the conflict policy explicitly:")
    forced = xr.merge([temp, temp_alt], compat="override")
    print(f"  compat='override' -> keeps the first dataset's values (equals first: {forced.t2m.equals(temp.t2m)})")
    same = xr.merge([temp, temp], compat="equals")
    print(f"  compat='equals'   -> demands identical values; self-merge passes (round-trips: {same.equals(temp)})")
    print("  compat='identical' additionally compares attrs -- the strictest check")

    # SECTION: combine_by_coords -- tiles back into one dataset
    print("\ncombine_by_coords reassembles a grid of tiles by READING their coordinates:")
    original = temperature_dataset(days=6, ny=8, nx=10)
    tiles = [
        original.isel(time=slice(0, 3), y=slice(0, 4)),  # first half of time, northern half
        original.isel(time=slice(0, 3), y=slice(4, 8)),  # first half of time, southern half
        original.isel(time=slice(3, 6), y=slice(0, 4)),  # second half of time, northern half
        original.isel(time=slice(3, 6), y=slice(4, 8)),  # second half of time, southern half
    ]
    print(f"  4 tiles, each {dict(tiles[0].sizes)} -- a 2x2 grid over (time, y)")
    shuffled = [tiles[3], tiles[0], tiles[2], tiles[1]]
    combined = xr.combine_by_coords(shuffled)
    if not isinstance(combined, xr.Dataset):  # combine_by_coords is typed Dataset | DataArray
        raise TypeError("expected combine_by_coords to return a Dataset")
    print(f"  combine_by_coords(shuffled tiles) -> sizes={dict(combined.sizes)}")
    print(f"  identical to the original dataset: {combined.identical(original)}")
    print("  tile order did not matter: coordinate values, not list position, decide placement")

    # SECTION: choosing between the three
    print("\nWhich tool when:")
    print("  concat            -- one dimension, you state it, you control the order (append a period)")
    print("  merge             -- different variables onto one shared grid")
    print("  combine_by_coords -- many tiles of the same variables; coords decide the layout")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- xr.merge builds multi-variable datasets from single-variable sources on one grid")
    print("- conflicting values for the same variable raise MergeError under compat='no_conflicts'")
    print("- compat='override'/'equals'/'identical' pick looser or stricter conflict policies")
    print("- xr.combine_by_coords stitches spatial/temporal tiles using coordinate values alone")
    print("- concat, merge, combine_by_coords cover append, add-variables, and assemble-tiles")


if __name__ == "__main__":
    main()
