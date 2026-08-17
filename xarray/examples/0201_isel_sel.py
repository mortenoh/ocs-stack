"""Positional isel vs label-based sel: the two ways to index a cube.

What: selects from a (time, y, x) temperature cube by integer position (isel)
and by coordinate label (sel), including string-date slices on time, and shows
when a selection drops a dimension versus keeping it at length 1.

Why: open-climate-service code paths slice stores constantly — "this period"
is a label slice on time, "this pyramid tile" is a positional slice on y/x.
Reading sel/isel fluently is reading OCS fluently, and the scalar-vs-range
distinction decides whether the time dim survives into the next operation.

Run: make run EXAMPLE=0201_isel_sel
"""

from playground_xarray import temperature_dataset


def main() -> None:
    """Select by position and by label, and watch which dims survive."""
    # SECTION: the cube under test
    print("One month of daily temperature on a small grid:")
    ds = temperature_dataset(days=31, ny=4, nx=5)
    print(f"  sizes: {dict(ds.sizes)}")
    print(f"  time:  {str(ds.time.values[0])[:10]} .. {str(ds.time.values[-1])[:10]}")
    print(f"  y:     {[round(float(v), 2) for v in ds.y.values]}  (labels, not indices)")

    # SECTION: isel -- positional, like numpy
    print("\nisel indexes by POSITION, exactly like numpy axis indexing:")
    print(f"  ds.isel(time=0)             -> sizes {dict(ds.isel(time=0).sizes)}  (first day, time dim gone)")
    print(f"  ds.isel(time=-1)            -> last day, {str(ds.isel(time=-1).time.values)[:10]}")
    week_pos = ds.isel(time=slice(0, 7))
    print(f"  ds.isel(time=slice(0, 7))   -> {week_pos.sizes['time']} days  (python slice: end EXCLUSIVE)")

    # SECTION: sel -- label-based
    print("\nsel indexes by LABEL, using the coordinate values:")
    day = ds.sel(time="2024-01-15")
    print(f"  ds.sel(time='2024-01-15')   -> sizes {dict(day.sizes)}, spatial mean {float(day.t2m.mean()):.2f} degC")
    y_label = float(ds.y.values[1])
    row = ds.sel(y=y_label)
    print(f"  ds.sel(y={y_label:.6f}) -> sizes {dict(row.sizes)}  (floats must match the label exactly)")

    # SECTION: slices on time with string dates
    print("\nTime slices take plain strings -- and label slices are INCLUSIVE on both ends:")
    week_lab = ds.sel(time=slice("2024-01-08", "2024-01-14"))
    print(f"  ds.sel(time=slice('2024-01-08', '2024-01-14')) -> {week_lab.sizes['time']} days (14th included)")
    half_open = ds.sel(time=slice("2024-01-20", None))
    print(f"  ds.sel(time=slice('2024-01-20', None))         -> {half_open.sizes['time']} days (open end)")
    print("  contrast: isel slice(0, 7) excluded index 7; sel slices never make you do the -1 dance")

    # SECTION: scalar vs range selection
    print("\nScalar selection DROPS the dim; list/slice selection KEEPS it:")
    scalar = ds.isel(time=0)
    kept = ds.isel(time=[0])
    print(f"  ds.isel(time=0)   -> dims {dict(scalar.sizes)}  (no time dim)")
    print(f"  ds.isel(time=[0]) -> dims {dict(kept.sizes)}  (time survives at length 1)")
    print("  the length-1 form matters when downstream code expects a time axis (concat, resample, zarr append)")

    # SECTION: what scalar selection leaves behind
    print("\nA dropped dim leaves a scalar coord behind (handy label, sometimes unwanted):")
    point = ds.sel(time="2024-01-15").isel(y=0, x=0)
    print(f"  point dims: {dict(point.sizes)}  -- 0-d, but coords remember where it came from:")
    print(f"  point coords: time={str(point.time.values)[:10]}, y={float(point.y):.2f}, x={float(point.x):.2f}")
    clean = ds.sel(time="2024-01-15", drop=True)
    print(f"  ds.sel(time='2024-01-15', drop=True) -> coords {list(clean.coords)}  (scalar time coord removed)")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- isel = integer positions (numpy rules, slice end exclusive)")
    print("- sel = coordinate labels; time takes string dates directly")
    print("- sel slices are inclusive on both ends; None leaves an end open")
    print("- scalar selection drops the dim, list/slice keeps it at length >= 1")
    print("- dropped dims leave scalar coords; sel(..., drop=True) removes them")


if __name__ == "__main__":
    main()
