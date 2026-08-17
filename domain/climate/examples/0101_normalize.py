"""Normalization: what a raw source looks like and what normalize() fixes.

What: fetches one period of raw temperature and rainfall from the synthetic
sources, prints exactly what is wrong with each, then runs normalize() and
prints the same facts corrected. Ends with the duplicate-timestamp case.

Why: open-climate-service serves many sources through one API. That only works
if every stored dataset looks identical -- same dim names, same units, same
axis direction. Normalization is where that uniformity is bought, once, at
ingest, so nothing downstream ever has to ask "which convention is this?".

Run: make run EXAMPLE=0101_normalize
"""

import xarray as xr

from playground_domain_climate import fetch_precipitation, fetch_temperature, normalize
from playground_domain_climate.sources import Period, enumerate_periods


def describe(ds: xr.Dataset, variable: str, label: str) -> None:
    """Print the four facts a service cares about for one dataset.

    Args:
        ds: The dataset to describe.
        variable: Name of the data variable to report on.
        label: Short prefix identifying which stage this is.
    """
    var = ds[variable]
    y_name = "y" if "y" in ds.coords else "lat"
    y_first, y_last = float(ds[y_name][0]), float(ds[y_name][-1])
    direction = "ascending (south-up)" if y_first < y_last else "descending (north-up)"
    print(f"  {label} dims      : {var.dims}")
    print(f"  {label} units attr: {var.attrs.get('units')!r}")
    print(f"  {label} y axis    : {y_first:.2f} -> {y_last:.2f}  {direction}")
    print(f"  {label} value mean: {float(var.mean()):.3f}")


def main() -> None:
    """Show a raw source, what is wrong with it, and what normalize() fixes."""
    period: Period = enumerate_periods(2024, 1)[0]

    # SECTION: the raw temperature source
    print("A source hands us one month of temperature. It is not wrong -- it is")
    print("just published in its own conventions, and every source has different ones.\n")
    raw_t = fetch_temperature(period, ny=12, nx=12)
    describe(raw_t, "t2m", "raw   ")

    print("\nThree things break a service that must serve every dataset identically:")
    print("  1. dims are (time, lat, lon). Generic code cannot write ds.mean('y')")
    print("     when half the sources spell that axis 'lat' and half 'latitude'.")
    print("  2. units are Kelvin. An API that promises degC would publish 300.")
    print("  3. y ascends, so row 0 is the SOUTHERNMOST. A GeoZarr affine assumes")
    print("     row 0 is north; render this as-is and the map is upside down.")

    # SECTION: normalized temperature
    print("\nnormalize() rewrites all three:\n")
    norm_t = normalize(raw_t)
    describe(norm_t, "t2m", "normal")
    kelvin_mean = float(raw_t["t2m"].mean())
    celsius_mean = float(norm_t["t2m"].mean())
    print(f"\n  check: {kelvin_mean:.3f} K - 273.15 = {kelvin_mean - 273.15:.3f} == {celsius_mean:.3f} degC")
    print(f"  check: row 0 of the raw grid is lat {float(raw_t['lat'][0]):.2f} (south);")
    print(f"         row 0 of the normalized grid is y {float(norm_t['y'][0]):.2f} (north)")

    # SECTION: attrs are inert
    print("\nWhy conversion and attribute rewriting happen in the same function:")
    print("  xarray never reads the 'units' attribute. Subtracting 273.15 changes the")
    print("  numbers and leaves attrs['units'] saying 'K'. Nothing raises. The dataset")
    print("  is now Celsius labelled Kelvin, and the error only surfaces months later")
    print("  in somebody's chart. convert_units() does both edits together so they")
    print("  cannot drift apart.")
    silently_wrong = raw_t["t2m"] - 273.15
    print(f"  demo: (raw - 273.15).attrs = {dict(silently_wrong.attrs)}  <- still says K")
    print(f"        normalize()  .attrs = {dict(norm_t['t2m'].attrs)}")

    # SECTION: the precipitation source
    print("\nThe rainfall source has the same axis problems plus its own unit:")
    raw_p = fetch_precipitation(period, ny=12, nx=12)
    describe(raw_p, "tp", "raw   ")
    norm_p = normalize(raw_p)
    print()
    describe(norm_p, "tp", "normal")
    print(f"\n  metres are a reanalysis habit; a mm total of {float(norm_p['tp'].sum()):.0f} mm reads as rainfall,")
    print(f"  where {float(raw_p['tp'].sum()):.3f} m does not. Same numbers, 1000x apart.")

    # SECTION: duplicate timestamps
    print("\nLast job: duplicate timestamps. A re-fetched period arrives holding days")
    print("the store may already have. Concatenating a period with itself simulates that:\n")
    doubled = xr.concat([norm_t, norm_t], dim="time")
    print(f"  concat of a period with itself: time={doubled.sizes['time']} steps")
    print(f"  unique timestamps?             : {doubled.indexes['time'].is_unique}")
    cleaned = normalize(doubled)
    print(f"  after normalize()              : time={cleaned.sizes['time']} steps")
    print(f"  unique timestamps?             : {cleaned.indexes['time'].is_unique}")
    print("  sort_time() keeps the LAST duplicate, so a re-fetch acts as a correction:")
    print("  the newer copy of a day wins over the one already held.")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- Sources disagree on dim names, units, and axis direction; all three are fixed once, at ingest")
    print("- (time, lat, lon) K south-up  ->  (time, y, x) degC north-up")
    print("- Precipitation in metres becomes mm; temperature in K becomes degC")
    print("- attrs are inert: xarray will not convert for you, so values and units are rewritten together")
    print("- Duplicate timestamps are dropped keeping the last, so re-fetching a period is a safe correction")
    print("- The payoff: every dataset downstream looks the same, so one API serves all of them")


if __name__ == "__main__":
    main()
