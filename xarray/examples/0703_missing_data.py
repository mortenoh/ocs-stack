"""Missing data: NaN in memory, _FillValue in storage, and gap repair.

What: punches gaps into a temperature field with where(), counts them with
isnull/notnull/count, contrasts fillna with interpolate_na(dim="time"), shows
how skipna interacts with reductions, and round-trips through a netCDF file to
reveal _FillValue living in encoding rather than in the data.

Why: real climate feeds have holes -- sensor dropouts, late arrivals, masked
ocean cells. In memory xarray represents all of them as NaN; on disk a
sentinel _FillValue stands in. open-climate-service leans on skipna-aware
reductions and on encoding round-tripping every time a period lands in zarr.

Run: make run EXAMPLE=0703_missing_data
"""

import tempfile
from pathlib import Path

import xarray as xr

from climate_stack_xarray import temperature_dataset


def main() -> None:
    """Create, count, repair, and round-trip missing values in a temperature field."""
    # SECTION: NaN is the in-memory missing value
    print("In memory, missing = NaN. where(cond) keeps values where cond holds, else NaN:")
    t2m = temperature_dataset(days=30, ny=4, nx=5)["t2m"]
    gappy = t2m.where(t2m.time.dt.day % 5 != 0)  # sensor down every 5th day
    print(f"  full field:  {t2m.size} values, dtype {t2m.dtype}")
    print("  gappy field: every 5th day masked -> whole-day gaps at days 5, 10, 15, 20, 25, 30")
    print("  NaN only exists for float dtypes; masking an int array silently casts it to float")

    # SECTION: counting the gaps
    print("\nisnull/notnull give boolean masks; count() tallies the non-missing values:")
    print(f"  gappy.isnull().sum()  = {int(gappy.isnull().sum())} missing (6 days x 4 y x 5 x)")
    print(f"  gappy.notnull().sum() = {int(gappy.notnull().sum())} present")
    per_pixel = gappy.count(dim="time")
    print(f"  gappy.count(dim='time') -> every pixel has {int(per_pixel.min())} of {t2m.sizes['time']} days")

    # SECTION: skipna in reductions
    print("\nReductions skip NaN by default (skipna=True for float data):")
    print(f"  gappy.mean()             = {float(gappy.mean()):.2f}  (mean of the 480 present values)")
    print(f"  gappy.mean(skipna=False) = {float(gappy.mean(skipna=False)):.2f}  (one NaN poisons the whole thing)")
    strict = gappy.mean(dim="time", skipna=False)
    print(f"  per-pixel mean with skipna=False -> {int(strict.isnull().sum())} of {strict.size} pixels NaN")
    print("  skipna=True computes over what exists; skipna=False demands completeness")

    # SECTION: fillna vs interpolate_na
    print("\nRepairing gaps: fillna substitutes a constant, interpolate_na uses neighbors in time:")
    series = gappy.isel(y=0, x=0)  # one pixel's 30-day time series
    filled = series.fillna(series.mean())
    interp = series.interpolate_na(dim="time")
    print("  around the day-5 gap (days 4..6):")
    for label, da in [("original", series), ("fillna(mean)", filled), ("interpolate_na", interp)]:
        vals = ", ".join(f"{float(v):6.2f}" for v in da.isel(time=slice(3, 6)).values)
        print(f"    {label:15}: {vals}")
    print(f"  interpolate_na left {int(interp.isnull().sum())} NaN: day 30 has no later neighbor to")
    print("  interpolate toward -- edge gaps need fill_value='extrapolate' or a fillna pass")

    # SECTION: _FillValue lives in storage encoding
    print("\nOn disk there is no NaN convention -- a sentinel _FillValue stands in for missing:")
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "gappy.nc"
        gappy.to_netcdf(path, encoding={"t2m": {"_FillValue": -9999.0}})
        decoded = xr.load_dataset(path)
        raw = xr.load_dataset(path, mask_and_scale=False)
        print("  written with encoding={'t2m': {'_FillValue': -9999.0}}")
        print(f"  raw file value at a gap (mask_and_scale=False): {float(raw['t2m'].isel(time=4, y=0, x=0)):.1f}")
        print(f"  decoded value at the same gap:                  {float(decoded['t2m'].isel(time=4, y=0, x=0))}")
        print(f"  decoded t2m.encoding['_FillValue'] = {decoded['t2m'].encoding['_FillValue']}")
        print(f"  decoded t2m.attrs                  = {decoded['t2m'].attrs}")
    print("  decoding moved _FillValue out of the data and into .encoding; the values became NaN")
    print("  the same mechanism drives zarr writes in OCS -- fill_value is store metadata")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- in memory missing = NaN (floats only); where() is how you introduce it")
    print("- isnull/notnull/count measure gaps; count(dim=...) shows coverage per pixel")
    print("- skipna=True (default) reduces over what exists; skipna=False propagates NaN")
    print("- fillna substitutes values; interpolate_na fills interior gaps along a dim")
    print("- on disk _FillValue is the sentinel; xarray decodes it to NaN and parks it in .encoding")


if __name__ == "__main__":
    main()
