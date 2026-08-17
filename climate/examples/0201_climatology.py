"""Climatology: monthly normals and the anomalies computed against them.

What: ingests a full year of daily temperature, computes the 12-month
climatological normal with climatological_normal(), prints the seasonal cycle
month by month, then computes monthly_anomaly() and checks it centres on zero.

Why: an absolute temperature is nearly uninterpretable on its own -- 29 degC is
alarming in Oslo and unremarkable in Freetown. open-climate-service publishes
anomalies for exactly this reason: subtracting the local normal turns a number
that only a specialist can read into one that answers "is this unusual here?".

Run: make run EXAMPLE=0201_climatology
"""

import calendar
import tempfile
from typing import Any

import icechunk  # no type stubs; repository handles stay narrowly typed as Any
import xarray as xr

from playground_climate import (
    climatological_normal,
    enumerate_periods,
    fetch_temperature,
    ingest,
    monthly_anomaly,
    store_path,
)
from playground_climate.sources import Period

# Silence the Rust-side WARN chatter icechunk emits during ordinary appends.
icechunk.set_logs_filter("error")


def make_repo(tmp: str, dataset_id: str) -> Any:
    """Create a fresh icechunk repository under the OCS store layout.

    Args:
        tmp: Temporary data directory standing in for an instance data dir.
        dataset_id: Public identifier of the dataset.

    Returns:
        A new icechunk repository.
    """
    path = store_path(tmp, dataset_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    return icechunk.Repository.create(icechunk.local_filesystem_storage(str(path)))


def small_temperature(period: Period) -> xr.Dataset:
    """Fetch one period of temperature on a small grid.

    Args:
        period: The period to fetch.

    Returns:
        The raw source dataset.
    """
    return fetch_temperature(period, ny=12, nx=12)


def bar(value: float, low: float, high: float, width: int = 32) -> str:
    """Render a value as an ASCII bar scaled between two bounds.

    Args:
        value: The value to render.
        low: Value mapped to an empty bar.
        high: Value mapped to a full bar.
        width: Bar width in characters.

    Returns:
        A string of ``#`` padded with spaces to ``width``.
    """
    span = high - low
    filled = 0 if span <= 0 else int(round((value - low) / span * width))
    return "#" * max(0, min(width, filled))


def main() -> None:
    """Ingest a year, derive its normals, and check the anomalies centre on zero."""
    with tempfile.TemporaryDirectory() as tmp:
        repo = make_repo(tmp, "temperature")
        periods: list[Period] = enumerate_periods(2024, 12)

        # SECTION: ingest a year
        print("Step 1 -- ingest twelve monthly periods, one commit each.\n")
        report = ingest(repo, periods, small_temperature)
        ds: xr.Dataset = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
        print(f"  ingested {len(report.ingested)} periods, failed {len(report.failed)}")
        print(f"  store: dims={dict(ds.sizes)} units={ds['t2m'].attrs['units']!r}")
        print(f"  span : {str(ds['time'].values[0])[:10]} .. {str(ds['time'].values[-1])[:10]}")

        # SECTION: the climatological normal
        print("\nStep 2 -- climatological_normal() groups every day by its calendar month")
        print("and averages. Twelve numbers per grid cell: what a typical January looks")
        print("like there, a typical February, and so on.\n")
        normal = climatological_normal(ds, "t2m")
        print(f"  normal dims = {normal.dims}, sizes = {dict(normal.sizes)}")
        print(f"  the daily (time, y, x) series collapsed to (month, y, x): {ds.sizes['time']} steps -> 12\n")

        area_mean = normal.mean(dim=("y", "x"))
        values = [float(v) for v in area_mean.values]
        low, high = min(values) - 0.3, max(values) + 0.3
        print(f"  {'month':>5}  {'normal degC':>11}  seasonal cycle")
        for month, value in enumerate(values, start=1):
            print(f"  {calendar.month_abbr[month]:>5}  {value:>11.2f}  {bar(value, low, high)}")
        warmest = values.index(max(values)) + 1
        coolest = values.index(min(values)) + 1
        print(f"\n  warmest normal: {calendar.month_name[warmest]} at {max(values):.2f} degC")
        print(f"  coolest normal: {calendar.month_name[coolest]} at {min(values):.2f} degC")
        print(f"  annual range  : {max(values) - min(values):.2f} degC -- a tropical grid has a shallow cycle")

        print("\n  Caveat worth stating plainly: with one year of data, 'the normal for")
        print("  January' is just this January. A real normal is a 30-year mean (the WMO")
        print("  convention), which is what keeps one freak month from defining normal.")
        print("  The arithmetic is identical; only the length of the record differs.")

        # SECTION: anomalies
        print("\nStep 3 -- monthly_anomaly() subtracts each day's own month normal, so")
        print("every value becomes a departure rather than an absolute reading.\n")
        anomaly = monthly_anomaly(ds, "t2m")
        print(f"  anomaly dims = {anomaly.dims}, shape matches the input: {anomaly.shape == ds['t2m'].shape}")
        print(f"  units carried through: {anomaly.attrs['units']!r}, long_name {anomaly.attrs['long_name']!r}")
        print(f"\n  grand mean of the anomaly field: {float(anomaly.mean()):.2e} degC  (zero, to floating point)")
        print("  That is not a coincidence -- subtracting each group's own mean forces")
        print("  every group to average to zero. It is the arithmetic checking itself.\n")

        by_month = anomaly.groupby("time.month").mean()
        print(f"  {'month':>5}  {'mean anomaly':>13}  {'coldest day':>12}  {'warmest day':>12}")
        for month in range(1, 13):
            sel = anomaly.where(anomaly["time"].dt.month == month, drop=True)
            mean_m = float(by_month.sel(month=month).mean())
            lo, hi = float(sel.min()), float(sel.max())
            print(f"  {calendar.month_abbr[month]:>5}  {mean_m:>13.2e}  {lo:>12.2f}  {hi:>12.2f}")

        spread = float(anomaly.std())
        print(f"\n  daily spread around the normal: {spread:.2f} degC standard deviation.")
        hottest = anomaly.max(dim=("y", "x"))
        peak_day = str(hottest.idxmax(dim="time").values)[:10]
        print(f"  the single most anomalous cell-day: {float(anomaly.max()):.2f} degC above normal, on {peak_day}.")
        span_lo, span_hi = float(ds["t2m"].min()), float(ds["t2m"].max())
        print(f"  In absolute terms that day is lost in the pack: the field spans {span_lo:.1f} to")
        print(f"  {span_hi:.1f} degC across the whole year, so no single reading looks wrong.")
        print("  That is the point -- the anomaly finds the outlier the absolute value hides.")

        # SECTION: why anomalies get published
        print("\nWhy anomalies are the published product:")
        print("  - Comparable across places: +2 degC means the same in Freetown and Oslo,")
        print("    while 29 degC does not.")
        print("  - Comparable across seasons: a warm January and a warm July are both")
        print("    visible once the seasonal cycle is removed.")
        print("  - The seasonal cycle is the loudest signal in the raw series and is also")
        print("    the least interesting one; subtracting it is how the trend shows up.")
        print("  - Downstream models (malaria risk, crop yield) are fitted on departures")
        print("    from local normal, so anomalies are what they expect as input.")

        # SECTION: summary
        print("\n=== Summary ===")
        print("- climatological_normal() = groupby('time.month').mean(): 12 maps of what is typical")
        print("- The stored daily series collapses from (time, y, x) to (month, y, x)")
        print("- monthly_anomaly() subtracts each day's own month normal, keeping the original shape")
        print("- Anomalies centre on zero by construction, per month and overall")
        print("- One year of data makes a fragile normal; the WMO convention is 30 years, same arithmetic")
        print("- Services publish anomalies because they are comparable across place and season")


if __name__ == "__main__":
    main()
