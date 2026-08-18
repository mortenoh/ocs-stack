"""Indices: hot days, wet days, monthly totals, and a standardized rainfall index.

What: ingests four years of daily temperature and rainfall, then derives the
products a user actually asks for -- hot_days(), wet_days(), monthly_total()
and spi_like() -- and reads a few of the resulting numbers back in prose.

Why: nobody requests "the daily 2m temperature field". They request "how many
hot days did this district get" or "was last season unusually dry". Those are
indices: small reductions over the stored series, which is what the openEO
processes in open-climate-service compute and serve.

Run: make run EXAMPLE=0202_indices
"""

import calendar
import tempfile
from typing import Any

import icechunk  # no type stubs; repository handles stay narrowly typed as Any
import xarray as xr

from ocs_stack_climate_pipeline import (
    enumerate_periods,
    fetch_precipitation,
    fetch_temperature,
    hot_days,
    ingest,
    monthly_total,
    spi_like,
    store_path,
    wet_days,
)
from ocs_stack_climate_pipeline.sources import Period

# Silence the Rust-side WARN chatter icechunk emits during ordinary appends.
icechunk.set_logs_filter("error")

YEARS = (2021, 2022, 2023, 2024)
REPORT_YEAR = 2024
HOT_THRESHOLD = 29.0
# One grid cell, used where a single location reads more clearly than a map mean.
CELL_Y, CELL_X = 6, 6


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


def multi_year_periods() -> list[Period]:
    """List every month of several consecutive years, in chronological order.

    Returns:
        Every period to ingest, so each calendar month has one sample per year.
    """
    periods: list[Period] = []
    for year in YEARS:
        periods.extend(enumerate_periods(year, 12))
    return periods


def ingest_variable(tmp: str, dataset_id: str, periods: list[Period]) -> xr.Dataset:
    """Ingest one variable's periods into its own store and read it back.

    Args:
        tmp: Temporary data directory.
        dataset_id: Which dataset to build, ``"temperature"`` or ``"rainfall"``.
        periods: Periods to ingest.

    Returns:
        The committed dataset.
    """
    repo = make_repo(tmp, dataset_id)
    if dataset_id == "temperature":
        ingest(repo, periods, lambda p: fetch_temperature(p, ny=12, nx=12))
    else:
        ingest(repo, periods, lambda p: fetch_precipitation(p, ny=12, nx=12))
    ds: xr.Dataset = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    return ds


def label(stamp: Any) -> str:
    """Format a month-end timestamp as ``YYYY-Mon``.

    Args:
        stamp: A numpy datetime64 value from a resampled time axis.

    Returns:
        A short year-and-month label.
    """
    text = str(stamp)
    return f"{int(text[:4])}-{calendar.month_abbr[int(text[5:7])]}"


def report_rows(index: xr.DataArray) -> list[int]:
    """Positions along time belonging to the year the tables print.

    Args:
        index: A monthly index with a ``time`` dimension.

    Returns:
        Indices into the time axis for :data:`REPORT_YEAR`.
    """
    return [i for i, stamp in enumerate(index["time"].values) if str(stamp).startswith(str(REPORT_YEAR))]


def same_month_rows(index: xr.DataArray, stamp: Any) -> list[int]:
    """Positions along time sharing a calendar month with one timestamp.

    Args:
        index: A monthly index with a ``time`` dimension.
        stamp: The timestamp whose calendar month is wanted.

    Returns:
        Indices into the time axis for that calendar month in every year.
    """
    month = str(stamp)[5:7]
    return [i for i, other in enumerate(index["time"].values) if str(other)[5:7] == month]


def main() -> None:
    """Derive temperature and rainfall indices over an ingested multi-year series."""
    periods = multi_year_periods()
    print(f"Ingesting {len(periods)} monthly periods -- {YEARS[0]} through {YEARS[-1]} -- into two")
    print("stores, one per variable. Several whole years matter here: an index that says")
    print("'unusually dry' needs more than one sample of what usual looks like.\n")

    with tempfile.TemporaryDirectory() as tmp:
        temperature = ingest_variable(tmp, "temperature", periods)
        rainfall = ingest_variable(tmp, "rainfall", periods)
        print(f"  temperature: dims={dict(temperature.sizes)} units={temperature['t2m'].attrs['units']!r}")
        print(f"  rainfall   : dims={dict(rainfall.sizes)} units={rainfall['tp'].attrs['units']!r}")
        print(f"\n  Every index below is computed over all {len(YEARS)} years; the tables print")
        print(f"  {REPORT_YEAR} so they stay readable.")

        # SECTION: hot days
        print(f"\nIndex 1 -- hot_days(threshold={HOT_THRESHOLD}). A threshold count, not an average:")
        print("heat stress is about how often it crossed a line, and an average hides that.\n")
        hot = hot_days(temperature, threshold=HOT_THRESHOLD)
        hot_area = hot.mean(dim=("y", "x"))
        monthly_mean_t = temperature["t2m"].resample(time="1ME").mean().mean(dim=("y", "x"))
        print(f"  {'month':>9}  {'mean degC':>9}  {'hot days':>8}  {'of':>3}")
        for i in report_rows(hot):
            stamp = hot["time"].values[i]
            days = calendar.monthrange(int(str(stamp)[:4]), int(str(stamp)[5:7]))[1]
            print(f"  {label(stamp):>9}  {float(monthly_mean_t[i]):>9.2f}  {float(hot_area[i]):>8.1f}  {days:>3}")
        print(f"\n  result dims={hot.dims}, units={hot.attrs['units']!r} -- {hot.attrs['long_name']}")
        print(f"  whole record: {float(hot.sum()) / (hot.sizes['y'] * hot.sizes['x']) / len(YEARS):.0f} hot days")
        print(f"  in an average year at an average cell, against a {HOT_THRESHOLD} degC line.")
        print("  Two months can share a mean and differ completely in hot-day count, because")
        print("  a count is sensitive to the shape of the distribution and a mean is not.")
        print("  The count is the thing a health ministry can plan around.")

        # SECTION: wet days
        print("\nIndex 2 -- wet_days(threshold=1.0). The same construction for rainfall,")
        print("with the conventional 1 mm cutoff:\n")
        wet = wet_days(rainfall, threshold=1.0)
        wet_area = wet.mean(dim=("y", "x"))
        drizzle = wet_days(rainfall, threshold=0.0).mean(dim=("y", "x"))
        print(f"  {'month':>9}  {'wet days >=1mm':>14}  {'days with any trace':>19}")
        for i in report_rows(wet):
            print(f"  {label(wet['time'].values[i]):>9}  {float(wet_area[i]):>14.1f}  {float(drizzle[i]):>19.1f}")
        gap = float(drizzle.mean() - wet_area.mean())
        print(f"\n  On average {gap:.1f} days a month register something but less than 1 mm.")
        print("  1 mm is the conventional wet-day cutoff because below it a reading is")
        print("  indistinguishable from dew, gauge wetting, or a satellite retrieval's noise")
        print("  floor. Count those and 'rainy days per year' measures instrument")
        print("  sensitivity rather than climate -- and stops being comparable between")
        print("  places, which is the entire purpose of an index.")

        # SECTION: intensive vs extensive
        print("\nIndex 3 -- monthly_total(). Rainfall is summed and temperature is averaged,")
        print("and the distinction is not a style preference:\n")
        totals = monthly_total(rainfall)
        totals_area = totals.mean(dim=("y", "x"))
        as_mean = rainfall["tp"].resample(time="1ME").mean().mean(dim=("y", "x"))
        as_sum = temperature["t2m"].resample(time="1ME").sum().mean(dim=("y", "x"))
        print(f"  {'month':>9}  {'total mm':>9}  {'mean mm/day':>11}")
        rows = report_rows(totals)
        for i in rows:
            month, total, per_day = label(totals["time"].values[i]), float(totals_area[i]), float(as_mean[i])
            print(f"  {month:>9}  {total:>9.1f}  {per_day:>11.2f}")
        top = max(rows, key=lambda i: float(totals_area[i]))
        top_label, top_total, top_rate = label(totals["time"].values[top]), float(totals_area[top]), float(as_mean[top])
        print(f"\n  Rainfall is EXTENSIVE -- it accumulates. {top_label} received {top_total:.0f} mm; its")
        print(f"  daily mean of {top_rate:.2f} mm/day is a true number that answers no question")
        print("  anyone asked, and it shrinks if you lengthen the month while the rain that")
        print("  fell stays the same.")
        print(f"  Temperature is INTENSIVE -- summing it gives {float(as_sum[rows[0]]):.0f} 'degC' for the month,")
        print("  a quantity with no physical meaning. But it is a float, it carries a unit")
        print("  attribute, it plots, and nothing in xarray or zarr will ever object. That")
        print("  is what makes the intensive/extensive mix-up a silent error, not a crash.")

        # SECTION: a standardized index
        print("\nIndex 4 -- spi_like(). Monthly totals standardized against the same calendar")
        print("month's own history, so the answer is 'unusual for a March here' rather than")
        print("'a lot of rain compared to the dry season'.\n")
        spi = spi_like(rainfall)
        cell_spi = spi.isel(y=CELL_Y, x=CELL_X)
        cell_totals = totals.isel(y=CELL_Y, x=CELL_X)
        print(f"  Read at one grid cell (y={CELL_Y}, x={CELL_X}); a map mean would average")
        print(f"  the local variability away. Each month is standardized against its {len(YEARS)}")
        print("  counterparts in the other years.\n")
        print(f"  {'month':>9}  {'total mm':>9}  {'SPI':>6}  reading")
        for i in report_rows(spi):
            value = float(cell_spi[i])
            reading = "drier than usual" if value <= -1.0 else "wetter than usual" if value >= 1.0 else "near normal"
            print(f"  {label(spi['time'].values[i]):>9}  {float(cell_totals[i]):>9.1f}  {value:>6.2f}  {reading}")

        scores = [float(v) for v in cell_spi.values]
        driest, wettest = scores.index(min(scores)), scores.index(max(scores))
        d_stamp, w_stamp = spi["time"].values[driest], spi["time"].values[wettest]
        d_month = calendar.month_name[int(str(d_stamp)[5:7])]
        w_month = calendar.month_name[int(str(w_stamp)[5:7])]
        d_average = float(cell_totals[same_month_rows(spi, d_stamp)].mean())
        w_average = float(cell_totals[same_month_rows(spi, w_stamp)].mean())
        print(f"\n  Reading the two extremes of the whole {len(YEARS)}-year record out loud:")
        print(f"  - {label(d_stamp)} scored {scores[driest]:.2f}: that month was {abs(scores[driest]):.2f} standard")
        print(f"    deviations DRIER than a typical {d_month} at this cell, receiving")
        print(f"    {float(cell_totals[driest]):.0f} mm against a {d_month} average of {d_average:.0f} mm.")
        print(f"  - {label(w_stamp)} scored {scores[wettest]:.2f}: {abs(scores[wettest]):.2f} standard deviations")
        print(f"    WETTER than a typical {w_month}, receiving {float(cell_totals[wettest]):.0f} mm against a")
        print(f"    {w_month} average of {w_average:.0f} mm.")
        print("  Notice that the two are not comparable in millimetres and are perfectly")
        print("  comparable in SPI. That is the whole trick: each month is judged against")
        print("  its own season, so a wet dry-season month can outscore a wet monsoon one.")
        print(f"\n  units={spi.attrs['units']!r} -- dimensionless, which is exactly why the same")
        print("  number is comparable between a rainforest and a savanna, and why drought")
        print("  monitoring can use one threshold everywhere (about -1.5 to raise a flag).")
        print(f"  Sample size caveat: each calendar month has {len(YEARS)} years behind it here. Real")
        print("  SPI wants 30, and fits a gamma distribution first, because monthly rainfall")
        print("  is skewed and zero-inflated rather than normal. The standardization idea --")
        print("  subtract this month's mean, divide by its spread -- is unchanged.")

        # SECTION: summary
        print("\n=== Summary ===")
        print("- hot_days() and wet_days() are threshold counts: how often a line was crossed, not an average")
        print("- 1 mm is the conventional wet-day cutoff; below it a reading is instrument noise, not weather")
        print("- monthly_total() sums rainfall because rainfall is extensive; temperature is intensive and averaged")
        print("- Using mean where sum belongs (or the reverse) yields a plausible number and no error at all")
        print("- spi_like() standardizes each month against its own history: dimensionless and comparable anywhere")
        print("- Every index here is a small reduction over the stored series -- which is what a service publishes")


if __name__ == "__main__":
    main()
