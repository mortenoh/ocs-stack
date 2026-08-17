"""Resample: daily-to-monthly aggregation and the sum-vs-mean choice per variable.

What: resamples one year of daily temperature and precipitation to monthly
frequency with resample(time="1ME"), contrasting .mean() (right for intensive
variables like temperature) with .sum() (right for accumulations like rainfall).

Why: open-climate-service serves monthly products from daily (time, y, x)
stores, keyed by ISO periods (2024-01 = P1M). resample is the calendar-aware
way to get there, and picking sum vs mean per variable is a semantic decision
the code must make -- xarray will happily compute the wrong one.

Run: make run EXAMPLE=0304_resample
"""

from playground_xarray import precipitation_dataset, temperature_dataset


def main() -> None:
    """Resample daily fields to monthly and compare mean vs sum semantics."""
    # SECTION: one year of daily data
    days = 366  # all of 2024
    t2m = temperature_dataset(days=days)["t2m"]
    tp = precipitation_dataset(days=days)["tp"]
    print("One year of daily data on a (time, y, x) grid:")
    print(f"  t2m [degC]:   shape={t2m.shape}")
    print(f"  tp  [mm/day]: shape={tp.shape}")

    # SECTION: daily -> monthly with resample
    print("\nresample(time='1ME') buckets by calendar month ('ME' = month-end frequency),")
    print("then a reduction collapses each bucket -- 366 days -> 12 months:")
    t2m_monthly = t2m.resample(time="1ME").mean()
    print(f"  t2m.resample(time='1ME').mean(): shape={t2m_monthly.shape}")
    labels = [str(t)[:10] for t in t2m_monthly.time.values[:3]]
    print(f"  new time labels are bucket ends: {labels} ...")
    print("  calendar-aware: February 2024 contributes 29 days, January 31 -- no fixed window math")

    # SECTION: mean semantics -- intensive variables
    print("\nTemperature is intensive: 'the mean January temperature' is meaningful, a sum is not.")
    print("  area-mean monthly temperature (degC):")
    for i in (0, 3, 6):
        month = str(t2m_monthly.time.values[i])[:7]
        print(f"    {month}: {float(t2m_monthly.isel(time=i).mean()):8.3f}")
    t2m_summed = t2m.resample(time="1ME").sum()
    print(f"  .sum() on temperature 'works' but is nonsense: January -> {float(t2m_summed.isel(time=0).mean()):.1f}")

    # SECTION: sum semantics -- accumulations
    print("\nPrecipitation is an accumulation: monthly TOTAL (mm) is .sum(); .mean() is intensity (mm/day):")
    tp_total = tp.resample(time="1ME").sum()
    tp_rate = tp.resample(time="1ME").mean()
    print("  month     sum [mm]   mean [mm/day]   mean * days_in_month")
    for i in (0, 1):
        stamp = tp_total.time.values[i]
        month = str(stamp)[:7]
        n_days = int(tp.sel(time=tp.time.dt.month == i + 1).sizes["time"])
        total = float(tp_total.isel(time=i).mean())
        rate = float(tp_rate.isel(time=i).mean())
        print(f"  {month}   {total:8.2f}   {rate:13.3f}   {rate * n_days:8.2f}  ({n_days} days)")
    print("  sum = mean * days-in-month, so the two only agree if you track month length -- resample does")

    # SECTION: ISO-period thinking
    print("\nClimate stores think in ISO 8601 periods: a monthly value is the period 2024-01 (duration P1M),")
    print("not the instant '2024-01-31'. resample's bucket labels map 1:1 onto those period keys, which is")
    print("how OCS addresses monthly slices when appending to or querying a store.")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- resample(time='1ME') gives calendar-aware daily -> monthly buckets (28/29/30/31 days each)")
    print("- pick the reduction per variable: .mean() for intensive (temperature),")
    print("  .sum() for accumulations (precipitation totals)")
    print("- monthly mean precip is an intensity in mm/day; the total is mean * days-in-month")
    print("- monthly buckets correspond to ISO periods (2024-01 / P1M) -- the keys climate stores use")


if __name__ == "__main__":
    main()
