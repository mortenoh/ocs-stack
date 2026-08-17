"""Groupby climatology: monthly normals and anomalies from three years of dailies.

What: groups three years of daily temperature by calendar month to compute a
climatological normal (the mean January, mean February, ...), then subtracts
that groupby mean from the dailies to get monthly anomalies.

Why: this is the climatology recipe open-climate-service runs on its (time, y, x)
stores: groupby("time.month").mean() collapses years into a 12-step normal, and
"observed minus normal for the same month" is the anomaly product served to
clients. groupby handles the calendar bookkeeping that indexing math gets wrong.

Run: make run EXAMPLE=0303_groupby_climatology
"""

from playground_data_xarray import temperature_dataset


def main() -> None:
    """Compute a monthly climatological normal and monthly anomalies via groupby."""
    # SECTION: three years of daily data
    t2m = temperature_dataset(days=1096)["t2m"]  # 2024 (366) + 2025 (365) + 2026 (365)
    first, last = (str(t2m.time.values[i])[:10] for i in (0, -1))
    print("Three years of daily 2 m temperature:")
    print(f"  dims={t2m.dims}, shape={t2m.shape}, time {first} .. {last}")

    # SECTION: grouping by a datetime component
    print("\ngroupby('time.month') buckets every timestamp by calendar month across ALL years:")
    grouped = t2m.groupby("time.month")
    print(f"  number of groups: {len(grouped.groups)} (three Januaries land in group 1, etc.)")
    jan_days = t2m.sel(time=t2m.time.dt.month == 1).sizes["time"]
    print(f"  group 1 holds {jan_days} time steps (31 January days x 3 years)")

    # SECTION: the climatological normal
    print("\n.mean() over the groups = climatological normal, one field per month:")
    clim = grouped.mean()
    print(f"  clim: dims={clim.dims}, shape={clim.shape}  (time is replaced by month=1..12)")
    print("  area-mean normal by month (the seasonal cycle, degC):")
    for month in (1, 4, 7, 10):
        print(f"    month {month:2d}: {float(clim.sel(month=month).mean()):.3f}")

    # SECTION: anomalies = dailies minus the matching monthly normal
    print("\nSubtracting a groupby mean broadcasts each month's normal back onto its own days:")
    anom = t2m.groupby("time.month") - clim
    print(f"  anom: dims={anom.dims}, shape={anom.shape}  (same shape as the dailies)")
    print("  every July day got the July normal subtracted, not one global mean")
    print(f"  anomaly grand mean:            {float(anom.mean()):+.5f}  (~0 by construction)")
    jan_anom = anom.sel(time=anom.time.dt.month == 1)
    jul_anom = anom.sel(time=anom.time.dt.month == 7)
    print(f"  mean anomaly over January days: {float(jan_anom.mean()):+.5f}")
    print(f"  mean anomaly over July days:    {float(jul_anom.mean()):+.5f}")
    warmest = anom.mean(dim=["y", "x"]).idxmax(dim="time")
    print(f"  most-above-normal day: {str(warmest.values)[:10]}")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- groupby('time.month') buckets by calendar month across years (datetime accessor syntax)")
    print("- grouped.mean() = climatological normal: (time, y, x) -> (month, y, x)")
    print("- groupby minus the groupby mean aligns on the group label: monthly anomalies in one line")
    print("- anomalies keep the original daily shape; each day is measured against its own month")
    print("- this is the OCS climatology/anomaly pipeline in miniature")


if __name__ == "__main__":
    main()
