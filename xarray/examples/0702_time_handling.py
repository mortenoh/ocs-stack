"""Time handling: datetime64 coords, the dt accessor, and calendar notes.

What: inspects the datetime64 time coordinate, pulls calendar fields out with
the .dt accessor (year, month, dayofyear, season), selects by partial date
strings, tours pandas date_range frequencies, and closes with prose on
non-standard calendars (cftime).

Why: every open-climate-service store has a time dimension, and the append-a-
period ingestion loop lives on date arithmetic -- "give me February", "resample
daily to monthly" -- all of which starts with a proper datetime64 coordinate.

Run: make run EXAMPLE=0702_time_handling
"""

import pandas as pd

from climate_stack_xarray import temperature_dataset


def main() -> None:
    """Explore datetime64 time coordinates, the dt accessor, and label-based time selection."""
    # SECTION: the time coordinate is datetime64
    print("A proper time coord is numpy datetime64, built here from pd.date_range:")
    ds = temperature_dataset(days=91, ny=3, nx=4)  # 2024-01-01 .. 2024-03-31 (leap year)
    time = ds["time"]
    print(f"  dtype: {time.dtype}")
    print(f"  span:  {str(time.values[0])[:10]} .. {str(time.values[-1])[:10]} ({time.sizes['time']} days)")
    print("  datetime64 is what unlocks .dt, partial-string selection, and resample")

    # SECTION: the dt accessor
    print("\nThe .dt accessor exposes calendar fields as new DataArrays over time:")
    months = sorted(set(time.dt.month.values.tolist()))
    seasons = sorted(set(time.dt.season.values.tolist()))
    print(f"  time.dt.year[0]        = {int(time.dt.year[0])}")
    print(f"  time.dt.month (unique) = {months}")
    print(f"  time.dt.dayofyear      = {int(time.dt.dayofyear[0])} .. {int(time.dt.dayofyear[-1])}")
    print(f"  time.dt.season (unique)= {seasons}  (meteorological: DJF, MAM, ...)")
    print("  these arrays are what groupby('time.month') and climatologies are built on")

    # SECTION: selecting by partial date strings
    print("\nPartial string selection: a label like '2024-02' means the whole month:")
    feb = ds.sel(time="2024-02")
    print(f"  ds.sel(time='2024-02') -> {feb.sizes['time']} days  (not 28: 2024 is a leap year)")
    window = ds.sel(time=slice("2024-01-15", "2024-02-15"))
    print(f"  ds.sel(time=slice('2024-01-15', '2024-02-15')) -> {window.sizes['time']} days (both ends inclusive)")
    print(f"  mean t2m in February = {float(feb['t2m'].mean()):.2f} degC")

    # SECTION: date_range frequencies
    print("\npd.date_range frequency codes generate the axes you append along:")
    for freq, note in [
        ("D", "calendar day"),
        ("6h", "every 6 hours"),
        ("W-MON", "weekly, anchored on Mondays"),
        ("MS", "month start"),
        ("ME", "month end"),
    ]:
        idx = pd.date_range("2024-01-01", periods=4, freq=freq)
        stamps = ", ".join(str(t)[:16] for t in idx)
        print(f"  freq={freq!r:8} -> {stamps}  ({note})")

    # SECTION: non-standard calendars (prose only)
    print("\nA word on non-standard calendars:")
    print("  Climate models often run on calendars real clocks do not: 'noleap' (no Feb 29)")
    print("  or '360_day' (12 x 30-day months). numpy datetime64 cannot represent those, so")
    print("  xarray decodes them into cftime objects instead (via the cftime package).")
    print("  Most of the API still works -- .dt, sel, resample -- but cftime coords are slower")
    print("  and do not mix with datetime64 axes. xr.date_range(..., calendar='noleap') builds")
    print("  them, and convert_calendar() moves data onto a standard calendar when needed.")
    print("  OCS sources (ERA5, CHIRPS) use the standard calendar, so datetime64 is the norm.")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- a datetime64 time coord is the entry ticket to all of xarray's time features")
    print("- .dt exposes year/month/dayofyear/season as DataArrays -- fuel for groupby")
    print("- sel(time='2024-02') selects a whole month; slices are inclusive on both ends")
    print("- date_range freq codes (D, 6h, MS, ME, W-MON) generate regular time axes")
    print("- non-standard model calendars (noleap, 360_day) need cftime, not datetime64")


if __name__ == "__main__":
    main()
