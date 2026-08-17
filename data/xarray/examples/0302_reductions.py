"""Reductions: mean/sum/std/min/max over named dims, and skipna with NaN holes.

What: reduces a (time, y, x) temperature field over single and multiple named
dims, then punches NaN holes into the data with where() and shows how skipna
controls whether reductions ignore or propagate the gaps.

Why: every open-climate-service product is a reduction over named dims of a
(time, y, x) store -- spatial means for time series, time means for maps. Real
stores have NaN holes (ocean pixels, missing source days), so the skipna
default is what keeps those products finite.

Run: make run EXAMPLE=0302_reductions
"""

from playground_data_xarray import temperature_dataset


def main() -> None:
    """Reduce over named dims and contrast skipna=True with skipna=False."""
    t2m = temperature_dataset(days=31)["t2m"]
    print("Daily 2 m temperature field:")
    print(f"  dims={t2m.dims}, shape={t2m.shape}")

    # SECTION: the reduction family over one named dim
    print("\nEvery reduction takes dim= by NAME; reducing 'time' leaves a (y, x) map:")
    for name, reduced in (
        ("mean", t2m.mean(dim="time")),
        ("sum", t2m.sum(dim="time")),
        ("std", t2m.std(dim="time")),
        ("min", t2m.min(dim="time")),
        ("max", t2m.max(dim="time")),
    ):
        print(f"  t2m.{name}(dim='time'): shape={reduced.shape}, grand {name} of map = {float(reduced.mean()):.3f}")

    # SECTION: multi-dim reductions
    print("\nPassing a list of dims reduces several at once:")
    series = t2m.mean(dim=["y", "x"])
    print(f"  t2m.mean(dim=['y', 'x']): dims={series.dims}, shape={series.shape}  (area-mean time series)")
    print(f"  t2m.mean(dim=['time', 'y', 'x']) = {float(t2m.mean(dim=['time', 'y', 'x'])):.3f}  (scalar)")
    print(f"  t2m.mean() reduces ALL dims:      {float(t2m.mean()):.3f}  (same scalar)")

    # SECTION: punching NaN holes with where()
    print("\nwhere(cond) keeps values where cond is True and inserts NaN elsewhere.")
    print("Mask out the eastern third of the grid, like ocean pixels outside a country border:")
    holey = t2m.where(t2m.x < -11.5)
    n_nan = int(holey.isnull().sum())
    print(f"  NaN cells: {n_nan} of {holey.size} ({100.0 * n_nan / holey.size:.1f} percent)")

    # SECTION: skipna semantics
    print("\nReductions skip NaN by default (skipna=True), so stats cover only real data:")
    print(f"  full field mean:            {float(t2m.mean()):.3f}")
    print(f"  holey.mean()  [skipna=True] {float(holey.mean()):.3f}  (mean of the surviving western cells)")
    print("\nskipna=False propagates NaN: one hole poisons every reduction that touches it:")
    print(f"  holey.mean(skipna=False)                = {float(holey.mean(skipna=False)):.3f}")
    strict_map = holey.mean(dim="time", skipna=False)
    print(f"  holey.mean(dim='time', skipna=False):     {int(strict_map.isnull().sum())} of {strict_map.size} map")
    print("    cells are NaN -- exactly the masked columns, since their whole time axis is NaN")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- mean/sum/std/min/max all take dim= by name; no axis numbers")
    print("- dim=['y', 'x'] reduces several dims at once; no dim reduces everything")
    print("- where(cond) is the deliberate way to punch NaN holes (masking)")
    print("- skipna=True (default): reductions ignore NaN and use the remaining data")
    print("- skipna=False: any NaN in the reduced window makes the result NaN")


if __name__ == "__main__":
    main()
