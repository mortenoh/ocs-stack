"""Boolean masks, where(), and isin on zero-inflated precipitation.

What: builds boolean masks over a precipitation cube, applies them with
where() in keep-shape and drop modes, selects time steps with isin, and counts
wet days per pixel — the standard moves on rainfall data that is mostly zeros.

Why: open-climate-service serves derived indicators, and rainfall indicators
are all masks — wet-day counts, dry spells, "rain above threshold" flags over
(time, y, x). Zero-inflated data also makes the keep-vs-drop distinction
concrete: masking to NaN preserves the grid, dropping destroys it.

Run: make run EXAMPLE=0203_masking
"""

from ocs_stack_xarray import precipitation_dataset


def main() -> None:
    """Mask, filter, and count events on a zero-inflated rainfall cube."""
    # SECTION: zero-inflated rainfall
    print("A month of daily precipitation -- most cells are exactly zero:")
    ds = precipitation_dataset(days=30)
    tp = ds.tp
    dry_frac = float((tp == 0.0).mean())
    print(f"  sizes: {dict(ds.sizes)}  ({tp.size} cells)")
    print(f"  {dry_frac:.0%} of cells are 0.0 mm/day; max is {float(tp.max()):.1f} mm/day")

    # SECTION: boolean masks are DataArrays
    print("\nA comparison produces a boolean DataArray with the same dims:")
    wet = tp > 0.0
    print(f"  (tp > 0.0) -> dtype {wet.dtype}, sizes {dict(wet.sizes)}")
    print(f"  mask.mean() is the wet fraction: {float(wet.mean()):.3f}")
    print(f"  masks combine with & | ~ :  heavy = wet & (tp > 10) -> {int((wet & (tp > 10.0)).sum())} cells")

    # SECTION: where() keeping shape
    print("\nwhere(mask) KEEPS the grid and writes NaN where the mask is False:")
    wet_only = tp.where(wet)
    print(f"  tp.where(tp > 0) -> sizes unchanged {dict(wet_only.sizes)}, valid cells: {int(wet_only.count())}")
    print(f"  mean over ALL days      (zeros included): {float(tp.mean()):.2f} mm/day")
    print(f"  mean over WET cells only (NaN skipped):   {float(wet_only.mean()):.2f} mm/day  -- rain intensity")
    print("  keeping shape is what lets the result go straight back into a (time, y, x) store")

    # SECTION: where() dropping
    print("\nwhere(mask, drop=True) SHRINKS the array to labels where the mask holds:")
    ts = tp.mean(dim=["y", "x"])
    rainy = ts.where(ts > 3.5, drop=True)
    print(f"  areal-mean series: {ts.sizes['time']} days; days above 3.5 mm/day: {rainy.sizes['time']}")
    print(f"  surviving labels:  {[str(t)[:10] for t in rainy.time.values]}")
    print("  drop=True only pays off on 1-d selections; in N-d it keeps the bounding box, NaN-padded")

    # SECTION: isin for membership tests
    print("\nisin() tests membership -- ideal for calendar-style selection:")
    weekend_mask = ds.time.dt.dayofweek.isin([5, 6])
    weekends = ds.sel(time=weekend_mask)
    print(f"  ds.time.dt.dayofweek.isin([5, 6]) -> {int(weekend_mask.sum())} of {ds.sizes['time']} days are weekend")
    print(f"  ds.sel(time=<mask>) -> sizes {dict(weekends.sizes)}")
    print(f"  weekend mean rain {float(weekends.tp.mean()):.2f} vs overall {float(tp.mean()):.2f} mm/day")

    # SECTION: counting wet days per pixel
    print("\nCounting events = sum a boolean mask over time (True counts as 1):")
    wet_days = (tp > 1.0).sum(dim="time")
    print(f"  (tp > 1.0).sum(dim='time') -> a (y, x) map, sizes {dict(wet_days.sizes)}")
    print(
        f"  wet days (> 1 mm) per pixel: min {int(wet_days.min())}, "
        f"max {int(wet_days.max())}, mean {float(wet_days.mean()):.1f} of {ds.sizes['time']}"
    )
    print("  this map IS the OCS wet-days indicator: one reduction over a mask, per pixel")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- comparisons give boolean DataArrays; combine with & | ~, reduce like numbers")
    print("- where(mask) keeps shape and inserts NaN -- store-friendly")
    print("- where(mask, drop=True) trims labels -- best on 1-d series")
    print("- isin() does membership tests; pairs well with the .dt accessor")
    print("- event counts = mask.sum(dim='time'); NaN-aware means need where + skipna")


if __name__ == "__main__":
    main()
