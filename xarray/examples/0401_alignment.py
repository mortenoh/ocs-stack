"""Alignment: how xarray lines up indexes before doing arithmetic.

What: runs arithmetic on two datasets whose time ranges only partly overlap,
exposing the silent inner-join xarray applies, then makes the join explicit
with xr.align (join="inner"/"outer"/"left") and shows the NaN padding that
outer joins introduce.

Why: open-climate-service combines (time, y, x) stores from many sources —
observations minus climatology, forecast minus reference. Whenever two objects
meet in an expression, xarray aligns their indexes first; if one source is
missing a period, the result silently shrinks to the overlap. Knowing the join
rules is what turns "why did my time axis shrink?" into a one-line diagnosis.

Run: make run EXAMPLE=0401_alignment
"""

import xarray as xr

from climate_stack_xarray import temperature_dataset


def main() -> None:
    """Demonstrate automatic index alignment and explicit joins."""
    # SECTION: two datasets with partially overlapping time ranges
    print("Two temperature datasets sliced from one month, overlapping in the middle:")
    full = temperature_dataset(days=31)
    early = full.isel(time=slice(0, 21))  # Jan 01 .. Jan 21
    late = full.isel(time=slice(14, 31))  # Jan 15 .. Jan 31
    for name, ds in (("early", early), ("late", late)):
        first, last = str(ds.time.values[0])[:10], str(ds.time.values[-1])[:10]
        print(f"  {name}: {ds.sizes['time']:2d} days, {first} .. {last}")
    print("  overlap: 2024-01-15 .. 2024-01-21 (7 days)")

    # SECTION: arithmetic silently inner-joins on the index
    print("\nArithmetic aligns on labels first -- and the default join is INNER:")
    diff = late.t2m - early.t2m
    print(f"  (late.t2m - early.t2m).sizes = {dict(diff.sizes)}")
    print(f"  21 days minus 17 days -> {diff.sizes['time']} days: only labels present in BOTH survive")
    print("  No error, no warning. This surprises people: a missing period in one")
    print("  input quietly shrinks the result instead of failing loudly.")

    # SECTION: xr.align makes the join explicit
    print("\nxr.align returns both objects re-indexed onto a common index, join= chosen by you:")
    for join in ("inner", "outer", "left"):
        a, b = xr.align(early, late, join=join)
        print(f"  join={join!r:8s} -> early: {a.sizes['time']:2d} days, late: {b.sizes['time']:2d} days")
    print("  inner = intersection, outer = union, left = index of the first argument")

    # SECTION: outer joins pad the gaps with NaN
    print("\nAn outer join cannot invent data -- missing labels are filled with NaN:")
    early_out, late_out = xr.align(early, late, join="outer")
    n_cells = early_out.sizes["y"] * early_out.sizes["x"]
    early_nan = int(early_out.t2m.isnull().sum())
    late_nan = int(late_out.t2m.isnull().sum())
    print(f"  outer index: {early_out.sizes['time']} days (the full month)")
    print(f"  early padded with {early_nan} NaNs = 10 missing days x {n_cells} cells")
    print(f"  late  padded with {late_nan} NaNs = 14 missing days x {n_cells} cells")

    # SECTION: NaN propagates through arithmetic on outer-aligned data
    print("\nArithmetic on the outer-aligned pair keeps the full axis but NaNs the gaps:")
    diff_out = late_out.t2m - early_out.t2m
    valid_days = int(diff_out.notnull().all(dim=["y", "x"]).sum())
    print(f"  (late - early) after outer align: {diff_out.sizes['time']} days, {valid_days} fully valid")
    filled = late_out.t2m.fillna(0.0)
    print(
        f"  .fillna(0.0) replaces the padding when a neutral value is correct: {int(filled.isnull().sum())} NaNs left"
    )
    print("  (whether 0.0 is correct depends on the variable -- fine for counts, wrong for temperature)")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- binary operations align indexes automatically; the default join is inner")
    print("- a missing period in either operand silently shrinks the result to the overlap")
    print("- xr.align(a, b, join=...) makes the choice explicit: inner, outer, left, right, exact")
    print("- outer joins pad missing labels with NaN; arithmetic then propagates the NaN")
    print("- join='exact' raises instead of aligning -- the strict guard for pipelines")


if __name__ == "__main__":
    main()
