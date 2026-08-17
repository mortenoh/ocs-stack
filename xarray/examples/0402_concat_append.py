"""Concat along time: the append-a-period ingestion pattern.

What: slices a three-month synthetic dataset into one-month "periods", appends
them one at a time with xr.concat along time, verifies the result is monotonic
and unique, then shows what happens when a period overlaps data already
ingested -- and how to detect and repair the duplicate timestamps.

Why: open-climate-service ingestion is exactly this loop: a new period of a
(time, y, x) store arrives, gets concatenated (ultimately zarr append_dim)
onto what exists. concat trusts you -- it never checks for overlap -- so the
monotonic/unique index checks here are the guard rails every append needs.

Run: make run EXAMPLE=0402_concat_append
"""

import xarray as xr

from playground_xarray import temperature_dataset


def main() -> None:
    """Append monthly periods with concat, then detect and fix duplicate times."""
    # SECTION: one long dataset, sliced into monthly periods
    print("A 91-day dataset (Jan-Mar 2024) sliced into the monthly periods a feed would deliver:")
    full = temperature_dataset(days=91)
    periods = [full.sel(time=month) for month in ("2024-01", "2024-02", "2024-03")]
    for month, period in zip(("2024-01", "2024-02", "2024-03"), periods, strict=True):
        print(f"  period {month}: {period.sizes['time']} days")

    # SECTION: the append loop
    print("\nThe append-a-period pattern: start with the first period, concat each arrival onto the store:")
    store = periods[0]
    print(f"  ingest 2024-01 -> store has {store.sizes['time']} days")
    for month, period in zip(("2024-02", "2024-03"), periods[1:], strict=True):
        store = xr.concat([store, period], dim="time")
        print(f"  ingest {month} -> store has {store.sizes['time']} days")

    # SECTION: verifying the result
    print("\nA healthy time axis after appending is monotonic, unique, and matches the source:")
    idx = store.indexes["time"]
    print(f"  monotonic increasing: {idx.is_monotonic_increasing}")
    print(f"  unique timestamps:    {idx.is_unique}")
    print(f"  identical to the unsliced original: {store.identical(full)}")

    # SECTION: what goes wrong -- an overlapping period
    print("\nNow the failure mode: the feed re-sends a period that overlaps the store (Mar 25-31):")
    resend = full.sel(time=slice("2024-03-25", "2024-03-31"))
    bad = xr.concat([store, resend], dim="time")
    bad_idx = bad.indexes["time"]
    print(f"  concat happily produces {bad.sizes['time']} days -- no error, no warning")
    print(f"  monotonic increasing: {bad_idx.is_monotonic_increasing}")
    print(f"  unique timestamps:    {bad_idx.is_unique}")
    print(f"  duplicated labels:    {int(bad_idx.duplicated().sum())}")
    dup_day = bad.sel(time="2024-03-28")
    print(f"  selecting one duplicated day returns {dup_day.sizes['time']} entries -- downstream code breaks here")

    # SECTION: detection and repair
    print("\nDetection is an index check; repair is drop_duplicates (or trim before appending):")
    print("  guard:  assert idx.is_unique and idx.is_monotonic_increasing  # after every append")
    fixed = bad.drop_duplicates(dim="time", keep="first").sortby("time")
    fixed_idx = fixed.indexes["time"]
    print(f"  bad.drop_duplicates(dim='time', keep='first') -> {fixed.sizes['time']} days")
    print(f"  monotonic: {fixed_idx.is_monotonic_increasing}, unique: {fixed_idx.is_unique}")
    print(f"  identical to the pre-overlap store: {fixed.identical(store)}")
    trimmed = resend.sel(time=resend.time > store.time.values[-1])
    print(f"  better: trim the incoming period to time > store max -> {trimmed.sizes['time']} genuinely new days")
    print("  (the resend was entirely old data, so nothing survives the trim -- the append becomes a no-op)")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- xr.concat(parts, dim='time') is the append-a-period pattern for (time, y, x) stores")
    print("- concat in arrival order keeps time monotonic -- but concat never checks overlap")
    print("- duplicate timestamps concat silently; .indexes['time'].is_unique / .duplicated() detect them")
    print("- repair with drop_duplicates(dim='time') + sortby, or trim incoming periods before appending")
    print("- zarr append_dim='time' has the same trust-the-caller contract: guard the index yourself")


if __name__ == "__main__":
    main()
