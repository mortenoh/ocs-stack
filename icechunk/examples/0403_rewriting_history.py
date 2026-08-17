"""Correcting a bad period: fix forward, and keep the bad snapshot readable.

What: ingests four monthly periods, one of them from a broken upstream feed.
The fix is a new commit that overwrites that period's region -- not an edit of
the old snapshot, which is immutable. The bad snapshot stays in history and
stays readable, which is shown by reading it back and diffing it against the
tip.

Why: OCS republishes periods when an upstream provider reissues data. Knowing
what was served on a given day is an audit requirement; keeping every superseded
version forever is a storage bill. This example makes both sides concrete.

Run: make run EXAMPLE=0403_rewriting_history
"""

import tempfile
from pathlib import Path
from typing import Any

import pandas as pd
import xarray as xr

from playground_icechunk import (
    climate_dataset,
    describe_history,
    open_repo,
    quiet_icechunk_logs,
    read_dataset,
)

PERIODS = ["2024-01", "2024-02", "2024-03", "2024-04"]
TIME_CHUNK = 31
NY = 24
NX = 24
BAD_PERIOD = "2024-02"


def month_dataset(period: str, offset: float) -> xr.Dataset:
    """Build one calendar month of daily data.

    Args:
        period: Month as ``YYYY-MM``.
        offset: Constant added to every value.

    Returns:
        A dataset covering exactly that month.
    """
    start = pd.Timestamp(f"{period}-01")
    days = int((start + pd.offsets.MonthBegin(1) - start).days)
    return climate_dataset(days=days, ny=NY, nx=NX, start=str(start.date()), offset=offset)


def period_slice(ds: xr.Dataset, period: str) -> slice:
    """Find the positional slice a period occupies on a dataset's time axis.

    Args:
        ds: Dataset whose time axis to search.
        period: Month as ``YYYY-MM``.

    Returns:
        A positional slice covering that period.

    Raises:
        ValueError: If the period is not present on the time axis.
    """
    times = pd.DatetimeIndex(ds.time.values)
    hits = [i for i, ts in enumerate(times) if f"{ts.year:04d}-{ts.month:02d}" == period]
    if not hits:
        raise ValueError(f"period {period} is not present on the time axis")
    return slice(hits[0], hits[-1] + 1)


def period_mean(ds: xr.Dataset, period: str) -> float:
    """Average a dataset over one period.

    Args:
        ds: Dataset to reduce.
        period: Month as ``YYYY-MM``.

    Returns:
        The mean of ``t2m`` over that period.
    """
    return float(ds.t2m.isel(time=period_slice(ds, period)).mean())


def show_periods(label: str, ds: xr.Dataset) -> None:
    """Print one mean per period for a dataset.

    Args:
        label: Prefix identifying the snapshot this view came from.
        ds: Dataset to summarize.
    """
    present = [
        p for p in PERIODS if any(f"{ts.year:04d}-{ts.month:02d}" == p for ts in pd.DatetimeIndex(ds.time.values))
    ]
    values = "  ".join(f"{p}={period_mean(ds, p):7.1f}" for p in present)
    print(f"  {label}: {values}")


def main() -> None:
    """Ingest a bad period, correct it forward, and read the bad snapshot back."""
    quiet_icechunk_logs()

    with tempfile.TemporaryDirectory() as tmp:
        repo: Any = open_repo(Path(tmp) / "era5_daily.icechunk")

        # SECTION: an ingest where one period is wrong
        print("Ingest four months. The upstream feed for 2024-02 is broken and delivers +900 degC.")
        snapshots: dict[str, str] = {}
        for index, period in enumerate(PERIODS):
            offset = 900.0 if period == BAD_PERIOD else float(index)
            session: Any = repo.writable_session("main")
            month = month_dataset(period, offset)
            if index == 0:
                month.to_zarr(
                    session.store,
                    mode="w",
                    zarr_format=3,
                    consolidated=False,
                    encoding={"t2m": {"chunks": (TIME_CHUNK, NY, NX)}},
                )
            else:
                month.to_zarr(session.store, append_dim="time", consolidated=False)
            snapshots[period] = str(session.commit(f"ingest {period}"))
            print(f"  {period}  {snapshots[period][:8]}")

        bad_tip = read_dataset(repo)
        show_periods("as served", bad_tip)
        print(f"  2024-02 is obviously wrong: {period_mean(bad_tip, BAD_PERIOD):.1f} degC is not a temperature")

        # SECTION: the correction is a new commit, not an edit
        print(f"\nThe fix is forward: rewrite only {BAD_PERIOD}'s region in a new commit.")
        print("  the snapshot that holds the bad data is immutable -- nothing can edit it in place")
        target = period_slice(bad_tip, BAD_PERIOD)
        print(f"  {BAD_PERIOD} occupies time[{target.start}:{target.stop}], so that is the region to write")
        session = repo.writable_session("main")
        corrected = month_dataset(BAD_PERIOD, 1.0).drop_vars("time")
        corrected.to_zarr(session.store, region={"time": target}, consolidated=False)
        snapshots["fix"] = str(session.commit(f"correct {BAD_PERIOD}: upstream reissued the month"))
        print(f"  committed {snapshots['fix'][:8]}")

        fixed_tip = read_dataset(repo)
        show_periods("tip now  ", fixed_tip)
        print(f"  time is still {fixed_tip.sizes['time']} steps -- a correction replaces values, not the axis")

        # SECTION: the bad snapshot is still there
        print("\nThe bad snapshot did not go anywhere. Read it by id:")
        for label, key in ((f"{BAD_PERIOD} ingest", BAD_PERIOD), ("last ingest", PERIODS[-1]), ("correction", "fix")):
            snapshot_id = snapshots[key]
            view = read_dataset(repo, snapshot_id=snapshot_id)
            print(f"  {snapshot_id[:8]}  {label:<16}  {BAD_PERIOD} mean = {period_mean(view, BAD_PERIOD):8.1f} degC")

        print("\nDiffing the snapshot before the fix against the tip shows exactly what changed:")
        before = read_dataset(repo, snapshot_id=snapshots[PERIODS[-1]])
        delta = (fixed_tip.t2m - before.t2m).mean(dim=["y", "x"])
        changed = [f"{p}={float(delta.isel(time=period_slice(fixed_tip, p)).mean()):.1f}" for p in PERIODS]
        print(f"  mean change per period: {'  '.join(changed)}")
        print("  only 2024-02 moved; the other three periods hold exactly the values they held before")
        chunk_index = target.start // TIME_CHUNK
        region = f"time[{target.start}:{target.stop}]"
        print(f"  the unit of rewriting is the chunk, though: the time chunk is {TIME_CHUNK} days, so")
        print(f"  {region} lands inside chunk {chunk_index}, which also holds the first days of 2024-03")
        print("  -- those March values are read and written straight back out, unchanged")
        print("  so the correction costs one period's worth of chunks, not the whole dataset")

        # SECTION: the history reads as an audit trail
        print("\nThe history records both the mistake and the correction:")
        for entry in describe_history(repo):
            print(f"  {entry['short']}  {entry['written_at']:%H:%M:%S}  {entry['message']}")

        # SECTION: the trade-off
        print("\nWhen keeping the bad snapshot is a feature:")
        print("  a user reports an anomaly in a report generated last Tuesday -- you can reproduce")
        print("  exactly what the service returned then, instead of arguing about it")
        print("  a downstream model trained on the bad month can be identified and retrained")
        print("  corrections are visible: 'this month was reissued' is in the log, not in a ticket")
        print("\nWhen it is a cost:")
        print("  the bad month's chunks stay on disk as long as any reference reaches that snapshot")
        print("  a feed that reissues the same period nightly accumulates one full copy per night")
        print("  the fix is a retention policy: expire old snapshots, then garbage-collect -- see 0502")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- snapshots are immutable; you never edit history, you commit a correction on top of it")
    print("- a region write replaces just the bad period's chunks and leaves the time axis alone")
    print("- the superseded snapshot stays readable by id, so what was served stays reproducible")
    print("- rewriting happens at chunk granularity, so a correction costs one period, not the dataset")
    print("- keeping every superseded version is auditability bought with storage")
    print("- expiry plus garbage collection is how you choose where on that trade-off to sit")


if __name__ == "__main__":
    main()
