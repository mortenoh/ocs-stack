"""Appending monthly periods: the shape of an open-climate-service ingest.

What: ingests six calendar months one at a time, each as its own writable
session and its own commit, and watches the time axis and the history grow in
step. Before each append it probes what a naive append would do, which exposes
the chunk-alignment error that variable-length months provoke against a fixed
time chunk -- and the ``align_chunks=True`` fix.

Why: this is the ingest loop OCS runs. One period per commit means the history
is a per-period audit trail, a failure only loses the period in flight, and a
reader always sees whole periods -- never half of March.

Run: make run EXAMPLE=0401_append_periods
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

PERIODS = ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06"]
TIME_CHUNK = 30
NY = 32
NX = 32


def days_in(period: str) -> int:
    """Count the days in a calendar month.

    Args:
        period: Month as ``YYYY-MM``.

    Returns:
        Number of days in that month.
    """
    start = pd.Timestamp(f"{period}-01")
    return int((start + pd.offsets.MonthBegin(1) - start).days)


def month_dataset(period: str, offset: float) -> xr.Dataset:
    """Build one calendar month of daily data, dask-backed like a real ingest.

    Args:
        period: Month as ``YYYY-MM``.
        offset: Constant added to every value, so periods are distinguishable.

    Returns:
        A dataset covering exactly that month, chunked along time.
    """
    start = pd.Timestamp(f"{period}-01")
    ds = climate_dataset(days=days_in(period), ny=NY, nx=NX, start=str(start.date()), offset=offset)
    return ds.chunk({"time": TIME_CHUNK})


def probe_naive_append(repo: Any, ds: xr.Dataset) -> str:
    """Try an append without ``align_chunks`` on a throwaway session.

    The session is always discarded, so the repository is untouched either way.

    Args:
        repo: The repository to probe.
        ds: The dataset that the real append will write.

    Returns:
        A short description of what a naive append would do.
    """
    scratch: Any = repo.writable_session("main")
    try:
        ds.to_zarr(scratch.store, append_dim="time", consolidated=False)
        result = "would have worked unaligned too"
    except ValueError as exc:
        result = f"ValueError: {str(exc).split('. ')[0]}"
    scratch.discard_changes()
    return result


def print_chunk_grid() -> None:
    """Print where each period lands on the store's fixed time-chunk grid."""
    cumulative = 0
    for period in PERIODS:
        days = days_in(period)
        start, end = cumulative, cumulative + days
        touched = (end - 1) // TIME_CHUNK - start // TIME_CHUNK + 1
        note = "one zarr chunk" if touched == 1 else f"{touched} zarr chunks"
        aligned = "aligned" if start % TIME_CHUNK == 0 else f"offset {start % TIME_CHUNK} into a chunk"
        print(f"    {period}: {days} days -> time[{start:>3}:{end:>3}]  touches {note}, starts {aligned}")
        cumulative = end


def main() -> None:
    """Ingest six months one commit at a time and inspect the growing store."""
    quiet_icechunk_logs()

    with tempfile.TemporaryDirectory() as tmp:
        repo: Any = open_repo(Path(tmp) / "era5_daily.icechunk")

        # SECTION: the layout the first write fixes
        print(f"Ingest {len(PERIODS)} monthly periods, one commit each, into one repository.")
        print(f"The first write is mode='w' and fixes the on-disk chunk shape: time chunk = {TIME_CHUNK} days.")
        print("Calendar months are 28-31 days, so periods and chunks drift apart as the axis grows:")
        print_chunk_grid()
        print("  a dask-backed append arrives as one block. If that block lands across two zarr chunks")
        print("  without being aligned to them, xarray refuses: two parallel write tasks would have to")
        print("  read-modify-write the same chunk. align_chunks=True rechunks the incoming data onto")
        print("  the store's grid first, so every write targets whole chunks and the append is legal.")

        # SECTION: period 1 creates the store
        print(f"\nPeriod 1 of {len(PERIODS)} -- mode='w', creating the arrays:")
        first: Any = repo.writable_session("main")
        month_dataset(PERIODS[0], 0.0).to_zarr(
            first.store,
            mode="w",
            zarr_format=3,
            consolidated=False,
            encoding={"t2m": {"chunks": (TIME_CHUNK, NY, NX)}},
        )
        snapshot = str(first.commit(f"ingest {PERIODS[0]}"))
        tip = read_dataset(repo)
        print(f"  {PERIODS[0]}  {snapshot[:8]}  time={tip.sizes['time']:>3}  last={str(tip.time.values[-1])[:10]}")

        # SECTION: the remaining periods, one session and one commit each
        print("\nEach later period: probe the naive append, then append with align_chunks and commit.")
        for index, period in enumerate(PERIODS[1:], start=1):
            month = month_dataset(period, float(index))
            print(f"  {period}  naive append -> {probe_naive_append(repo, month)}")
            session: Any = repo.writable_session("main")
            month.to_zarr(session.store, append_dim="time", consolidated=False, align_chunks=True)
            snapshot = str(session.commit(f"ingest {period}"))
            tip = read_dataset(repo)
            last = str(tip.time.values[-1])[:10]
            print(f"  {period}  {snapshot[:8]}  time={tip.sizes['time']:>3}  last={last}")

        # SECTION: one snapshot per period
        print("\nThe history is one snapshot per period, newest first:")
        for entry in describe_history(repo):
            print(f"  {entry['short']}  {entry['written_at']:%Y-%m-%d %H:%M:%S}  {entry['message']}")

        # SECTION: the finished dataset
        print("\nThe finished dataset:")
        final = read_dataset(repo)
        print(f"  dims={dict(final.sizes)}")
        print(f"  time spans {str(final.time.values[0])[:10]} .. {str(final.time.values[-1])[:10]}")
        print(f"  t2m chunks on disk: {final.t2m.encoding.get('chunks')} -- unchanged by six appends")
        monthly = final.t2m.mean(dim=["y", "x"]).groupby("time.month").mean().compute()
        means = " ".join(
            f"{int(m):02d}:{float(v):.1f}" for m, v in zip(monthly.month.values, monthly.values, strict=True)
        )
        print(f"  mean per month (each period carries its own offset): {means}")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- the ingest loop is: open session, to_zarr(append_dim='time'), commit -- once per period")
    print("- one commit per period gives one auditable snapshot per period in the history")
    print("- a period that fails mid-write commits nothing, so the store never holds half a month")
    print("- the first write fixes the chunk shape; every later append has to live with it")
    print("- variable-length periods against a fixed time chunk make appends straddle chunk edges")
    print("- align_chunks=True rechunks the incoming data onto the store's grid and makes it legal")
    print("- the probe sessions above prove the point without touching the store: no commit, no change")


if __name__ == "__main__":
    main()
