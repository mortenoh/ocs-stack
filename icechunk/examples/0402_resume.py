"""Resuming an interrupted ingest by asking the store what it already has.

What: ingests three of six monthly periods, then "crashes". A second run opens
the repository, reads the committed time coordinate, derives which periods are
already present, and ingests only the missing ones. Running the resume a second
time is a no-op, because the store now says everything is there.

Why: OCS ingest jobs are restarted -- by a scheduler, by an operator, after a
pod eviction. Any bookkeeping kept beside the data (a status row, a marker
file, a log) can disagree with the data after a crash. The committed time
coordinate cannot, because it only exists if the commit that wrote it landed.

Run: make run EXAMPLE=0402_resume
"""

import tempfile
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import xarray as xr

from ocs_stack_icechunk import (
    climate_dataset,
    describe_history,
    open_repo,
    quiet_icechunk_logs,
    read_dataset,
)

PERIODS = ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06"]
TIME_CHUNK = 30
NY = 16
NX = 16


def month_dataset(period: str) -> xr.Dataset:
    """Build one calendar month of daily data, offset by its position in the year.

    Args:
        period: Month as ``YYYY-MM``.

    Returns:
        A dataset covering exactly that month, chunked along time.
    """
    start = pd.Timestamp(f"{period}-01")
    days = int((start + pd.offsets.MonthBegin(1) - start).days)
    offset = float(start.month)
    ds = climate_dataset(days=days, ny=NY, nx=NX, start=str(start.date()), offset=offset)
    return ds.chunk({"time": TIME_CHUNK})


def committed_periods(repo: Any) -> set[str]:
    """Derive the set of periods already present from the committed time axis.

    An empty repository -- one where nothing has been written to ``main`` yet --
    has no arrays to read, so it reports no periods.

    Args:
        repo: The repository to interrogate.

    Returns:
        The ``YYYY-MM`` periods that have at least one committed time step.
    """
    try:
        ds = read_dataset(repo)
    except Exception:  # a repository with nothing committed to main raises from the zarr layer
        return set()
    times = pd.to_datetime(np.asarray(ds.time.values))
    return {f"{ts.year:04d}-{ts.month:02d}" for ts in times}


def ingest(repo: Any, periods: list[str], *, stop_after: int | None = None) -> list[str]:
    """Ingest the periods that are not already committed, one commit each.

    Args:
        repo: The repository to write into.
        periods: The full list of periods the dataset should end up holding.
        stop_after: When given, raise after this many periods to simulate a crash.

    Returns:
        The periods this call actually ingested.

    Raises:
        RuntimeError: When ``stop_after`` periods have been ingested.
    """
    present = committed_periods(repo)
    todo = [p for p in periods if p not in present]
    print(f"  store already holds {sorted(present)}")
    print(f"  still to ingest:     {todo}")

    done: list[str] = []
    for period in todo:
        session: Any = repo.writable_session("main")
        month = month_dataset(period)
        if present or done:
            month.to_zarr(session.store, append_dim="time", consolidated=False, align_chunks=True)
        else:
            month.to_zarr(
                session.store,
                mode="w",
                zarr_format=3,
                consolidated=False,
                encoding={"t2m": {"chunks": (TIME_CHUNK, NY, NX)}},
            )
        snapshot = str(session.commit(f"ingest {period}"))
        done.append(period)
        print(f"  committed {period} as {snapshot[:8]}")
        if stop_after is not None and len(done) >= stop_after:
            raise RuntimeError(f"job killed after {stop_after} periods")
    return done


def main() -> None:
    """Interrupt an ingest, then resume it from the committed time coordinate."""
    quiet_icechunk_logs()

    with tempfile.TemporaryDirectory() as tmp:
        repo: Any = open_repo(Path(tmp) / "era5_daily.icechunk")

        # SECTION: the first run dies partway through
        print(f"Run 1 -- ingest {PERIODS} but the job is killed after 3 periods.")
        try:
            ingest(repo, PERIODS, stop_after=3)
        except RuntimeError as exc:
            print(f"  {exc}")

        print("\nWhat survived the crash is exactly what was committed:")
        tip = read_dataset(repo)
        print(f"  time={tip.sizes['time']}  spans {str(tip.time.values[0])[:10]} .. {str(tip.time.values[-1])[:10]}")
        print(f"  periods present: {sorted(committed_periods(repo))}")

        # SECTION: the store is the source of truth
        print("\nNothing external recorded that. The state is derived from the data itself:")
        print("  read the branch tip, take ds.time, and map each timestamp to its period")
        print("  no status table, no marker file, nothing that can drift out of sync with the chunks")
        print("  a period appears in that set only because its commit landed -- see 0301")

        # SECTION: the resume run
        print("\nRun 2 -- the scheduler restarts the same job. It re-derives its own to-do list:")
        ingested = ingest(repo, PERIODS)
        print(f"  this run ingested {ingested}")

        # SECTION: idempotence
        print("\nRun 3 -- the job runs again with nothing left to do:")
        ingested = ingest(repo, PERIODS)
        print(f"  this run ingested {ingested} -- resuming is idempotent, so a retry is always safe")

        # SECTION: the result
        print("\nThe finished dataset and its history:")
        final = read_dataset(repo)
        print(
            f"  dims={dict(final.sizes)}  spans {str(final.time.values[0])[:10]} .. {str(final.time.values[-1])[:10]}"
        )
        monthly = final.t2m.mean(dim=["y", "x"]).groupby("time.month").mean().compute()
        means = " ".join(
            f"{int(m):02d}:{float(v):.1f}" for m, v in zip(monthly.month.values, monthly.values, strict=True)
        )
        print(f"  mean per month: {means}")
        print("  one snapshot per period, whichever run wrote it:")
        for entry in describe_history(repo):
            print(f"    {entry['short']}  {entry['message']}")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- an interrupted ingest leaves whole committed periods and nothing else")
    print("- read ds.time from the branch tip to derive which periods are already present")
    print("- the to-do list is then the full period list minus that set")
    print("- the store is the source of truth; external bookkeeping can outlive a failed write")
    print("- because the to-do list is derived, re-running a finished ingest is a no-op")
    print("- history shows one snapshot per period regardless of how many runs it took")


if __name__ == "__main__":
    main()
