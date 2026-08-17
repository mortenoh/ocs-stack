"""Resume: interrupt an ingest, then let the store tell you where to restart.

What: runs ingest(..., stop_after=2) to fake a crash, asks committed_periods()
what the store actually holds, re-runs ingest() and watches it skip what is
present. Then ingests through a source that fails for one month and shows the
run completing around the hole.

Why: open-climate-service ingests years of data over unreliable networks. Runs
die. The recovery rule that survives that is: the store's own time coordinate
is the source of truth. No sidecar manifest, no job database, nothing that can
disagree with the data -- because anything that can disagree eventually will.

Run: make run EXAMPLE=0103_resume
"""

import tempfile
from typing import Any

import icechunk  # no type stubs; repository handles stay narrowly typed as Any
import xarray as xr

from playground_domain_climate import (
    committed_periods,
    enumerate_periods,
    fetch_temperature,
    ingest,
    store_path,
)
from playground_domain_climate.sources import Period

# Silence the Rust-side WARN chatter icechunk emits during ordinary appends.
icechunk.set_logs_filter("error")


def make_repo(tmp: str, dataset_id: str) -> Any:
    """Create a fresh icechunk repository under the OCS store layout.

    Args:
        tmp: Temporary data directory standing in for an instance data dir.
        dataset_id: Public identifier of the dataset.

    Returns:
        A new icechunk repository.
    """
    path = store_path(tmp, dataset_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    return icechunk.Repository.create(icechunk.local_filesystem_storage(str(path)))


def small_temperature(period: Period) -> xr.Dataset:
    """Fetch one period of temperature on a small grid.

    Args:
        period: The period to fetch.

    Returns:
        The raw source dataset.
    """
    return fetch_temperature(period, ny=12, nx=12)


def flaky_temperature(period: Period) -> xr.Dataset:
    """Fetch temperature, but fail for one month as an unreliable source would.

    Args:
        period: The period to fetch.

    Returns:
        The raw source dataset for every period but the failing one.

    Raises:
        RuntimeError: For period ``2024-03``, standing in for a source outage.
    """
    if period.period_id == "2024-03":
        raise RuntimeError("source unavailable: upstream returned HTTP 503")
    return small_temperature(period)


def describe_store(repo: Any, label: str) -> None:
    """Print what a store currently holds, read from the store itself.

    Args:
        repo: An icechunk repository.
        label: Short prefix identifying the moment being described.
    """
    present = sorted(committed_periods(repo))
    try:
        ds: xr.Dataset = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
        span = f"{str(ds['time'].values[0])[:10]} .. {str(ds['time'].values[-1])[:10]}"
        steps = int(ds.sizes["time"])
    except Exception:
        span, steps = "(empty)", 0
    print(f"  {label}: periods={present} steps={steps} span={span}")


def main() -> None:
    """Interrupt an ingest, resume it from the store, and survive a bad period."""
    periods: list[Period] = enumerate_periods(2024, 5)
    print(f"Target: {len(periods)} monthly periods, {periods[0].period_id} through {periods[-1].period_id}.\n")

    with tempfile.TemporaryDirectory() as tmp:
        repo = make_repo(tmp, "temperature")

        # SECTION: the interrupted run
        print("Run 1 -- interrupted. stop_after=2 stands in for a process killed after")
        print("two months: a timeout, an OOM, a redeploy. The run simply stops.\n")
        first = ingest(repo, periods, small_temperature, stop_after=2)
        print(f"  report.ingested = {first.ingested}")
        print(f"  report.skipped  = {first.skipped}")
        print(f"  report.failed   = {first.failed}")
        describe_store(repo, "store after run 1")

        # SECTION: asking the store what it holds
        print("\nBefore restarting, ask the store what survived. committed_periods() does")
        print("not read a manifest or a job table -- it opens the store and derives the")
        print("period ids from the time coordinate that is actually there:\n")
        present = committed_periods(repo)
        print(f"  committed_periods(repo) = {sorted(present)}")
        missing = [p.period_id for p in periods if p.period_id not in present]
        print(f"  still missing           = {missing}")
        print("  Because the last commit is the last thing that succeeded, this answer is")
        print("  true even if the process died mid-write on the very next period.")

        # SECTION: the resumed run
        print("\nRun 2 -- resume. Same periods, same call. resume=True (the default) makes")
        print("ingest() consult the store first and do only the work that is left:\n")
        second = ingest(repo, periods, small_temperature)
        print(f"  report.ingested = {second.ingested}")
        print(f"  report.skipped  = {second.skipped}")
        print(f"  report.total    = {second.total} periods considered")
        describe_store(repo, "store after run 2")
        print("\n  No duplicated months, no re-download of what was already paid for.")
        print("  Run 2 is also safe to run again: a third run would skip all five.")
        third = ingest(repo, periods, small_temperature)
        print(f"  run 3: ingested={third.ingested} skipped={third.skipped}  <- idempotent")

    # SECTION: a period that fails
    with tempfile.TemporaryDirectory() as tmp:
        repo = make_repo(tmp, "temperature")
        print("\nA different failure: the source itself breaks for one month. Here the")
        print("fetch function raises for 2024-03 while the others work fine.\n")
        report = ingest(repo, periods, flaky_temperature)
        print(f"  report.ingested = {report.ingested}")
        print(f"  report.failed   = {dict(report.failed)}")
        print(f"  report.total    = {report.total}")
        describe_store(repo, "store after the flaky run")
        print("\n  Four months landed; one did not. The run did not abort at the first")
        print("  error, because one bad month is not a reason to discard four good ones.")
        print("  The gap is recorded in report.failed and is also visible in the store:")
        gap = sorted(set(p.period_id for p in periods) - committed_periods(repo))
        print(f"  periods absent from the store = {gap}")
        print("\n  Retrying is the same call. Once the source recovers, ingest() fills the")
        print("  hole and touches nothing else:")
        retry = ingest(repo, periods, small_temperature)
        print(f"  retry: ingested={retry.ingested} skipped={retry.skipped}")
        describe_store(repo, "store after the retry")
        stored: xr.Dataset = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
        print(f"\n  all five periods present: {len(committed_periods(repo)) == len(periods)}")
        print(f"  time axis monotonic     : {stored.indexes['time'].is_monotonic_increasing}")
        print("  Note the span above ends at 2024-03-31: March was appended AFTER May, so")
        print("  the time axis is complete but out of order. Appending only ever adds to")
        print("  the end. Backfilling a gap in the middle is a rewrite, not an append --")
        print("  which is the argument for ingesting periods in order and retrying early.")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- An interrupted ingest leaves a store complete to its last commit, never mid-period")
    print("- committed_periods() derives progress from the store's own time coordinate")
    print("- Re-running ingest() skips what is present and ingests only the rest; report.skipped shows it")
    print("- Repeating a finished run is a no-op, so retries are safe by construction")
    print("- A failing source period lands in report.failed while the other periods still succeed")
    print("- The lesson: the store is the source of truth for resume, not external bookkeeping")


if __name__ == "__main__":
    main()
