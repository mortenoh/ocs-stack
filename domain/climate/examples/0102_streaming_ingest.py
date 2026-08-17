"""Streaming ingest: one period in, one commit out.

What: enumerates monthly periods, ingests them into an icechunk store one at a
time with ingest_period(), prints the time axis growing after each, and reads
back the commit history the run produced.

Why: this is the open-climate-service ingestion contract. A source plugin says
which periods it can supply; the framework fetches, normalizes, appends, and
commits each one on its own. The commit boundary is the whole point -- it is
what makes an interrupted ingest recoverable instead of corrupt.

Run: make run EXAMPLE=0102_streaming_ingest
"""

import tempfile
from typing import Any

import icechunk  # no type stubs; repository handles stay narrowly typed as Any
import xarray as xr

from playground_domain_climate import enumerate_periods, fetch_temperature, ingest_period, store_path
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


def read_store(repo: Any) -> xr.Dataset:
    """Open the committed state of a repository's main branch.

    Args:
        repo: An icechunk repository.

    Returns:
        The dataset as of the latest commit on main.
    """
    ds: xr.Dataset = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    return ds


def main() -> None:
    """Ingest monthly periods one commit at a time and inspect the history."""
    with tempfile.TemporaryDirectory() as tmp:
        repo = make_repo(tmp, "temperature")

        # SECTION: the source enumerates what it can supply
        print("Step 1 -- the source lists its periods. It does not hand over a year of")
        print("data in one call; it says what exists and lets the framework pull it.\n")
        periods: list[Period] = enumerate_periods(2024, 5)
        for period in periods:
            print(f"  {period.period_id}  start={period.start}  days={period.days}")
        print(f"\n  {len(periods)} periods, {sum(p.days for p in periods)} daily steps in total.")
        print("  Note the day counts: 31, 29, 31, 30, 31. Months are not a fixed size.")

        # SECTION: ingest, one period per commit
        print("\nStep 2 -- ingest one period at a time. Each call fetches, normalizes,")
        print("appends along time, and commits. Watch the stored time axis grow:\n")
        print(f"  {'period':>8}  {'time steps':>10}  {'last day':>12}  snapshot")
        for period in periods:
            snapshot = ingest_period(repo, period, lambda p: fetch_temperature(p, ny=12, nx=12))
            stored = read_store(repo)
            last_day = str(stored["time"].values[-1])[:10]
            print(f"  {period.period_id:>8}  {stored.sizes['time']:>10}  {last_day:>12}  {snapshot[:12]}...")

        stored = read_store(repo)
        print(f"\n  final store: dims={dict(stored.sizes)}, units={stored['t2m'].attrs['units']!r}")
        print(f"  time axis monotonic: {stored.indexes['time'].is_monotonic_increasing}")
        print(f"  time axis unique   : {stored.indexes['time'].is_unique}")

        # SECTION: the commit history
        print("\nStep 3 -- the history. One snapshot per period, plus the repository's")
        print("initial empty commit. This is the audit trail a data product needs:\n")
        history = list(repo.ancestry(branch="main"))
        for info in history:
            print(f"  {str(info.id)[:12]}...  {info.written_at:%Y-%m-%d %H:%M:%S}  {info.message}")
        print(f"\n  {len(history)} snapshots for {len(periods)} periods (the extra one is repo creation).")

        # SECTION: why the commit boundary matters
        print("\nWhy per-period commits and not one commit at the end:")
        print("  A month of daily data is many chunk writes. If the process dies partway")
        print("  through one, an ordinary zarr store on disk is left holding some new")
        print("  chunks and a time axis that may or may not have been extended -- torn.")
        print("  icechunk only publishes a snapshot when commit() succeeds, so a crash")
        print("  leaves the store exactly as of the last completed period. Never half a")
        print("  month. Resume then becomes a question you ask the store, not a log file.")
        print("  The cost is one snapshot per period instead of one per run, which is")
        print("  cheap and doubles as provenance: you can point at when each month landed.")

        # SECTION: chunk alignment
        print("\nOne implementation detail worth knowing -- ingest() passes align_chunks=True:")
        print("  The store's time chunk is fixed at 30 days. Months are 28-31. So after")
        print("  the first append the final chunk on disk is partial, and the next month's")
        print("  data straddles it. xarray refuses that write outright ('would overlap")
        print("  multiple Dask chunks') because two writers touching one chunk can corrupt")
        print("  it. align_chunks=True rechunks the incoming period onto the store's")
        print("  existing boundaries first, which is why 29-day February appends cleanly.")
        chunks = stored["t2m"].encoding.get("chunks")
        print(f"  stored chunk shape: {chunks}, time length {stored.sizes['time']}")

        # SECTION: summary
        print("\n=== Summary ===")
        print("- enumerate_periods() lists what the source can supply; the framework pulls one at a time")
        print("- ingest_period() = fetch -> normalize -> append along time -> commit")
        print("- The time axis grows a month per commit and stays sorted and unique")
        print("- repo.ancestry() shows one snapshot per period: an audit trail for free")
        print("- A crash leaves the store complete to the last commit, never mid-period")
        print("- align_chunks=True reconciles 28-31 day months against the fixed 30-day time chunk")


if __name__ == "__main__":
    main()
