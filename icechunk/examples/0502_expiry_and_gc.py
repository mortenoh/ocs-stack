"""Expiry and garbage collection: trading time travel for storage.

What: builds a history of four ingests and three corrections, then expires the
older snapshots and garbage-collects what nothing references any more. Prints
the history, the reachable chunk bytes, and the on-disk size at each stage, and
tries to read an expired snapshot before and after the collection.

Why: an OCS repository that reingests periods accumulates superseded chunks
forever. Expiry plus garbage collection is the retention policy, and it is the
one operation in this whole project that destroys data -- so it is worth
understanding exactly what it removes before running it.

Run: make run EXAMPLE=0502_expiry_and_gc
"""

import datetime as dt
import tempfile
from pathlib import Path
from typing import Any

from ocs_stack_icechunk import (
    climate_dataset,
    describe_history,
    open_repo,
    quiet_icechunk_logs,
    read_dataset,
)

TIME_CHUNK = 30
NY = 48
NX = 48
PERIODS = 4
CORRECTIONS = 3


def directory_size(path: Path) -> int:
    """Sum the size of every file under a directory.

    Args:
        path: Directory to measure.

    Returns:
        Total size in bytes.
    """
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def show_state(repo: Any, path: Path, label: str) -> None:
    """Print the history depth, reachable chunk bytes, and on-disk size.

    Args:
        repo: The repository to measure.
        path: The repository directory on disk.
        label: Stage name for the printed row.
    """
    stats: Any = repo.chunk_storage_stats()
    history = describe_history(repo)
    print(
        f"  {label:<22} snapshots={len(history):>2}  reachable chunk bytes={int(stats.native_bytes):>9,}  "
        f"on disk={directory_size(path):>9,}"
    )


def print_history(repo: Any) -> None:
    """Print the branch history, newest first.

    Args:
        repo: The repository to inspect.
    """
    for entry in describe_history(repo):
        print(f"    {entry['short']}  {entry['written_at']:%H:%M:%S.%f}  {entry['message']}")


def try_read(repo: Any, snapshot_id: str, label: str) -> None:
    """Try to open a snapshot by id and report whether it is still readable.

    Args:
        repo: The repository to read from.
        snapshot_id: The snapshot to open.
        label: Description of the snapshot for the printed line.
    """
    try:
        ds = read_dataset(repo, snapshot_id=snapshot_id)
        mean = float(ds.t2m.isel(time=slice(0, TIME_CHUNK)).mean())
        print(f"    {snapshot_id[:8]}  {label:<24} readable, period 1 mean = {mean:.1f} degC")
    except Exception as exc:
        print(f"    {snapshot_id[:8]}  {label:<24} unreadable: {type(exc).__name__}")


def build_history(repo: Any) -> dict[str, str]:
    """Ingest four periods, then correct the first one three times.

    Args:
        repo: The repository to write into.

    Returns:
        A mapping from label to snapshot id for the interesting commits.
    """
    snapshots: dict[str, str] = {}
    for index in range(PERIODS):
        session: Any = repo.writable_session("main")
        data = climate_dataset(days=TIME_CHUNK, ny=NY, nx=NX, start=f"2024-{index + 1:02d}-01", offset=float(index))
        if index == 0:
            data.to_zarr(
                session.store,
                mode="w",
                zarr_format=3,
                consolidated=False,
                encoding={"t2m": {"chunks": (TIME_CHUNK, NY, NX)}},
            )
        else:
            data.to_zarr(session.store, append_dim="time", consolidated=False)
        snapshots[f"ingest {index + 1}"] = str(session.commit(f"ingest period {index + 1}"))

    for revision in range(CORRECTIONS):
        session = repo.writable_session("main")
        fixed = climate_dataset(days=TIME_CHUNK, ny=NY, nx=NX, start="2024-01-01", offset=100.0 + revision)
        fixed.drop_vars("time").to_zarr(session.store, region={"time": slice(0, TIME_CHUNK)}, consolidated=False)
        snapshots[f"correction {revision + 1}"] = str(session.commit(f"correct period 1, revision {revision + 1}"))
    return snapshots


def main() -> None:
    """Expire old snapshots, garbage-collect, and measure what each step reclaims."""
    quiet_icechunk_logs()

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "retention.icechunk"
        repo: Any = open_repo(path)

        # SECTION: a history with superseded data in it
        print(f"Ingest {PERIODS} periods, then correct period 1 {CORRECTIONS} times.")
        print("Each correction rewrites period 1's chunk, so the previous version is superseded")
        print("but stays reachable from the snapshot that wrote it.\n")
        snapshots = build_history(repo)
        show_state(repo, path, "after ingest + fixes")
        print("\n  history, newest first:")
        print_history(repo)

        # SECTION: everything is still readable
        print("\n  every past revision of period 1 is still readable by snapshot id:")
        for label in ("ingest 4", "correction 1", "correction 2", "correction 3"):
            try_read(repo, snapshots[label], label)

        # SECTION: expiry drops snapshots from history
        history = describe_history(repo)
        cutoff = history[1]["written_at"]
        print(f"\nExpire everything written before {cutoff:%H:%M:%S.%f} -- that is, all but the last two.")
        print("  expire_snapshots walks the refs and removes those snapshots from the ancestry chain")
        print("  the root snapshot and the main branch tip are never expired")
        expired: set[str] = {str(s) for s in repo.expire_snapshots(older_than=cutoff)}
        print(f"  expired {len(expired)} snapshots")
        show_state(repo, path, "after expire")
        print("\n  history is now:")
        print_history(repo)
        print("  reachable chunk bytes dropped immediately -- those chunks no longer count as needed")
        print("  on-disk size did not: expiry rewrites the ancestry, it does not delete objects")

        # SECTION: garbage collection deletes the objects
        print("\nGarbage-collect objects that nothing reachable references. Dry run first:")
        cutoff_objects = dt.datetime.now(dt.UTC)
        dry: Any = repo.garbage_collect(cutoff_objects, dry_run=True)
        print(f"  would delete: {dry.chunks_deleted} chunks, {dry.manifests_deleted} manifests,")
        print(f"                {dry.snapshots_deleted} snapshots, {dry.bytes_deleted:,} bytes in total")
        show_state(repo, path, "after dry run")
        print("  a dry run changes nothing, which the unchanged on-disk size confirms")

        print("\nNow for real:")
        summary: Any = repo.garbage_collect(cutoff_objects)
        print(f"  deleted: {summary.chunks_deleted} chunks, {summary.manifests_deleted} manifests,")
        print(f"           {summary.snapshots_deleted} snapshots, {summary.bytes_deleted:,} bytes in total")
        show_state(repo, path, "after gc")

        # SECTION: what was lost and what survived
        print("\nThe tip is untouched -- the data being served is exactly what it was:")
        tip = read_dataset(repo)
        first = float(tip.t2m.isel(time=slice(0, TIME_CHUNK)).mean())
        print(f"  dims={dict(tip.sizes)}  period 1 mean = {first:.1f} degC (the last correction)")
        print("\nThe expired snapshots are gone, and gone means gone:")
        for label in ("ingest 4", "correction 1", "correction 2", "correction 3"):
            try_read(repo, snapshots[label], label)

        # SECTION: the trade-off
        print("\nThe trade-off:")
        print("  keeping history means keeping every superseded chunk any snapshot still points at")
        print("  a nightly reingest of the same period costs one full copy of that period per night")
        print("  expiry buys that space back, and pays for it with the ability to answer")
        print("  'what did we serve on the 3rd?' -- which for a health service can be a real question")
        print("  expiry is irreversible: there is no un-expire, and gc then removes the objects")
        print("  the usual policy is a window (keep 90 days of snapshots) plus tags on the")
        print("  snapshots that must outlive it, since a tagged snapshot keeps its chunks alive")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- expire_snapshots(older_than=...) removes snapshots from the ancestry; the tip is safe")
    print("- it takes effect immediately in reachable chunk bytes, and not at all on disk")
    print("- garbage_collect(delete_object_older_than=...) deletes what nothing reachable points at")
    print("- run it with dry_run=True first; the GCSummary tells you what would go")
    print("- in an append-only history gc reclaims almost nothing: every chunk is still in the tip")
    print("- the savings come from superseded chunks, which corrections and reingests create")
    print("- both operations are irreversible; tag any snapshot that must survive a retention sweep")


if __name__ == "__main__":
    main()
