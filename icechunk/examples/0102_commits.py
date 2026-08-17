"""Commits are atomic: readers see nothing at all until commit() lands.

What: writes a dataset through a session while a *separate* readonly session
watches. The reader sees no dataset, then no *new* dataset, until the writer
commits -- at which point the whole change appears at once.

Why: in open-climate-service an ingest appends to
``{data_dir}/downloads/{dataset_id}.icechunk`` while the API is serving reads
from that same store. With plain zarr that is a race: a reader can land on a
store with half its chunks rewritten. With icechunk the write is a transaction,
so this example is the single property the whole storage layer rests on.

Run: make run EXAMPLE=0102_commits
"""

import tempfile
from pathlib import Path
from typing import Any

import xarray as xr
import zarr.errors

from climate_stack_icechunk import climate_dataset, open_repo, quiet_icechunk_logs


def peek(repo: Any, label: str) -> None:
    """Open a fresh readonly session on main and report what a reader would see.

    Args:
        repo: Repository to read from.
        label: Description of the moment the read is taken at.
    """
    session: Any = repo.readonly_session("main")
    try:
        ds = xr.open_zarr(session.store, consolidated=False)
    except zarr.errors.GroupNotFoundError:
        print(f"  {label}: nothing -- xarray raises GroupNotFoundError, the store looks empty")
        return
    print(f"  {label}: time={ds.sizes['time']}, t2m mean={float(ds.t2m.mean()):.3f}")


def main() -> None:
    """Show that uncommitted writes are invisible and commits are all-or-nothing."""
    quiet_icechunk_logs()

    with tempfile.TemporaryDirectory() as tmpdir:
        repo_path = Path(tmpdir) / "era5_t2m.icechunk"
        repo: Any = open_repo(repo_path)

        # SECTION: the first write, watched from outside
        print("A fresh repository has a snapshot but no arrays. Open a writable session,")
        print("write 30 days of data into it, and check what an independent reader sees.")
        peek(repo, "reader before the write ")

        writer: Any = repo.writable_session("main")
        climate_dataset(days=30, ny=32, nx=32, offset=0.0).to_zarr(
            writer.store, mode="w", zarr_format=3, consolidated=False
        )
        print(f"  writer has written data: has_uncommitted_changes={writer.has_uncommitted_changes}")
        peek(repo, "reader after the write  ")
        print("  The bytes are on disk. The reader still cannot see them, because no")
        print("  snapshot points at them yet.")

        first_id = writer.commit("ingest 2024-01, offset 0.0")
        print(f"\nNow commit -> snapshot {first_id}")
        peek(repo, "reader after the commit ")
        print("  The whole 30-day dataset appeared in one step. There was no moment where")
        print("  a reader could have seen 10 days, or one array but not the other.")

        # SECTION: a reader that opened first is unaffected
        print("\nSecond round, the harder case: a reader opens the store, THEN a writer")
        print("replaces every value with offset=+10. The reader keeps reading.")
        held: Any = repo.readonly_session("main")
        held_ds = xr.open_zarr(held.store, consolidated=False)
        print(f"  reader opened at snapshot {held.snapshot_id}, mean={float(held_ds.t2m.mean()):.3f}")

        writer2: Any = repo.writable_session("main")
        climate_dataset(days=30, ny=32, nx=32, offset=10.0).to_zarr(
            writer2.store, mode="w", zarr_format=3, consolidated=False
        )
        print("  writer2 has rewritten all chunks, not yet committed.")
        print(f"  held reader still reads mean={float(held_ds.t2m.mean()):.3f}  (unchanged)")
        peek(repo, "a brand new reader too   ")

        second_id = writer2.commit("reprocess 2024-01, offset 10.0")
        print(f"\nCommit -> snapshot {second_id}")
        print(f"  held reader STILL reads mean={float(held_ds.t2m.mean()):.3f}")
        print("  It is pinned to the snapshot it opened at; a commit cannot move it.")
        peek(repo, "a new reader now sees    ")

        # SECTION: abandoning a write
        print("\nAnd the failure path: write into a session, then never commit it.")
        doomed: Any = repo.writable_session("main")
        climate_dataset(days=30, ny=32, nx=32, offset=-99.0).to_zarr(
            doomed.store, mode="w", zarr_format=3, consolidated=False
        )
        print(f"  doomed session has uncommitted changes: {doomed.has_uncommitted_changes}")
        doomed.discard_changes()
        print(f"  after discard_changes():                {doomed.has_uncommitted_changes}")
        peek(repo, "reader is untouched      ")
        print("  A crash instead of discard_changes() has the same effect: the branch tip")
        print("  never moved, so the orphaned chunks are simply unreachable.")

        # SECTION: the history that resulted
        print("\nTwo commits landed, and both are permanent points in the history:")
        for snapshot in repo.ancestry(branch="main"):
            print(f"  {str(snapshot.id)[:8]}  {snapshot.message}")

        print("\nBoth are still readable, which is what the next examples build on:")
        for label, snap_id in (("first ", first_id), ("second", second_id)):
            session: Any = repo.readonly_session(snapshot_id=snap_id)
            ds = xr.open_zarr(session.store, consolidated=False)
            print(f"  {label} {str(snap_id)[:8]}: mean={float(ds.t2m.mean()):.3f}")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- writes into a session are invisible to every other reader until commit()")
    print("- commit() is atomic: the entire change appears in one step, or not at all")
    print("- a reader is pinned to the snapshot it opened; later commits cannot move it")
    print("- an abandoned session leaves the branch tip untouched, so nothing is corrupted")
    print("- this is why OCS can append to a dataset that is simultaneously being served")


if __name__ == "__main__":
    main()
