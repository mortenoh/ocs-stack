"""Isolation: a reader pinned to a snapshot is unaffected by concurrent writes.

What: opens a readonly session, then commits two new revisions from a writer
while the reader stays open. The reader keeps returning its original values;
only a freshly opened session sees the new state.

Why: open-climate-service serves requests out of the same repositories its
ingest jobs are appending to. A request that started before an ingest finished
must return a coherent dataset, not a mixture. Snapshot isolation gives that
for free: a session resolves the branch once, at creation, and never again.

Run: make run EXAMPLE=0302_isolation
"""

import tempfile
from pathlib import Path
from typing import Any

import xarray as xr

from ocs_stack_icechunk import (
    climate_dataset,
    describe_history,
    open_repo,
    quiet_icechunk_logs,
    read_dataset,
    write_dataset,
)


def summarize(label: str, ds: xr.Dataset) -> None:
    """Print a one-line summary of a dataset's size and values.

    Args:
        label: Prefix identifying which reader produced this view.
        ds: Dataset to summarize.
    """
    last = str(ds.time.values[-1])[:10]
    print(f"  {label}: time={ds.sizes['time']:>2}  last={last}  mean={float(ds.t2m.mean()):6.2f} degC")


def main() -> None:
    """Hold a readonly session open across two commits and compare views."""
    quiet_icechunk_logs()

    with tempfile.TemporaryDirectory() as tmp:
        repo_path = Path(tmp) / "isolation.icechunk"

        # SECTION: the state the reader will be pinned to
        print("A repository with 10 committed days. A long-running request opens it now.")
        repo: Any = open_repo(repo_path)
        v1 = write_dataset(repo, climate_dataset(days=10, ny=16, nx=16, offset=0.0), "ingest 2024-01-01..10")
        print(f"  branch tip is snapshot {v1[:8]}")

        # SECTION: the reader pins a snapshot at session creation
        print("\nThe reader takes a readonly session. That call resolves the branch exactly once:")
        reader: Any = repo.readonly_session("main")
        print(f"  reader.snapshot_id = {str(reader.snapshot_id)[:8]}  read_only={reader.read_only}")
        reader_view = xr.open_zarr(reader.store, consolidated=False)
        summarize("reader view", reader_view)

        # SECTION: a writer commits underneath the reader
        print("\nMeanwhile an ingest job appends 10 more days at +50 degC and commits:")
        v2 = write_dataset(
            repo,
            climate_dataset(days=10, ny=16, nx=16, start="2024-01-11", offset=50.0),
            "ingest 2024-01-11..20",
            append_dim="time",
        )
        print(f"  branch tip moved to snapshot {v2[:8]}")

        # SECTION: the reader is unmoved
        print("\nThe reader is still holding its session. Ask it again -- nothing changed:")
        summarize("reader view", reader_view)
        summarize("fresh read of the same session", xr.open_zarr(reader.store, consolidated=False))
        print(f"  reader.snapshot_id is still {str(reader.snapshot_id)[:8]} -- sessions never follow the branch")

        # SECTION: a second commit, same result
        print("\nA second ingest lands while the reader is still open:")
        v3 = write_dataset(
            repo,
            climate_dataset(days=10, ny=16, nx=16, start="2024-01-21", offset=90.0),
            "ingest 2024-01-21..30",
            append_dim="time",
        )
        print(f"  branch tip moved to snapshot {v3[:8]}")
        summarize("reader view", reader_view)

        # SECTION: a new session sees the new state
        print("\nA new request opens a new session and sees everything committed so far:")
        summarize("fresh reader", read_dataset(repo))
        print("\nAnd any past snapshot is still directly addressable by id:")
        summarize(f"pinned to {v1[:8]}", read_dataset(repo, snapshot_id=v1))
        summarize(f"pinned to {v2[:8]}", read_dataset(repo, snapshot_id=v2))
        summarize(f"pinned to {v3[:8]}", read_dataset(repo, snapshot_id=v3))

        # SECTION: the history behind those views
        print("\nThe three views above are three points on one chain:")
        for entry in describe_history(repo):
            print(f"  {entry['short']}  {entry['message']}")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- readonly_session(branch) resolves the branch to a snapshot id once, at creation")
    print("- every read through that session goes to that snapshot, whatever the branch does later")
    print("- readers therefore need no locks and never block a writer, nor a writer them")
    print("- readonly_session(snapshot_id=...) pins explicitly; same isolation, chosen point")
    print("- to pick up new data a reader must open a new session -- staleness is opt-out, not accidental")


if __name__ == "__main__":
    main()
