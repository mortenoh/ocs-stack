"""History: walking ancestry, and why the chain only ever grows.

What: makes several commits against one branch, walks ``repo.ancestry`` to
print each snapshot's short id, message, and timestamp, points out the
"Repository initialized" snapshot at the root, and shows that adding commits
never changes the ids or contents of the ones already there.

Why: an open-climate-service store accumulates one commit per ingested period.
The ancestry chain is the audit trail: which periods arrived, when, and in what
order. Because it is append-only, "what did the API serve last Tuesday" has an
exact answer rather than an estimate.

Run: make run EXAMPLE=0103_history
"""

import tempfile
from pathlib import Path
from typing import Any

import xarray as xr

from playground_data_icechunk import (
    climate_dataset,
    describe_history,
    open_repo,
    quiet_icechunk_logs,
    write_dataset,
)


def print_history(repo: Any, label: str) -> list[dict[str, Any]]:
    """Print a branch's ancestry newest-first and return the rows.

    Args:
        repo: Repository to inspect.
        label: Heading describing the moment being shown.

    Returns:
        The history rows from ``describe_history``, newest first.
    """
    history = describe_history(repo)
    plural = "snapshot" if len(history) == 1 else "snapshots"
    print(f"  {label} ({len(history)} {plural}, newest first)")
    for row in history:
        stamp = row["written_at"].strftime("%Y-%m-%d %H:%M:%S")
        print(f"    {row['short']}  {stamp}  {row['message']}")
    return history


def main() -> None:
    """Build a multi-commit history and show that it is append-only."""
    quiet_icechunk_logs()

    with tempfile.TemporaryDirectory() as tmpdir:
        repo_path = Path(tmpdir) / "era5_t2m.icechunk"
        repo: Any = open_repo(repo_path)

        # SECTION: the root snapshot
        print("Before anything is written, the history is not empty. Creating a repository")
        print("makes a root snapshot so that the first real commit has a parent.")
        print_history(repo, "history of a fresh repository:")

        # SECTION: one commit per period, the OCS shape
        print("\nNow ingest three months, one commit each -- the shape an OCS ingest has.")
        print("The first write creates the arrays; the rest append along time.")
        ids: list[str] = []
        periods = (
            ("2024-01-01", 31, "ingest era5 t2m 2024-01"),
            ("2024-02-01", 29, "ingest era5 t2m 2024-02"),
            ("2024-03-01", 31, "ingest era5 t2m 2024-03"),
        )
        for index, (start, days, message) in enumerate(periods):
            ds = climate_dataset(days=days, ny=32, nx=32, start=start, seed=index)
            append_dim = None if index == 0 else "time"
            snapshot_id = write_dataset(repo, ds, message, append_dim=append_dim)
            ids.append(snapshot_id)
            print(f"  {message} -> {snapshot_id[:8]}  (append_dim={append_dim!r})")

        # SECTION: walking ancestry
        print("\nrepo.ancestry(branch='main') walks the parent chain from the tip backwards.")
        print("Each snapshot carries an id, the message you gave commit(), and a timestamp.")
        history = print_history(repo, "history of main:")

        print("\n  The last row is the root: 'Repository initialized'. Its id is a fixed")
        print("  sentinel, the same in every icechunk repository ever created:")
        print(f"    {history[-1]['id']}")
        print("  Everything else descends from it, so a full walk always terminates there.")

        print("\n  Timestamps are UTC and strictly increase down the chain (newest first):")
        stamps = [row["written_at"] for row in history]
        print(f"    newest {stamps[0].isoformat()}")
        print(f"    oldest {stamps[-1].isoformat()}")
        print(f"    monotonic: {all(a > b for a, b in zip(stamps, stamps[1:], strict=False))}")

        # SECTION: the parent chain
        print("\nThe chain is explicit -- every snapshot names its parent:")
        for snapshot in repo.ancestry(branch="main"):
            parent = str(snapshot.parent_id)[:8] if snapshot.parent_id else "(none)"
            print(f"    {str(snapshot.id)[:8]}  parent={parent}")

        # SECTION: append-only
        print("\nAppend-only means a new commit adds a row and touches nothing above it.")
        print("Capture the ids, commit a fourth period, and compare.")
        before = [row["id"] for row in describe_history(repo)]
        fourth = write_dataset(
            repo,
            climate_dataset(days=30, ny=32, nx=32, start="2024-04-01", seed=3),
            "ingest era5 t2m 2024-04",
            append_dim="time",
        )
        after = [row["id"] for row in describe_history(repo)]
        print(f"  before: {len(before)} snapshots, after: {len(after)} snapshots")
        print(f"  the new tip is {fourth[:8]}, prepended to the walk")
        print(f"  every previously seen id is still present, in order: {after[1:] == before}")
        print("  Nothing was rewritten. There is no icechunk operation that edits a")
        print("  committed snapshot -- a correction is another commit on top.")

        print()
        print_history(repo, "final history of main:")

        # SECTION: what the data looks like at the tip
        print("\n  And the tip holds all four periods stacked along time:")
        session: Any = repo.readonly_session("main")
        ds = xr.open_zarr(session.store, consolidated=False)
        print(f"    time={ds.sizes['time']} steps, {str(ds.time.values[0])[:10]} .. {str(ds.time.values[-1])[:10]}")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- repo.ancestry(branch=...) walks parent links from the tip to the root")
    print("- each snapshot has .id, .message, .written_at, .parent_id")
    print("- the root 'Repository initialized' snapshot has a fixed sentinel id")
    print("- committing prepends a row; existing ids and their contents never change")
    print("- so the chain is a genuine audit trail of an ingest, not a mutable log")


if __name__ == "__main__":
    main()
