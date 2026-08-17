"""Atomicity: a write that fails leaves no trace in the repository.

What: starts an icechunk write, raises partway through before the commit, and
shows the branch tip, the history, and a reader are all exactly as they were.
Then does the same crash against a plain zarr directory, which ends up holding
half old data and half new.

Why: open-climate-service appends to datasets that are simultaneously being
served over HTTP. Atomicity is the property that makes that safe -- an ingest
that dies mid-flight cannot leave a client reading a store where January is the
new revision and February is still the old one.

Run: make run EXAMPLE=0301_atomicity
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
    read_dataset,
    write_dataset,
)


def directory_size(path: Path) -> int:
    """Sum the size of every file under a directory.

    Args:
        path: Directory to measure.

    Returns:
        Total size in bytes.
    """
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def period_means(ds: xr.Dataset) -> list[float]:
    """Reduce each time step of a dataset to its spatial mean.

    Args:
        ds: Dataset with a ``t2m`` variable and a ``time`` dimension.

    Returns:
        One rounded mean per time step.
    """
    return [round(float(ds.t2m.isel(time=i).mean()), 1) for i in range(ds.sizes["time"])]


def crashing_icechunk_write(repo: Any, path: Path) -> None:
    """Append a revision to a repository and fail before committing.

    Args:
        repo: The repository to write into.
        path: The repository directory on disk, for size reporting.
    """
    before_bytes = directory_size(path)
    session = repo.writable_session("main")
    try:
        revision = climate_dataset(days=10, ny=16, nx=16, start="2024-01-11", offset=100.0)
        revision.to_zarr(session.store, append_dim="time", consolidated=False)
        print(f"  wrote 10 more steps into the session: has_uncommitted_changes={session.has_uncommitted_changes}")
        raise RuntimeError("upstream API returned 503 halfway through the ingest")
    except RuntimeError as exc:
        print(f"  ingest raised: {exc}")
    print("  the session is abandoned without commit() -- no snapshot was created")
    print(f"  on-disk bytes before={before_bytes}, after={directory_size(path)} (chunk objects exist but are orphaned)")


def crashing_plain_zarr_write(path: Path) -> None:
    """Overwrite a plain zarr store step by step and crash halfway.

    Args:
        path: Directory holding the zarr store.
    """
    revision = climate_dataset(days=10, ny=16, nx=16, offset=100.0)
    try:
        for i in range(10):
            if i == 5:
                raise RuntimeError("upstream API returned 503 halfway through the ingest")
            step = revision.isel(time=slice(i, i + 1)).drop_vars("time")
            step.to_zarr(path, region={"time": slice(i, i + 1)}, consolidated=False)
    except RuntimeError as exc:
        print(f"  ingest raised: {exc}")


def main() -> None:
    """Contrast a failed icechunk write with a failed plain-zarr write."""
    quiet_icechunk_logs()

    with tempfile.TemporaryDirectory() as tmp:
        repo_path = Path(tmp) / "atomicity.icechunk"
        zarr_path = Path(tmp) / "atomicity.zarr"

        # SECTION: a committed baseline
        print("A repository with one committed snapshot: 10 days of t2m around 26 degC.")
        repo: Any = open_repo(repo_path)
        baseline = climate_dataset(days=10, ny=16, nx=16, offset=0.0)
        baseline_id = write_dataset(repo, baseline, "ingest 2024-01-01..10")
        before = read_dataset(repo)
        print(f"  snapshot {baseline_id[:8]}: time={before.sizes['time']}, step means={period_means(before)}")

        # SECTION: a write that dies before commit
        print("\nNow an ingest appends 10 more steps at +100 degC and dies before commit():")
        crashing_icechunk_write(repo, repo_path)

        # SECTION: what a reader sees afterwards
        print("\nWhat a reader sees after the failure -- open the branch tip again:")
        after = read_dataset(repo)
        print(f"  time={after.sizes['time']}, step means={period_means(after)}")
        print(f"  identical to before: {period_means(before) == period_means(after)}")
        print(f"  no partial data: max value is {float(after.t2m.max()):.1f} degC, not 126")

        history = describe_history(repo)
        print(f"  history is still {len(history)} snapshots deep, tip is still {baseline_id[:8]}:")
        for entry in history:
            print(f"    {entry['short']}  {entry['message']}")

        # SECTION: the same crash against a plain zarr directory
        print("\nThe same crash against a plain zarr directory (one chunk per time step):")
        baseline.to_zarr(
            zarr_path, mode="w", zarr_format=3, consolidated=False, encoding={"t2m": {"chunks": (1, 16, 16)}}
        )
        print(f"  before: step means={period_means(xr.open_zarr(zarr_path, consolidated=False))}")
        crashing_plain_zarr_write(zarr_path)
        corrupted = xr.open_zarr(zarr_path, consolidated=False)
        print(f"  after:  step means={period_means(corrupted)}")
        print("  steps 0-4 are the new revision, steps 5-9 are the old one -- a store no reader should see")

        # SECTION: why this matters for a served dataset
        print("\nWhy this is the property that matters for a served dataset:")
        print("  a zarr chunk write is one PUT per chunk; there is no boundary around the set")
        print("  icechunk buffers chunk writes, then makes exactly one atomic reference update at commit()")
        print("  until that update lands, every reader resolves the branch to the previous snapshot")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- a write is only visible once session.commit() returns; a raise before it changes nothing")
    print("- the failed session's chunk objects do reach storage, but no snapshot references them")
    print("- those orphans are invisible to readers and reclaimable by garbage_collect (see 0502)")
    print("- a plain zarr directory has no transaction boundary: a crash mixes old and new chunks")
    print("- this is what makes appending to a dataset that is being served concurrently safe")


if __name__ == "__main__":
    main()
