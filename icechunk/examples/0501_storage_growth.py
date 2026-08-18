"""What a commit costs: measuring chunk storage as the history grows.

What: measures reachable chunk bytes and on-disk bytes after every commit, for
four kinds of commit -- an append, a metadata-only change, a single-period
correction, and a full rewrite with unchanged values. The numbers show what is
shared between snapshots and what is not.

Why: OCS repositories are append-mostly and long-lived. Knowing that a snapshot
costs only the chunks it changed is what makes commit-per-period affordable;
knowing that icechunk shares by reference and not by content is what stops you
from assuming an identical rewrite is free.

Run: make run EXAMPLE=0501_storage_growth
"""

import tempfile
from pathlib import Path
from typing import Any

import xarray as xr
import zarr

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
PERIOD_BYTES = TIME_CHUNK * NY * NX * 8


def directory_size(path: Path) -> int:
    """Sum the size of every file under a directory.

    Args:
        path: Directory to measure.

    Returns:
        Total size in bytes.
    """
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def chunk_bytes(repo: Any) -> int:
    """Report the reachable native chunk bytes in a repository.

    ``Repository.total_chunks_storage()`` still exists in icechunk 2.1.2 but
    emits a DeprecationWarning; ``chunk_storage_stats().native_bytes`` is the
    replacement and reports the same number.

    Args:
        repo: The repository to measure.

    Returns:
        Bytes of native chunk data reachable from any branch or tag.
    """
    return int(repo.chunk_storage_stats().native_bytes)


def report(repo: Any, path: Path, label: str, previous: int) -> int:
    """Print one measurement row and return the new chunk-byte total.

    Args:
        repo: The repository to measure.
        path: The repository directory on disk.
        label: What the commit just did.
        previous: The chunk-byte total before this commit.

    Returns:
        The chunk-byte total after this commit.
    """
    current = chunk_bytes(repo)
    delta = current - previous
    snapshots = len(describe_history(repo))
    print(
        f"  {label:<34} snapshots={snapshots:>2}  chunk bytes={current:>9,}  "
        f"delta={delta:>+9,}  on disk={directory_size(path):>9,}"
    )
    return current


def month(index: int, offset: float) -> xr.Dataset:
    """Build one 30-day period, sized to exactly one time chunk.

    Args:
        index: Zero-based period number, which sets the start date.
        offset: Constant added to every value.

    Returns:
        A dataset covering 30 days.
    """
    start = f"2024-{index + 1:02d}-01"
    return climate_dataset(days=TIME_CHUNK, ny=NY, nx=NX, start=start, offset=offset)


def main() -> None:
    """Measure chunk storage after appends, a no-op, a correction, and a rewrite."""
    quiet_icechunk_logs()

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "growth.icechunk"
        repo: Any = open_repo(path)

        # SECTION: the baseline unit of cost
        print(f"Each period is {TIME_CHUNK} days x {NY} x {NX} float64 = {PERIOD_BYTES:,} bytes, exactly one chunk.")
        print("Periods are chunk-aligned on purpose, so every number below is a whole number of periods.")
        print("Chunks are compressed on the way out, so a stored period is a bit smaller than that.\n")

        # SECTION: appends cost one period each
        print("Six appends, one commit each:")
        total = 0
        for index in range(6):
            session: Any = repo.writable_session("main")
            data = month(index, float(index))
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
            session.commit(f"ingest period {index + 1}")
            total = report(repo, path, f"append period {index + 1}", total)

        stored = read_dataset(repo)
        logical = int(stored.t2m.size) * 8
        after_appends = total
        print(f"\n  after 6 snapshots the store holds {total:,} chunk bytes")
        print(f"  the tip is {logical:,} uncompressed bytes, so the whole history costs {total / logical:.2f}x it")
        print(f"  six full copies would have cost about {6 * total:,} bytes -- {6 * logical:,} uncompressed")
        print("  each snapshot points at the chunks the previous one wrote and adds only what it changed;")
        print("  sharing is the default, not something you opt into")

        # SECTION: a commit that changes no chunks
        print("\nA commit that only changes metadata:")
        session = repo.writable_session("main")
        group = zarr.open_group(session.store, mode="a")
        group.attrs["institution"] = "open-climate-service"
        session.commit("set institution attribute")
        total = report(repo, path, "metadata-only commit", total)
        print("  zero chunk bytes: no array data changed, so no chunk was written")

        # SECTION: a correction costs the chunks it touches
        print("\nA correction that rewrites one period's values:")
        session = repo.writable_session("main")
        month(2, 500.0).drop_vars("time").to_zarr(session.store, region={"time": slice(60, 90)}, consolidated=False)
        session.commit("correct period 3")
        total = report(repo, path, "correct 1 of 6 periods", total)
        print(f"  the delta is one period's worth, the same size as each append above ({after_appends // 6:,})")
        print("  the other five periods were not touched, so their chunks are simply referenced again")
        print("  the superseded chunk is still reachable from the pre-correction snapshots, so it stays")

        # SECTION: rewriting identical data is not free
        print("\nRewriting the whole dataset with byte-identical values:")
        session = repo.writable_session("main")
        rewrite = xr.concat([month(i, 500.0 if i == 2 else float(i)) for i in range(6)], dim="time")
        rewrite.to_zarr(
            session.store,
            mode="w",
            zarr_format=3,
            consolidated=False,
            encoding={"t2m": {"chunks": (TIME_CHUNK, NY, NX)}},
        )
        session.commit("rewrite with identical values")
        total = report(repo, path, "full rewrite, same values", total)
        identical = bool(read_dataset(repo).t2m.equals(rewrite.t2m))
        print(f"  the tip's values are unchanged ({identical}) but the store grew by a full dataset")
        print("  icechunk shares chunks by reference, not by content hash: a snapshot reuses a chunk")
        print("  only when the write never touched it. Writing the same bytes writes new objects.")
        print("  so 'is this a no-op?' is a question about which chunks your code wrote, not about values")

        # SECTION: where the bytes actually went
        print("\nThe full breakdown from chunk_storage_stats():")
        stats: Any = repo.chunk_storage_stats()
        print(f"  native_bytes={stats.native_bytes:,}  virtual_bytes={stats.virtual_bytes:,}")
        print(f"  inlined_bytes={stats.inlined_bytes:,}  (tiny arrays like the time coordinate)")
        print(f"  on-disk total={directory_size(path):,} -- chunks plus snapshots, manifests, and refs")
        print(f"  final chunk-byte total tracked across this run: {total:,}")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- a commit costs the chunks it wrote; unchanged chunks are referenced, never copied")
    print("- so an append-only history of N periods costs N periods, not N snapshots x N periods")
    print("- a metadata-only commit costs zero chunk bytes")
    print("- a correction costs the chunks it overlaps, and the superseded chunks stay reachable")
    print("- sharing is by reference: rewriting identical values still writes new chunk objects")
    print("- chunk_storage_stats().native_bytes is the current API; total_chunks_storage() is deprecated")
    print("- reachable bytes ignore chunks only old snapshots need; expiry and gc reclaim those (0502)")


if __name__ == "__main__":
    main()
