"""Diffing: asking the store what actually changed between two snapshots.

What: builds a history with four different kinds of change -- a first ingest, an
append, a one-day correction, and a metadata edit -- then runs ``repo.diff``
across each pair and reads what it reports: arrays created, definitions
updated, and the exact chunk coordinates rewritten.

Why: two questions come up constantly against an open-climate-service store.
"What did last night's ingest actually add?" and "the fix for that bad day --
did it touch only that day?" A diff answers both from the store itself rather
than from a log the writer had to remember to emit, and chunk coordinates are
precise enough to falsify a claim that a fix was surgical.

Run: make run EXAMPLE=0203_diffing
"""

import tempfile
from pathlib import Path
from typing import Any

import icechunk
import xarray as xr
import zarr

from playground_data_icechunk import (
    climate_dataset,
    open_repo,
    quiet_icechunk_logs,
    read_dataset,
    write_dataset,
)


def show_diff(repo: Any, from_id: str, to_id: str, label: str) -> None:
    """Print a structured summary of the diff between two snapshots.

    Args:
        repo: Repository holding both snapshots.
        from_id: The older snapshot.
        to_id: The newer snapshot.
        label: Description of the change being diffed.
    """
    diff: Any = repo.diff(from_snapshot_id=from_id, to_snapshot_id=to_id)
    chunk_total = sum(len(coords) for coords in diff.updated_chunks.values())
    print(f"  {label}")
    print(f"    {from_id[:8]} -> {to_id[:8]}")
    print(f"    is_empty()        {diff.is_empty()}")
    print(f"    new_groups        {sorted(diff.new_groups)}")
    print(f"    new_arrays        {sorted(diff.new_arrays)}")
    print(f"    deleted_groups    {sorted(diff.deleted_groups)}")
    print(f"    deleted_arrays    {sorted(diff.deleted_arrays)}")
    print(f"    updated_groups    {sorted(diff.updated_groups)}   (metadata/attrs)")
    print(f"    updated_arrays    {sorted(diff.updated_arrays)}   (shape/dtype/attrs)")
    print(f"    updated_chunks    {chunk_total} chunk(s) across {len(diff.updated_chunks)} array(s)")
    for array, coords in sorted(diff.updated_chunks.items()):
        rendered = ", ".join(str(c) for c in coords[:6])
        suffix = ", ..." if len(coords) > 6 else ""
        print(f"      {array}: {rendered}{suffix}")


def main() -> None:
    """Diff four kinds of change and read what the store reports for each."""
    quiet_icechunk_logs()

    with tempfile.TemporaryDirectory() as tmpdir:
        repo_path = Path(tmpdir) / "era5_t2m.icechunk"
        repo: Any = open_repo(repo_path)
        root = str(next(s.id for s in repo.ancestry(branch="main")))

        # SECTION: the first ingest
        print("repo.diff(from_snapshot_id=..., to_snapshot_id=...) reports the change between")
        print("any two snapshots. Start with the first ingest, diffed against the empty root.")
        first = write_dataset(repo, climate_dataset(days=30, ny=64, nx=64, start="2024-01-01"), "ingest 2024-01")
        show_diff(repo, root, first, "first ingest: 30 days on a 64x64 grid")
        print("    Arrays appear where there were none, and every chunk of both arrays is")
        print("    listed as updated -- a from-nothing ingest touches everything.")

        # SECTION: an append
        print("\nNow append February. This is the nightly-ingest case, and the diff is the")
        print("answer to 'what did last night add?'")
        second = write_dataset(
            repo,
            climate_dataset(days=29, ny=64, nx=64, start="2024-01-31", seed=1),
            "ingest 2024-02",
            append_dim="time",
        )
        show_diff(repo, first, second, "append 29 days along time")
        print("    Nothing was created or deleted. Both arrays are reported as *updated*")
        print("    because their shape along time grew, and only the chunks covering the new")
        print("    time steps were written. The January chunks are untouched and shared.")

        # SECTION: a surgical correction
        print("\nNext the audit case: one day came out wrong and gets patched in place with a")
        print("region write. The claim to check is that the fix touched only that day.")
        session: Any = repo.writable_session("main")
        patch = read_dataset(repo).isel(time=slice(0, 1)).load()
        patch["t2m"] = patch.t2m + 1.5
        patch.drop_vars("time").to_zarr(session.store, region={"time": slice(0, 1)}, consolidated=False)
        third = session.commit("correct 2024-01-01")
        show_diff(repo, second, third, "region write over time step 0")
        chunking = read_dataset(repo).t2m.encoding["chunks"]
        print(f"    Only t2m changed, and only 2 of its chunks. With chunks={chunking} the grid")
        print("    is split in two along y, so day 0 lives in chunks [0, 0, 0] and [0, 1, 0].")
        print("    The time array is untouched, no other array moved, and no chunk outside")
        print("    time-chunk 0 was rewritten: the fix was surgical, and this is the evidence")
        print("    rather than the writer's word for it. Note the honest caveat -- chunking is")
        print("    the resolution of the audit, so those 2 chunks also carry days 1..14.")

        # SECTION: a metadata-only change
        print("\nA fourth kind: changing only metadata, writing no data at all.")
        meta_session: Any = repo.writable_session("main")
        group = zarr.open_group(meta_session.store, mode="a")
        group.attrs["source"] = "era5-land"
        group.attrs["ingest_version"] = "2.1"
        fourth = meta_session.commit("record provenance attrs")
        show_diff(repo, third, fourth, "set two attrs on the root group")
        print("    updated_groups names the root and updated_chunks is empty -- proof that a")
        print("    provenance edit did not silently rewrite any data.")

        # SECTION: a full overwrite, for contrast
        print("\nAnd the case to avoid. A mode='w' rewrite does not update the arrays, it")
        print("deletes and recreates them:")
        fifth = write_dataset(repo, climate_dataset(days=59, ny=64, nx=64, start="2024-01-01"), "full rewrite")
        show_diff(repo, fourth, fifth, "mode='w' over the whole store")
        print("    The same path shows up under both deleted_arrays and new_arrays. The diff")
        print("    is honest -- that really is what happened -- but it carries no information")
        print("    about which values moved, and the attrs from the previous commit are gone:")
        print(f"      attrs at {fourth[:8]}: {read_dataset(repo, snapshot_id=fourth).attrs}")
        print(f"      attrs at {fifth[:8]}: {read_dataset(repo, snapshot_id=fifth).attrs}")
        print("    Prefer append_dim= or region= when you want an auditable history.")

        # SECTION: diffs span any distance
        print("\nA diff is not limited to adjacent commits -- it compares two states, so it")
        print("summarizes an entire run of an ingest in one call:")
        show_diff(repo, first, fourth, "everything between the first ingest and the attrs commit")

        # SECTION: the constraint on which pairs can be diffed
        print("\nOne constraint worth knowing: diff walks ancestry, so `from` must be an")
        print("ancestor of `to`. It is not a symmetric comparison of two arbitrary states.")
        for label, (from_id, to_id) in (
            ("backwards, newer -> older ", (fourth, first)),
            ("a snapshot against itself ", (third, third)),
        ):
            try:
                repo.diff(from_snapshot_id=from_id, to_snapshot_id=to_id)
            except icechunk.IcechunkError as exc:
                print(f"  {label} raises {type(exc).__name__}: {str(exc).splitlines()[0]}")
        print("  So a diff always reads as 'what happened between then and now', never as")
        print("  'how do these two branches differ' -- for that, diff each from their fork.")

        # SECTION: the history that produced all this
        print("\nThe history the diffs were taken over:")
        for snapshot in repo.ancestry(branch="main"):
            print(f"    {str(snapshot.id)[:8]}  {snapshot.message}")
        final = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
        print(f"  tip holds time={final.sizes['time']} steps, mean={float(final.t2m.mean()):.3f}")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- repo.diff(from_snapshot_id=, to_snapshot_id=) reports what a run of commits did")
    print("- it reports created/deleted/updated groups and arrays, plus chunk coordinates")
    print("- an append shows updated array definitions and only the new chunks")
    print("- a region write names the exact chunks touched: an auditable, surgical fix")
    print("- a metadata edit reports updated_groups with no chunks at all")
    print("- a mode='w' rewrite reports delete-then-create, which tells you nothing useful")
    print("- diffs span any distance, but `from` must be an ancestor of `to`")


if __name__ == "__main__":
    main()
