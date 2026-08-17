"""Tags and branches: naming snapshots and forking work off an old one.

What: tags a snapshot with a meaningful name and reads it back with ``tag=``,
then branches from an *older* snapshot, writes a correction on that branch, and
shows main is completely unaffected. Ends by listing both namespaces.

Why: nobody quotes a twenty-character snapshot id in a model card. A tag turns
one point in an open-climate-service store into a stable, human name --
``era5-2024q1-final`` -- that outlives reprocessing. A branch lets you rebuild a
suspect period from before the mistake without taking the served dataset
offline; you only move main once the fix reads correctly.

Run: make run EXAMPLE=0202_tags_and_branches
"""

import tempfile
from pathlib import Path
from typing import Any

import icechunk

from playground_icechunk import (
    climate_dataset,
    open_repo,
    quiet_icechunk_logs,
    read_dataset,
    write_dataset,
)


def branch_means(repo: Any, branches: tuple[str, ...]) -> None:
    """Print the tip snapshot and ``t2m`` mean of each named branch.

    Args:
        repo: Repository to read from.
        branches: Branch names to report on.
    """
    for name in branches:
        ds = read_dataset(repo, branch=name)
        print(f"    {name:<12} tip={repo.lookup_branch(name)[:8]}  mean={float(ds.t2m.mean()):>8.3f}")


def main() -> None:
    """Name snapshots with tags and fork an old snapshot onto its own branch."""
    quiet_icechunk_logs()

    with tempfile.TemporaryDirectory() as tmpdir:
        repo_path = Path(tmpdir) / "era5_t2m.icechunk"
        repo: Any = open_repo(repo_path)

        # SECTION: build a small history to name things in
        print("Build three revisions on main. Revision 2 is the one that shipped and was")
        print("correct; revision 3 is a reprocess that came out in Kelvin by mistake.")
        good = write_dataset(repo, climate_dataset(days=30, ny=32, nx=32, offset=0.0), "ingest era5 2024-q1 raw")
        shipped = write_dataset(
            repo, climate_dataset(days=30, ny=32, nx=32, offset=2.0), "ingest era5 2024-q1 bias corrected"
        )
        broken = write_dataset(
            repo, climate_dataset(days=30, ny=32, nx=32, offset=273.15), "reprocess era5 2024-q1 (bad units)"
        )
        for label, snapshot_id in (("raw    ", good), ("shipped", shipped), ("broken ", broken)):
            print(
                f"  {label} {snapshot_id[:8]}  mean={float(read_dataset(repo, snapshot_id=snapshot_id).t2m.mean()):.3f}"
            )

        # SECTION: tagging
        print("\nA tag is an immutable name for one snapshot. Give the shipped revision one:")
        repo.create_tag("era5-2024q1-final", shipped)
        print(f"  create_tag('era5-2024q1-final', {shipped[:8]}...)")
        by_tag = read_dataset(repo, tag="era5-2024q1-final")
        print(f"  read_dataset(repo, tag='era5-2024q1-final') -> mean={float(by_tag.t2m.mean()):.3f}")
        print(f"  which resolves to the same snapshot: {repo.lookup_tag('era5-2024q1-final') == shipped}")

        print("\n  Tags are immutable by design -- pointing one at a new snapshot is an error,")
        print("  which is exactly what you want from a name you published in a model card:")
        try:
            repo.create_tag("era5-2024q1-final", broken)
        except icechunk.AlreadyExistsError as exc:
            print(f"    retagging raises {type(exc).__name__}: {str(exc).splitlines()[0]}")

        repo.create_tag("era5-2024q1-raw", good)
        print(f"\n  A second tag for the raw ingest. list_tags() -> {sorted(repo.list_tags())}")

        # SECTION: branching from an older snapshot
        print("\nNow the fix. Rather than committing on top of the broken tip, branch from")
        print("the last good snapshot -- the branch starts life as a copy of that state.")
        repo.create_branch("fix-units", shipped)
        print(f"  create_branch('fix-units', {shipped[:8]}...)")
        print("  the new branch starts at the shipped snapshot; main stays on the broken tip:")
        branch_means(repo, ("main", "fix-units"))
        print("  No data was copied to make fix-units -- it is a name pointing at a snapshot.")

        # SECTION: writing on the branch
        print("\nRedo the reprocess on the branch, this time in Celsius:")
        fixed = write_dataset(
            repo,
            climate_dataset(days=30, ny=32, nx=32, offset=3.0),
            "reprocess era5 2024-q1 (degC, corrected)",
            branch="fix-units",
        )
        print(f"  wrote {fixed[:8]} on fix-units")
        branch_means(repo, ("main", "fix-units"))
        print("  main did not move. Anything serving from main is still serving what it")
        print("  was serving before -- wrong, but stable and unaffected by the repair work.")

        # SECTION: the branches have genuinely diverged
        print("\nThe two branches now have different histories that share a common ancestor:")
        for name in ("main", "fix-units"):
            chain = [f"{str(s.id)[:8]}" for s in repo.ancestry(branch=name)]
            print(f"    {name:<12} {' <- '.join(chain)}")
        print(f"  their shared ancestor is {shipped[:8]}, the snapshot fix-units forked from")

        print("\n  And the tag is unmoved by any of it -- it still names the shipped data:")
        print(f"    tag era5-2024q1-final -> mean={float(read_dataset(repo, tag='era5-2024q1-final').t2m.mean()):.3f}")

        # SECTION: promoting the fix
        print("\nWhen the fix reads correctly, move main to it. reset_branch is the single")
        print("atomic step that promotes the repaired data:")
        repo.reset_branch("main", fixed)
        branch_means(repo, ("main", "fix-units"))
        repo.create_tag("era5-2024q1-v2", fixed)
        print("  tagged the promoted snapshot era5-2024q1-v2")

        # SECTION: what exists now
        print("\nThe two namespaces, side by side:")
        print(f"  branches: {sorted(repo.list_branches())}")
        for tag in sorted(repo.list_tags()):
            snapshot_id = repo.lookup_tag(tag)
            print(f"  tag {tag:<20} -> {snapshot_id[:8]}  mean={float(read_dataset(repo, tag=tag).t2m.mean()):>8.3f}")
        print("\n  A branch moves as you commit to it. A tag never moves. Both are just")
        print("  names for snapshot ids -- neither copies a byte of data.")

        repo.delete_branch("fix-units")
        print(f"\n  Cleaning up the now-redundant branch: list_branches() -> {sorted(repo.list_branches())}")
        print("  Deleting the branch does not delete its snapshots; the history main now")
        print(f"  points at still runs through {fixed[:8]}.")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- create_tag(name, snapshot_id) gives one snapshot a permanent human name")
    print("- tags are immutable: retagging raises rather than silently repointing")
    print("- create_branch(name, snapshot_id) forks from any snapshot, not just a tip")
    print("- commits on a branch leave every other branch exactly where it was")
    print("- reset_branch promotes a fix onto main in one atomic step")
    print("- both are pointers to snapshot ids; branching and tagging copy no data")


if __name__ == "__main__":
    main()
