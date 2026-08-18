"""Conflicts: two writers on one branch, and how the loser recovers.

What: opens two writable sessions from the same branch tip and commits both.
The first succeeds; the second raises ``icechunk.ConflictError``. The example
prints exactly what the error says, then shows both outcomes of ``rebase``:
disjoint chunk edits replay cleanly, while two appends to the same dimension
cannot be reconciled and have to be redone on a fresh session.

Why: open-climate-service can have a scheduled ingest and a manual backfill
aimed at the same dataset. Icechunk will not silently interleave them -- the
second commit fails loudly, and the job has to decide whether its work still
makes sense against the new tip.

Run: make run EXAMPLE=0303_conflicts
"""

import tempfile
from pathlib import Path
from typing import Any

import icechunk  # no type stubs; the Any-typed locals below are deliberate

from ocs_stack_icechunk import (
    climate_dataset,
    describe_history,
    open_repo,
    quiet_icechunk_logs,
    read_dataset,
)


def seed_repository(repo: Any) -> str:
    """Write four days with one zarr chunk per day, so chunks can be edited apart.

    Args:
        repo: The repository to seed.

    Returns:
        The snapshot id of the seed commit.
    """
    session = repo.writable_session("main")
    ds = climate_dataset(days=4, ny=16, nx=16, offset=0.0)
    ds.to_zarr(session.store, mode="w", zarr_format=3, consolidated=False, encoding={"t2m": {"chunks": (1, 16, 16)}})
    return str(session.commit("seed 2024-01-01..04"))


def step_means(repo: Any) -> list[float]:
    """Read the branch tip and reduce each time step to its spatial mean.

    Args:
        repo: The repository to read.

    Returns:
        One rounded mean per time step.
    """
    ds = read_dataset(repo)
    return [round(float(ds.t2m.isel(time=i).mean()), 1) for i in range(ds.sizes["time"])]


def print_history(repo: Any) -> None:
    """Print the branch history, newest first.

    Args:
        repo: The repository to inspect.
    """
    for entry in describe_history(repo):
        print(f"    {entry['short']}  {entry['message']}")


def demo_resolvable_conflict(repo: Any) -> None:
    """Two writers touch different chunks: the commit fails, the rebase succeeds.

    Args:
        repo: A seeded repository whose ``t2m`` has one chunk per time step.
    """
    writer_a: Any = repo.writable_session("main")
    writer_b: Any = repo.writable_session("main")
    print(f"  both sessions start from parent {str(writer_a.snapshot_id)[:8]}")

    fix_a = climate_dataset(days=1, ny=16, nx=16, start="2024-01-01", offset=50.0).drop_vars("time")
    fix_a.to_zarr(writer_a.store, region={"time": slice(0, 1)}, consolidated=False)
    fix_b = climate_dataset(days=1, ny=16, nx=16, start="2024-01-04", offset=70.0).drop_vars("time")
    fix_b.to_zarr(writer_b.store, region={"time": slice(3, 4)}, consolidated=False)
    print("  writer A rewrites step 0; writer B rewrites step 3 -- different chunks")

    print(f"  A commits first: {str(writer_a.commit('A corrects 2024-01-01'))[:8]}")
    try:
        writer_b.commit("B corrects 2024-01-04")
    except icechunk.ConflictError as exc:
        print("  B commits second and raises icechunk.ConflictError:")
        print(f"    {exc}")
        print(f"    expected_parent={str(exc.expected_parent)[:8]}  actual_parent={str(exc.actual_parent)[:8]}")
        print("  the check is purely on the parent pointer, so it fires even for disjoint edits")
        print("  B rebases with a ConflictDetector, which raises only on real overlap:")
        writer_b.rebase(icechunk.ConflictDetector())
        print("    rebase returned -- no chunk was written by both sides")
        print(f"  B commits again: {str(writer_b.commit('B corrects 2024-01-04'))[:8]}")

    print(f"  both corrections survive: step means = {step_means(repo)}")
    print_history(repo)


def demo_unresolvable_conflict(repo: Any) -> None:
    """Two writers append to the same dimension: no rebase can reconcile them.

    Args:
        repo: A repository whose branch tip both writers will branch from.
    """
    writer_a: Any = repo.writable_session("main")
    writer_b: Any = repo.writable_session("main")
    print(f"  both sessions start from parent {str(writer_a.snapshot_id)[:8]}")

    batch_a = climate_dataset(days=2, ny=16, nx=16, start="2024-01-05", offset=10.0)
    batch_a.to_zarr(writer_a.store, append_dim="time", consolidated=False)
    batch_b = climate_dataset(days=2, ny=16, nx=16, start="2024-01-05", offset=20.0)
    batch_b.to_zarr(writer_b.store, append_dim="time", consolidated=False)
    print("  both append 2 steps for 2024-01-05..06 -- both grow time from 4 to 6")

    print(f"  A commits first: {str(writer_a.commit('A ingests 2024-01-05..06'))[:8]}")
    try:
        writer_b.commit("B ingests 2024-01-05..06")
    except icechunk.ConflictError as exc:
        print(f"  B raises icechunk.ConflictError: {exc}")

    for solver_name, solver in (
        ("ConflictDetector()", icechunk.ConflictDetector()),
        ("BasicConflictSolver()", icechunk.BasicConflictSolver()),
    ):
        try:
            writer_b.rebase(solver)
            print(f"  rebase({solver_name}) succeeded")
        except icechunk.RebaseFailedError as exc:
            print(f"  rebase({solver_name}) raises icechunk.RebaseFailedError:")
            print(f"    {exc}")
            for conflict in exc.conflicts:
                chunks = "" if conflict.conflicted_chunks is None else f"  chunks={conflict.conflicted_chunks}"
                print(f"      {conflict.conflict_type} on {conflict.path}{chunks}")

    print("  both sides changed the array's shape metadata; there is no correct merge of that")
    print("  the resolution is to discard B's session, open a fresh one, and redo the work:")
    writer_b.discard_changes()
    retry: Any = repo.writable_session("main")
    already_present = int(read_dataset(repo).sizes["time"])
    print(f"    fresh session sees time={already_present}; B's 2 steps are already there, so B appends the next 2")
    batch_b_retry = climate_dataset(days=2, ny=16, nx=16, start="2024-01-07", offset=20.0)
    batch_b_retry.to_zarr(retry.store, append_dim="time", consolidated=False)
    print(f"    retry commits: {str(retry.commit('B ingests 2024-01-07..08'))[:8]}")
    print(f"  final step means = {step_means(repo)}")
    print_history(repo)


def main() -> None:
    """Show conflict detection, a rebase that works, and one that cannot."""
    quiet_icechunk_logs()

    with tempfile.TemporaryDirectory() as tmp:
        repo: Any = open_repo(Path(tmp) / "conflicts.icechunk")

        # SECTION: the shared starting point
        print("Seed the repository with 4 days, one zarr chunk per day.")
        seed = seed_repository(repo)
        print(f"  snapshot {seed[:8]}: step means = {step_means(repo)}")

        # SECTION: a conflict that rebase can replay
        print("\nCase 1 -- two writers, different chunks. The conflict is resolvable.")
        demo_resolvable_conflict(repo)

        # SECTION: a conflict that rebase cannot replay
        print("\nCase 2 -- two writers, both appending. The conflict is not resolvable.")
        demo_unresolvable_conflict(repo)

        # SECTION: why local filesystem storage warns about this
        print("\nWhy local filesystem storage warns about concurrent commits:")
        print("  the commit is a compare-and-swap on the branch reference: write the new snapshot id")
        print("  only if the reference still points at the parent this session started from")
        print("  object stores like S3 provide that as a conditional PUT, so the swap is atomic")
        print("  a POSIX filesystem has no portable conditional write, so two processes committing")
        print("  at the same instant can both believe they won -- icechunk warns rather than pretend")
        print("  in-process sessions like this example are still detected correctly; the gap is")
        print("  multi-process writers against one local directory, which is why OCS uses one writer")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- a commit fails if the branch moved since the session opened: icechunk.ConflictError")
    print("- the error names expected_parent and actual_parent -- it is a parent-pointer check")
    print("- session.rebase(solver) replays the session on the new tip; ConflictDetector only reports")
    print("- BasicConflictSolver(on_chunk_conflict=VersionSelection.UseOurs) can pick a side for")
    print("  overlapping chunks, but nothing can pick a side for a doubly-updated array shape")
    print("- rebase raises icechunk.RebaseFailedError listing each Conflict: type, path, chunks")
    print("- two appends to the same dim are unresolvable by construction; re-read the tip and redo")
    print("- commit(rebase_with=..., rebase_tries=N) folds the retry loop into the commit call")
    print("- local filesystem storage cannot make the reference swap atomic across processes")


if __name__ == "__main__":
    main()
