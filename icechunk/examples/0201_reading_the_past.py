"""Time travel: reading the store as it stood at any earlier snapshot.

What: writes three revisions of the same dataset, each shifted by a different
``offset=``, keeping the snapshot id each commit returns. Then reads every
snapshot back and shows the values differ -- and that writing a fourth revision
leaves all three earlier reads bit-for-bit identical.

Why: when an open-climate-service dataset is reprocessed, "what did the model
actually train on" and "what did the API return in March" stay answerable. A
snapshot id is a permanent, quotable handle on a dataset version, which is what
makes a prediction reproducible after the store has moved on.

Run: make run EXAMPLE=0201_reading_the_past
"""

import tempfile
from pathlib import Path
from typing import Any

import numpy as np

from climate_stack_icechunk import (
    climate_dataset,
    open_repo,
    quiet_icechunk_logs,
    read_dataset,
    write_dataset,
)


def summarize(repo: Any, snapshot_id: str) -> tuple[float, float]:
    """Read one snapshot and return a summary of its ``t2m`` values.

    Args:
        repo: Repository to read from.
        snapshot_id: Snapshot to read at.

    Returns:
        The mean and the first grid value of ``t2m`` at that snapshot.
    """
    ds = read_dataset(repo, snapshot_id=snapshot_id)
    return float(ds.t2m.mean()), float(ds.t2m.isel(time=0, y=0, x=0))


def main() -> None:
    """Write successive revisions and read each one back from its snapshot id."""
    quiet_icechunk_logs()

    with tempfile.TemporaryDirectory() as tmpdir:
        repo_path = Path(tmpdir) / "era5_t2m.icechunk"
        repo: Any = open_repo(repo_path)

        # SECTION: writing revisions and keeping the ids
        print("Write the same 30-day grid three times, each revision shifted by a known")
        print("offset. commit() returns the snapshot id -- keep it and you keep the version.")
        revisions = (
            (0.0, "ingest v1: raw era5"),
            (5.0, "ingest v2: bias correction applied"),
            (-2.0, "ingest v3: bias correction fixed"),
        )
        ids: dict[str, str] = {}
        for offset, message in revisions:
            ds = climate_dataset(days=30, ny=32, nx=32, offset=offset, seed=0)
            snapshot_id = write_dataset(repo, ds, message)
            ids[message] = snapshot_id
            print(f"  offset={offset:+6.1f} -> {snapshot_id}  {message}")

        # SECTION: reading each one back
        print("\nEvery one of those ids is still readable. readonly_session(snapshot_id=...)")
        print("gives a store frozen at that commit; xarray opens it like anything else.")
        for message, snapshot_id in ids.items():
            mean, first = summarize(repo, snapshot_id)
            print(f"  {snapshot_id[:8]}  mean={mean:>7.3f}  t2m[0,0,0]={first:>7.3f}  {message}")

        tip_mean, _ = summarize(repo, ids["ingest v3: bias correction fixed"])
        print(f"\n  The branch tip (what a plain read gets) is v3: mean={tip_mean:.3f}")
        print(f"  A plain read_dataset(repo) agrees: mean={float(read_dataset(repo).t2m.mean()):.3f}")
        print("  The v1 and v2 values did not survive by luck -- they are separate chunk")
        print("  files that the v1 and v2 snapshots still point at.")

        # SECTION: the differences are exactly what we wrote
        print("\nThe gaps between revisions are exactly the offsets, so nothing has been")
        print("quietly rewritten under us:")
        v1 = summarize(repo, ids["ingest v1: raw era5"])[0]
        v2 = summarize(repo, ids["ingest v2: bias correction applied"])[0]
        v3 = summarize(repo, ids["ingest v3: bias correction fixed"])[0]
        print(f"  v2 - v1 = {v2 - v1:+.3f}  (expected +5.000)")
        print(f"  v3 - v1 = {v3 - v1:+.3f}  (expected -2.000)")

        # SECTION: later writes cannot disturb earlier snapshots
        print("\nNow the load-bearing claim: capture the full arrays at v1 and v2, write two")
        print("more revisions on top, then re-read v1 and v2 and compare element by element.")
        before_v1 = read_dataset(repo, snapshot_id=ids["ingest v1: raw era5"]).t2m.values.copy()
        before_v2 = read_dataset(repo, snapshot_id=ids["ingest v2: bias correction applied"]).t2m.values.copy()

        for offset, message in ((100.0, "ingest v4: bad units, degK"), (0.5, "ingest v5: reverted to degC")):
            snapshot_id = write_dataset(repo, climate_dataset(days=30, ny=32, nx=32, offset=offset, seed=0), message)
            print(f"  wrote {snapshot_id[:8]}  {message}")

        after_v1 = read_dataset(repo, snapshot_id=ids["ingest v1: raw era5"]).t2m.values
        after_v2 = read_dataset(repo, snapshot_id=ids["ingest v2: bias correction applied"]).t2m.values
        print(f"  v1 identical after two more commits: {bool(np.array_equal(before_v1, after_v1))}")
        print(f"  v2 identical after two more commits: {bool(np.array_equal(before_v2, after_v2))}")
        print(f"  max |difference| at v1: {float(np.abs(after_v1 - before_v1).max()):.1e}")

        # SECTION: the shape of a real recovery
        print("\nThis is what makes a bad ingest survivable. v4 shipped Kelvin by mistake;")
        print("the correct data was never lost, it just stopped being the tip:")
        for row in reversed([r for r in repo.ancestry(branch="main")][:5]):
            mean, _ = summarize(repo, row.id)
            print(f"  {str(row.id)[:8]}  mean={mean:>8.3f}  {row.message}")
        print("  A model trained against the v2 id keeps reading v2 forever, even though")
        print("  main has moved on twice since.")

        # SECTION: a snapshot id is enough on its own
        print("\nAnd the id is all a caller needs -- no branch, no coordination with the writer:")
        quoted = ids["ingest v2: bias correction applied"]
        reopened: Any = open_repo(repo_path)
        ds_quoted = read_dataset(reopened, snapshot_id=quoted)
        print(f"  reopened the repo from disk, read snapshot {quoted}")
        print(f"  sizes={dict(ds_quoted.sizes)}, mean={float(ds_quoted.t2m.mean()):.3f}")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- commit() returns a snapshot id; that id is a permanent handle on a version")
    print("- readonly_session(snapshot_id=...) opens the store exactly as it was then")
    print("- later commits add new chunks; they never touch the chunks an old snapshot uses")
    print("- so old reads are bit-for-bit stable, verified here with array_equal")
    print("- quoting a snapshot id makes a prediction reproducible after a reprocess")


if __name__ == "__main__":
    main()
