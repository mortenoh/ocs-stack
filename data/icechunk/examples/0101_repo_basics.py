"""Repository basics: creating a store, what lands on disk, and sessions.

What: creates an icechunk repository in a temporary directory, walks the files
it writes, and contrasts a writable session with a readonly one -- ending on
the fact that a session's ``.store`` is just a zarr store as far as xarray is
concerned.

Why: every dataset in open-climate-service lives at
``{data_dir}/downloads/{dataset_id}.icechunk``. That path looks like a
directory of data, but it is not a zarr tree you can poke at -- it is a log of
snapshots, manifests, and content-addressed chunks. Knowing the shape of it
explains why appending to a dataset while it is being served is safe.

Run: make run EXAMPLE=0101_repo_basics
"""

import tempfile
from pathlib import Path
from typing import Any

import icechunk
import xarray as xr

from playground_data_icechunk import climate_dataset, quiet_icechunk_logs


def walk_repo(root: Path) -> list[tuple[str, int, int]]:
    """List every directory in a repository with its file count and byte size.

    Args:
        root: Repository directory to walk.

    Returns:
        One ``(relative_path, file_count, total_bytes)`` tuple per directory,
        sorted by path.
    """
    rows: list[tuple[str, int, int]] = []
    for directory in sorted(p for p in root.rglob("*") if p.is_dir()):
        files = [p for p in directory.iterdir() if p.is_file()]
        rows.append((directory.relative_to(root).as_posix(), len(files), sum(p.stat().st_size for p in files)))
    top_files = [p for p in root.iterdir() if p.is_file()]
    rows.insert(0, (".", len(top_files), sum(p.stat().st_size for p in top_files)))
    return rows


def print_layout(root: Path, label: str) -> None:
    """Print the on-disk layout of a repository under a heading.

    Args:
        root: Repository directory to walk.
        label: Heading describing the moment being shown.
    """
    print(f"  {label}")
    for path, count, size in walk_repo(root):
        print(f"    {path:<14} {count:>3} entries  {size:>8} bytes")


def main() -> None:
    """Create a repository, inspect its on-disk form, and compare session kinds."""
    quiet_icechunk_logs()

    with tempfile.TemporaryDirectory() as tmpdir:
        repo_path = Path(tmpdir) / "era5_t2m.icechunk"

        # SECTION: creating a repository
        print("A repository is created against a storage backend. Here that backend is a")
        print("local directory; in production it is S3 or GCS with the same API.")
        # icechunk ships no type stubs, so its objects are annotated as Any.
        repo: Any = icechunk.Repository.create(icechunk.local_filesystem_storage(str(repo_path)))
        print(f"  created {repo_path.name}")
        print(f"  branches at creation: {sorted(repo.list_branches())}")

        print("\nCreation alone already writes files -- an empty repository is not an empty")
        print("directory. It has one snapshot: the empty root the first commit descends from.")
        print_layout(repo_path, "on disk after create:")

        # SECTION: writing through a session
        print("\nWrites go through a session bound to a branch. The session hands you a store;")
        print("xarray writes into it exactly as it would write a plain zarr directory.")
        session: Any = repo.writable_session("main")
        print(f"  session.store is a {type(session.store).__name__}")
        print(f"  uncommitted changes before writing: {session.has_uncommitted_changes}")

        ds = climate_dataset(days=30, ny=64, nx=64)
        ds.to_zarr(session.store, mode="w", zarr_format=3, consolidated=False)
        print(f"  uncommitted changes after writing:  {session.has_uncommitted_changes}")
        snapshot_id = session.commit("ingest 2024-01 t2m")
        print(f"  committed as snapshot {snapshot_id}")

        # SECTION: what a commit leaves behind
        print("\nAfter the commit the directory has grown four new kinds of file:")
        print_layout(repo_path, "on disk after commit:")
        print("  chunks/      the compressed array data, content-addressed")
        print("  manifests/   which chunk file holds which chunk coordinate")
        print("  snapshots/   the immutable tree of arrays and metadata per commit")
        print("  transactions/ the change log entry each commit appends")
        print("  overwritten/ superseded copies of the small repository config file")

        # SECTION: this is not a zarr tree
        print("\nNote what is NOT there. A plain zarr v3 store on disk would contain")
        print("zarr.json metadata files and a t2m/ directory of chunk keys:")
        names = sorted({p.name for p in repo_path.rglob("*") if p.is_file()})
        print(f"  any file named zarr.json? {any(n == 'zarr.json' for n in names)}")
        print(f"  any directory named t2m?  {any(p.name == 't2m' for p in repo_path.rglob('*') if p.is_dir())}")
        print("  every filename is an opaque id:")
        for name in names[:4]:
            print(f"    {name}")
        print("  The zarr hierarchy exists only inside the snapshot files. You cannot")
        print("  read this store by pointing zarr at the directory -- you go through icechunk.")

        # SECTION: writable versus readonly sessions
        print("\nTwo kinds of session, and the difference is enforced by the store itself:")
        writable: Any = repo.writable_session("main")
        readonly: Any = repo.readonly_session("main")
        print(f"  writable_session('main'): read_only={writable.read_only}, branch={writable.branch!r}")
        print(f"  readonly_session('main'): read_only={readonly.read_only}, branch={readonly.branch!r}")
        print(f"  the readonly session pinned snapshot {readonly.snapshot_id}")
        try:
            ds.to_zarr(readonly.store, mode="w", zarr_format=3, consolidated=False)
        except ValueError as exc:
            print(f"  writing to the readonly store raises {type(exc).__name__}: {exc}")

        # SECTION: the store is just a store
        print("\nAnd the payoff: a readonly session's store opens like any other zarr store.")
        loaded = xr.open_zarr(readonly.store, consolidated=False)
        print(f"  sizes={dict(loaded.sizes)}, vars={list(loaded.data_vars)}")
        print(f"  t2m chunks={loaded.t2m.encoding['chunks']}, mean={float(loaded.t2m.mean()):.3f}")
        print(f"  attrs={loaded.t2m.attrs}")

        # SECTION: reopening
        print("\nReopening the same path returns the same history -- nothing lives in memory.")
        reopened: Any = icechunk.Repository.open(icechunk.local_filesystem_storage(str(repo_path)))
        print(f"  tip of main: {reopened.lookup_branch('main')}")
        print(f"  matches the snapshot we committed: {reopened.lookup_branch('main') == snapshot_id}")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- Repository.create/.open bind a repository to a storage backend")
    print("- an empty repository already has one snapshot, 'Repository initialized'")
    print("- on disk: chunks, manifests, snapshots, transactions -- not a zarr tree")
    print("- filenames are opaque ids; the hierarchy lives inside the snapshot files")
    print("- writable_session(branch) can write; readonly_session pins one snapshot")
    print("- session.store is a zarr store, so xarray needs no icechunk-specific code")


if __name__ == "__main__":
    main()
