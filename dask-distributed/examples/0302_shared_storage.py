"""Shared storage: the client's filesystem is not the worker's.

What: proves that a file the client can see does not exist for the workers,
then uses the shared volume mounted at /data in every container to write a
zarr store from one worker and read it back from another.

Why: this is the single most common surprise when moving code from a
LocalCluster to a real deployment. ``ds.to_zarr("/tmp/out.zarr")`` works on a
laptop and silently writes into three different container filesystems on a
cluster. open-climate-service sidesteps this by having every worker read and
write the same object store; the path must be valid on the WORKER.

Run: make run EXAMPLE=0302_shared_storage
"""

import os
import socket
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from climate_stack_dask_distributed import connect

# Mounted into the scheduler and every worker by compose.yml.
SHARED_DIR = "/data"


def path_exists_on_worker(path: str) -> tuple[str, bool]:
    """Report whether a path exists from the worker's point of view.

    Args:
        path: A filesystem path, as seen by whoever runs this.

    Returns:
        The hostname that checked, and whether the path exists there.
    """
    return socket.gethostname(), os.path.exists(path)


def write_store(path: str, days: int, size: int, seed: int) -> tuple[str, int]:
    """Build a small climate-shaped dataset and write it as zarr.

    Runs on a worker, so the path must be valid inside that container.

    Args:
        path: Destination zarr store path.
        days: Number of daily time steps.
        size: Grid height and width.
        seed: Seed for reproducible values.

    Returns:
        The hostname that wrote the store and the number of files created.
    """
    rng = np.random.default_rng(seed)
    data = 20.0 + rng.normal(0.0, 3.0, size=(days, size, size))
    ds = xr.DataArray(
        data,
        dims=("time", "y", "x"),
        coords={"time": pd.date_range("2024-01-01", periods=days, freq="D")},
        name="t2m",
        attrs={"units": "degC"},
    ).to_dataset()
    # consolidated=False: consolidated metadata is not part of the zarr v3
    # spec, and writing it emits a ZarrUserWarning.
    ds.to_zarr(path, mode="w", consolidated=False)
    return socket.gethostname(), sum(1 for _ in Path(path).rglob("*") if _.is_file())


def read_store(path: str) -> tuple[str, float, tuple[int, ...]]:
    """Open a zarr store and summarize it, from whichever worker runs this.

    Args:
        path: The zarr store to read.

    Returns:
        The hostname that read it, the mean of t2m, and its shape.
    """
    ds = xr.open_zarr(path, consolidated=False)
    return socket.gethostname(), float(ds["t2m"].mean()), tuple(ds["t2m"].shape)


def remove_store(path: str) -> bool:
    """Delete a store so repeated runs stay clean.

    Args:
        path: The store to remove.

    Returns:
        True once the path no longer exists.
    """
    import shutil

    shutil.rmtree(path, ignore_errors=True)
    return not os.path.exists(path)


def main() -> None:
    """Show the filesystem boundary, then cross it with a shared volume."""
    with connect() as session:
        client = session.client
        print(session.banner())

        # SECTION: a path only the client can see
        print("\nFirst: write a file the CLIENT can see, and ask the workers about it.")
        with tempfile.TemporaryDirectory() as tmp:
            host_file = os.path.join(tmp, "client-only.txt")
            Path(host_file).write_text("written by the client\n")
            print(f"  client wrote {host_file}")
            print(f"  client sees it: {os.path.exists(host_file)}")

            answers = client.run(path_exists_on_worker, host_file)
            for address, (host, exists) in sorted(answers.items()):
                print(f"  worker {host} ({address.rsplit('/', 1)[-1]}) sees it: {exists}")

            if session.is_compose:
                print("\n  False everywhere. The workers are separate containers with their own")
                print("  filesystems -- the path is meaningless to them. Passing it to to_zarr()")
                print("  would not error; it would write somewhere useless.")
            else:
                print("\n  True everywhere, because the fallback runs in THIS process and shares")
                print("  its filesystem. That is exactly why the bug hides until deployment.")

        # SECTION: the shared volume
        if session.is_compose:
            base = SHARED_DIR
            print(f"\nNow the shared volume: compose mounts one docker volume at {base}")
            print("in the scheduler and every worker, so all containers see the same files.")
        else:
            base = tempfile.mkdtemp(prefix="dask-fallback-")
            print(f"\nNo containers, so standing in with a local directory: {base}")
            print("(Against the real cluster this would be the shared /data volume.)")

        store = f"{base}/demo.zarr"

        # SECTION: one worker writes
        writer_host, file_count = client.submit(write_store, store, 30, 64, 0).result()
        print(f"\n  worker {writer_host} wrote {store} ({file_count} files)")

        # SECTION: every worker can see it
        listings = client.run(os.listdir, base)
        print("\n  every container now lists the same directory:")
        for address, entries in sorted(listings.items()):
            print(f"    {address.rsplit('/', 1)[-1]:<22} {sorted(entries)}")

        # SECTION: a different worker reads it back
        reader_host, mean, shape = client.submit(read_store, store).result()
        print(f"\n  worker {reader_host} read it back: shape={shape}, mean t2m={mean:.2f} degC")
        if session.is_compose and reader_host != writer_host:
            print("  A DIFFERENT container than the one that wrote it -- the volume is genuinely shared.")

        # SECTION: clean up
        removed = client.submit(remove_store, store).result()
        print(f"\n  cleaned up the store: {removed}")

        # SECTION: summary
        print("\n=== Summary ===")
        print("- the client's filesystem and the workers' filesystems are different")
        print("- a local path passed to a worker is at best wrong and at worst silently wrong")
        print("- give workers a path THEY can resolve: a shared volume or an object store URL")
        print("- client.run() executes on every worker, which is how you inspect them")
        print("- this is the bug a LocalCluster can never show you")


if __name__ == "__main__":
    main()
