"""Worker memory: limits, thresholds, and what happens as they fill.

What: reads each worker's memory limit and the four thresholds distributed
uses, then watches managed memory rise as data is persisted and fall when it
is released.

Why: a worker is not an infinite bucket. Each container here has a hard
1.5 GiB limit, and distributed reacts in stages as that fills -- spilling to
disk, pausing, and finally killing the worker. Most "my cluster died"
incidents are this, and the fix is chunk sizing rather than more memory.

Run: make run EXAMPLE=0301_worker_memory
"""

import time
from typing import Any

import dask.array as da
import dask.config

from climate_stack_dask_distributed import connect, describe_workers

# The thresholds live in dask config as fractions of each worker's limit.
THRESHOLDS = (
    ("target", "distributed.worker.memory.target", "start spilling the least-recently-used data to disk"),
    ("spill", "distributed.worker.memory.spill", "spill aggressively; the worker is now doing disk I/O, not work"),
    ("pause", "distributed.worker.memory.pause", "stop accepting new tasks; the worker goes quiet"),
    ("terminate", "distributed.worker.memory.terminate", "kill and restart the worker; its in-memory data is lost"),
)


def worker_memory_mb(client: Any) -> dict[str, float]:
    """Return managed memory in MB per worker, keyed by address.

    Args:
        client: A connected client.

    Returns:
        A mapping of worker address to its currently managed memory in MB.
    """
    info = client.scheduler_info()
    usage: dict[str, float] = {}
    for address, meta in info.get("workers", {}).items():
        metrics = meta.get("metrics", {})
        usage[address] = float(metrics.get("managed_bytes", 0)) / 1e6
    return usage


def main() -> None:
    """Report memory limits and thresholds, then watch usage rise and fall."""
    with connect() as session:
        client = session.client
        workers = describe_workers(client)
        print(session.banner())

        # SECTION: the hard limits
        total_gib = round(sum(w["memory_limit_gib"] for w in workers), 2)
        print(f"\n{len(workers)} workers, {total_gib} GiB of memory in total:")
        for worker in workers:
            print(f"  {worker['address']:<28} limit {worker['memory_limit_gib']} GiB")

        # SECTION: the thresholds
        print("\nAs a worker's memory fills, distributed escalates through four stages")
        print("(fractions of that worker's own limit):")
        limit_gib = workers[0]["memory_limit_gib"] if workers else 0.0
        for name, key, effect in THRESHOLDS:
            fraction = dask.config.get(key, default=None)
            if isinstance(fraction, (int, float)):
                at_gib = round(float(fraction) * limit_gib, 2)
                print(f"  {name:<10} {float(fraction):.2f}  (~{at_gib} GiB here) -- {effect}")
            else:
                print(f"  {name:<10} {str(fraction):<6} -- {effect}")

        # SECTION: watch memory rise
        print("\nPersisting a ~400 MB array and watching managed memory rise.")
        before = worker_memory_mb(client)
        print(f"  before: {sum(before.values()):.0f} MB managed across the cluster")

        array = da.random.random((7000, 7000), chunks=(1750, 1750))
        print(f"  the array is {array.nbytes / 1e6:.0f} MB in {array.npartitions} chunks")
        persisted = array.persist()
        client.gather(client.futures_of(persisted))
        time.sleep(1.0)  # let the workers report fresh metrics to the scheduler

        during = worker_memory_mb(client)
        print(f"  after persist: {sum(during.values()):.0f} MB managed")
        for address, mb in sorted(during.items()):
            limit_mb = next((w["memory_limit_gib"] * 1024 for w in workers if w["address"] == address), 0.0)
            pct = (mb / limit_mb * 100) if limit_mb else 0.0
            print(f"    {address:<28} {mb:>7.0f} MB  ({pct:.0f}% of its limit)")

        # SECTION: and fall again
        del persisted
        time.sleep(1.5)  # release propagates asynchronously
        after = worker_memory_mb(client)
        print(f"\n  after releasing the reference: {sum(after.values()):.0f} MB managed")
        print("  Dropping the last reference is what frees cluster memory. A persisted")
        print("  result you never release is a leak that outlives the computation.")

        # SECTION: the practical advice
        print("\nStaying under the limit is a chunking problem, not a hardware problem:")
        print("  - a worker needs room for several chunks at once, not just one")
        print("  - rule of thumb: chunk size around 100 MB, and well under limit/threads")
        print("  - unmanaged memory (numpy scratch, leaked references) counts too")
        print("  - persist() only what you will reuse; the rest should stay lazy")

        if not session.is_compose:
            print("\nNote: the fallback shares this process's memory and has no hard limit,")
            print("so the numbers above are not a real demonstration. Run 'make up'.")

        # SECTION: summary
        print("\n=== Summary ===")
        print("- every worker has a hard memory limit; here it is 1.5 GiB per container")
        print("- target/spill/pause/terminate are the four escalating responses")
        print("- managed memory is visible in scheduler_info metrics, per worker")
        print("- releasing the last reference to a persisted result frees the memory")
        print("- the fix for memory pressure is smaller chunks, not a bigger box")


if __name__ == "__main__":
    main()
