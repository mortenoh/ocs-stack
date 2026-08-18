"""Scaling: matching cluster capacity to the work in front of it.

What: runs the same batch against progressively fewer workers to measure what
capacity actually buys, then explains graceful retirement and how capacity is
added back.

Why: capacity is a dial, not a constant. Scaling up shortens a queue only if
the work actually parallelizes; scaling down has to move data off a worker
before stopping it, or you lose results. Adding containers is an
infrastructure action -- ``make scale N=5`` here, a replica count in
Kubernetes in production -- while retiring is something the client can drive.

Run: make run EXAMPLE=0403_scaling
"""

import time
from typing import Any

from ocs_stack_dask_distributed import connect

N_TASKS = 24


def unit_of_work(index: int, seconds: float = 0.25) -> int:
    """Hold a task slot for a fixed time.

    Sleeping keeps the batch's duration governed purely by how many slots the
    cluster has, which is what this example measures.

    Args:
        index: Task identifier, returned unchanged.
        seconds: How long to occupy the slot.

    Returns:
        The index it was given.
    """
    time.sleep(seconds)
    return index


def capacity(client: Any) -> tuple[int, int]:
    """Return the current worker count and total thread count.

    Args:
        client: A connected client.

    Returns:
        The number of workers and the sum of their threads.
    """
    workers = client.scheduler_info().get("workers", {})
    return len(workers), sum(int(w.get("nthreads", 0)) for w in workers.values())


def timed_batch(client: Any, n_tasks: int) -> float:
    """Run a batch of sleep tasks and return the wall time.

    Args:
        client: A connected client.
        n_tasks: How many tasks to submit.

    Returns:
        Seconds taken for the whole batch.
    """
    started = time.perf_counter()
    client.gather([client.submit(unit_of_work, i, pure=False) for i in range(n_tasks)])
    return time.perf_counter() - started


def main() -> None:
    """Measure throughput, shrink the cluster, and let it recover."""
    with connect() as session:
        client = session.client
        print(session.banner())

        # SECTION: current capacity and what it buys
        workers, threads = capacity(client)
        print(f"\nCurrent capacity: {workers} workers, {threads} threads.")
        print(f"Running {N_TASKS} tasks of 0.25s each.")
        serial = N_TASKS * 0.25
        elapsed = timed_batch(client, N_TASKS)
        print(f"  wall time {elapsed:.2f}s versus {serial:.1f}s serial ({serial / elapsed:.1f}x)")
        print(f"  with {threads} slots the floor is about {serial / threads:.2f}s")

        if not session.is_compose:
            print("\nThe fallback cluster cannot be rescaled meaningfully -- its workers")
            print("are threads in this process. Run 'make up' for the real thing.")
            print("\n=== Summary ===")
            print("- throughput is governed by total threads, not worker count")
            print("- adding workers is an infrastructure action: make scale N=5")
            print("- retire_workers() drains a worker's data before removing it")
            print("- scaling up only helps work that actually parallelizes")
            return

        # SECTION: less capacity, same work
        # Restricting the batch to a subset of workers measures the effect of a
        # smaller cluster without actually shrinking the shared one. Retiring
        # workers for real is covered in prose below, because a graceful
        # retirement is permanent -- the nanny does not undo it.
        addresses = sorted(client.scheduler_info()["workers"])
        print("\nThe same batch, restricted to fewer workers with workers=:")
        for count in range(len(addresses), 0, -1):
            subset = addresses[:count]
            slots = sum(
                int(meta.get("nthreads", 0))
                for address, meta in client.scheduler_info()["workers"].items()
                if address in subset
            )
            started = time.perf_counter()
            client.gather([client.submit(unit_of_work, i, pure=False, workers=subset) for i in range(N_TASKS)])
            took = time.perf_counter() - started
            bar = "#" * int(took * 8)
            print(f"  {count} worker(s), {slots} slots: {took:5.2f}s  {bar}")
        print("  Halving the slots roughly doubles the wall time: throughput tracks")
        print("  total threads, and nothing failed when capacity got tight.")

        # SECTION: shrinking for real
        print("\nTo genuinely remove a worker, client.retire_workers() is the safe way.")
        print("Unlike killing it, retiring MOVES its data to the survivors first, then")
        print("stops it -- the drain a rolling deploy needs. Two things to know:")
        print("  - it is permanent: a retired worker is not restarted by its nanny,")
        print("    unlike a crashed one, so capacity has to be added back explicitly")
        print("  - data with no recipe (anything scattered) is preserved by the drain,")
        print("    which is exactly what killing the worker would have destroyed")
        print("This example does not run it, so the cluster it shares stays intact.")

        # SECTION: scaling up is an infrastructure action
        print("\nGrowing the cluster happens outside Python:")
        print("  make scale N=5        add containers to this compose cluster")
        print("  docker compose up -d --scale worker=5")
        print("In Kubernetes it is a replica count; with dask-kubernetes or")
        print("adaptive clusters, cluster.adapt(minimum=2, maximum=10) lets the")
        print("scheduler request capacity based on the queue itself.")

        print("\nScaling up helps only when the work splits. Twelve tasks across six")
        print("slots halve the wall time; one long task does not care how many")
        print("workers you add.")

        # SECTION: summary
        print("\n=== Summary ===")
        print("- throughput is governed by total threads, not by worker count")
        print("- retire_workers() drains data off a worker before removing it")
        print("- killing a worker loses its data; retiring it does not")
        print("- adding capacity is an infrastructure action, not a client call")
        print("- more workers only help work that actually parallelizes")


if __name__ == "__main__":
    main()
