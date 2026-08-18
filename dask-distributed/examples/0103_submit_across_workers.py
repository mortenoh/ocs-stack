"""Submitting work: how tasks spread across worker containers.

What: submits a batch of tasks that each report where they ran, then tallies
the result per container to show the scheduler balancing work across the
cluster.

Why: on a LocalCluster "which worker ran this" is a curiosity. On a real
cluster it is the thing you reason about constantly -- it determines how much
data has to move, which worker's memory fills up, and what happens when one
container dies.

Run: make run EXAMPLE=0103_submit_across_workers
"""

import os
import socket
import time
from collections import Counter

from ocs_stack_dask_distributed import connect, describe_workers

N_TASKS = 12


def busy_task(task_id: int, seconds: float = 0.4) -> tuple[int, str, int]:
    """Occupy a worker thread briefly and report where it ran.

    Sleeping rather than computing keeps the example fast while still holding
    a task slot long enough for the scheduler to spread the batch out.

    Args:
        task_id: Identifier echoed back in the result.
        seconds: How long to hold the slot.

    Returns:
        The task id, the hostname that ran it, and its process id.
    """
    time.sleep(seconds)
    return task_id, socket.gethostname(), os.getpid()


def square(value: int) -> int:
    """Return the square of a value, for the client.map demo.

    Args:
        value: The number to square.

    Returns:
        value squared.
    """
    return value * value


def main() -> None:
    """Submit a batch of tasks and show how the scheduler distributes them."""
    with connect() as session:
        client = session.client
        workers = describe_workers(client)
        slots = sum(w["threads"] for w in workers)

        # SECTION: the capacity we are submitting into
        print(session.banner())
        print()
        print(f"{len(workers)} workers x {workers[0]['threads'] if workers else 0} threads = {slots} concurrent slots.")
        print(f"Submitting {N_TASKS} tasks of ~0.4s each into {slots} slots.")

        # SECTION: submit and gather
        started = time.perf_counter()
        futures = [client.submit(busy_task, i) for i in range(N_TASKS)]
        results = client.gather(futures)
        elapsed = time.perf_counter() - started

        serial = N_TASKS * 0.4
        print(f"\nWall time {elapsed:.2f}s versus {serial:.1f}s if run one after another")
        print(f"  speedup: {serial / elapsed:.1f}x (ideal would be {min(slots, N_TASKS)}x)")

        # SECTION: where did the work land
        placements = Counter(host for _, host, _ in results)
        print("\nTasks per worker container:")
        for host, count in sorted(placements.items()):
            bar = "#" * count
            print(f"  {host:<16} {count:>2}  {bar}")

        if session.is_compose:
            print(f"\n{len(placements)} distinct containers shared the batch.")
            print("The scheduler assigns each task to whichever worker has a free slot.")
        else:
            print("\nThe fallback runs everything in one process, so every task reports the")
            print("same host and pid. Start the containers (make up) to see real distribution.")

        # SECTION: client.map for the common case
        print("\nclient.map fans one function over many inputs (no manual loop):")
        mapped = client.gather(client.map(square, range(10)))
        print(f"  squares of 0..9 -> {mapped}")

        # SECTION: summary
        print("\n=== Summary ===")
        print("- submit() returns a Future immediately; the work happens on a worker")
        print("- gather() collects results, blocking until they are ready")
        print("- concurrency is bounded by total threads, not by worker count")
        print("- map() is submit() over an iterable, and the usual way to fan out")
        print("- which worker runs what decides how much data has to move next")


if __name__ == "__main__":
    main()
