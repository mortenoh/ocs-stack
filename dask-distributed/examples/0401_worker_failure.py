"""Worker failure: killing a container mid-flight and surviving it.

What: kills a worker process outright while the cluster holds data, shows the
nanny replacing it, and confirms a computation still returns the right answer
because the scheduler recomputed what was lost.

Why: workers die. They run out of memory, their host is preempted, someone
redeploys. A LocalCluster gives you no way to rehearse this, so the first time
you see it is in production. dask's answer is that the graph is the source of
truth: anything lost can be rebuilt from it, as long as the inputs are still
reachable.

Run: make run EXAMPLE=0401_worker_failure
"""

import os
import time
from typing import Any

import dask.array as da

from climate_stack_dask_distributed import connect

# The nanny needs a moment to notice the death and start a replacement.
RECOVERY_TIMEOUT = 45.0


def kill_this_worker() -> None:
    """Terminate the worker process immediately, with no cleanup.

    ``os._exit`` skips atexit handlers and shutdown hooks, which is exactly
    what an OOM kill or a yanked machine looks like from the outside.
    """
    os._exit(1)


def worker_addresses(client: Any) -> set[str]:
    """Return the set of worker addresses the scheduler currently knows.

    Args:
        client: A connected client.

    Returns:
        The worker addresses, which change when a worker is replaced.
    """
    return set(client.scheduler_info().get("workers", {}))


def wait_for_worker_count(client: Any, target: int, timeout: float) -> tuple[int, float]:
    """Poll until the cluster has the expected number of workers.

    Args:
        client: A connected client.
        target: The worker count to wait for.
        timeout: Seconds to keep polling before giving up.

    Returns:
        The final worker count and how long the wait took.
    """
    started = time.perf_counter()
    deadline = started + timeout
    count = len(worker_addresses(client))
    while count != target and time.perf_counter() < deadline:
        time.sleep(1.0)
        count = len(worker_addresses(client))
    return count, time.perf_counter() - started


def main() -> None:
    """Kill a worker and show the cluster recovering around it."""
    with connect() as session:
        client = session.client
        print(session.banner())

        if not session.is_compose:
            print("\nThis example needs real worker processes to kill.")
            print("The fallback runs workers as threads inside THIS process, so killing")
            print("one would take the example down with it. Skipping the destructive part.")
            print("\nStart the container cluster to see it properly:  make up")
            print("\n=== Summary ===")
            print("- worker death is normal: OOM kills, preemption, redeploys")
            print("- a nanny process supervises each worker and restarts it")
            print("- the task graph lets the scheduler recompute anything that was lost")
            print("- results stay correct; you pay in latency, not in wrong answers")
            return

        # SECTION: the healthy cluster
        before = worker_addresses(client)
        print(f"\nStarting with {len(before)} workers:")
        for address in sorted(before):
            print(f"  {address}")

        # SECTION: put data on the cluster
        array = da.random.random((6000, 6000), chunks=(1500, 1500))
        persisted = array.persist()
        client.gather(client.futures_of(persisted))
        expected = float(persisted.sum().compute())
        print(f"\nPersisted {array.nbytes / 1e6:.0f} MB in {array.npartitions} chunks.")
        print(f"  reference answer, computed while healthy: {expected:,.2f}")

        held = {addr: len(keys) for addr, keys in client.has_what().items()}
        print("  chunks per worker: " + ", ".join(f"{a.rsplit('/', 1)[-1]}={n}" for a, n in sorted(held.items())))

        # SECTION: kill one
        victim = sorted(before)[0]
        victim_chunks = held.get(victim, 0)
        print(f"\nKilling {victim} with os._exit -- it holds {victim_chunks} chunks.")
        print("Those chunks exist nowhere else. They are simply gone.")
        try:
            client.run(kill_this_worker, workers=[victim])
        except Exception as exc:
            # The worker dies before it can reply, so the call itself fails.
            # That is the expected outcome, not an error in this example.
            print(f"  the RPC died with the worker: {type(exc).__name__} (expected)")

        # SECTION: recovery
        count, waited = wait_for_worker_count(client, len(before), RECOVERY_TIMEOUT)
        after = worker_addresses(client)
        replaced = after - before
        gone = before - after
        print(f"\nAfter {waited:.1f}s the scheduler reports {count} workers.")
        if gone:
            print(f"  gone:        {', '.join(sorted(gone))}")
        if replaced:
            print(f"  replacement: {', '.join(sorted(replaced))}")
            print("  A new address means a NEW process: the nanny supervising that container")
            print("  noticed the exit and started a fresh worker. The container never restarted.")
        elif count == len(before):
            print("  The cluster is back to full strength.")

        # SECTION: correctness survives
        print("\nRecomputing the same sum on the healed cluster:")
        started = time.perf_counter()
        recovered = float(persisted.sum().compute())
        elapsed = time.perf_counter() - started
        print(f"  {recovered:,.2f} in {elapsed:.2f}s")
        print(f"  matches the reference: {abs(recovered - expected) < 1e-6}")
        print("  The lost chunks were rebuilt from the graph. Same answer, extra work.")

        # SECTION: the limits of this
        print("\nWhat this does NOT save you from:")
        print("  - scattered data: client.scatter puts data on workers with no recipe to")
        print("    rebuild it, so losing that worker loses the data for good")
        print("  - the client dying: the graph lives in the client process")
        print("  - a task that kills every worker it touches (a poison pill), which dask")
        print("    eventually gives up on rather than retrying forever")

        del persisted

        # SECTION: summary
        print("\n=== Summary ===")
        print("- each worker is supervised by a nanny that restarts it after a crash")
        print("- a new worker address is the tell that a process was replaced")
        print("- the task graph is the source of truth; lost results get recomputed")
        print("- correctness survives worker loss, latency does not")
        print("- scattered data has no recipe and cannot be recovered this way")


if __name__ == "__main__":
    main()
