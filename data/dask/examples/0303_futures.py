"""Futures: eager task submission with client.submit, map, and gather.

What: uses client.submit to fire tasks that run in the background while the
main thread does other work, reads them back with future.result() and
client.gather, fans out with client.map, and contrasts this eager imperative
style with lazy declarative collections like delayed.

Why: open-climate-service executes openEO process graphs on dask, and its
service layer needs the futures style: submit work now, keep serving requests,
collect results later. Knowing eager vs lazy is knowing which API to reach for.

Run: make run EXAMPLE=0303_futures
"""

import time
from typing import Any, cast

from dask.delayed import delayed
from distributed import Client, LocalCluster

TASK_DELAY = 0.3


def start_cluster() -> LocalCluster:
    """Start a small in-process LocalCluster, falling back to no dashboard.

    Returns:
        A running LocalCluster with 2 single-threaded in-process workers
        (the same settings as example 0302).
    """
    settings: dict[str, Any] = {"processes": False, "n_workers": 2, "threads_per_worker": 1}
    try:
        return LocalCluster(dashboard_address="127.0.0.1:0", **settings)
    except Exception:
        print("note: could not bind a dashboard port; continuing with dashboard_address=None")
        no_dashboard: Any = None  # runtime accepts None to disable the dashboard; the stub says str only
        return LocalCluster(dashboard_address=no_dashboard, **settings)


def slow_square(x: int, delay: float = TASK_DELAY) -> int:
    """Square a number after a sleep that stands in for real work.

    Args:
        x: The number to square.
        delay: Seconds to sleep before returning.

    Returns:
        x squared.
    """
    time.sleep(delay)
    return x * x


def main() -> None:
    """Submit eager futures, gather them, and contrast with lazy collections."""
    with start_cluster() as entered, Client(entered) as client:
        cluster = cast(LocalCluster, entered)  # sync __enter__ returns the cluster; the stub offers a coroutine too
        n_workers = len(cluster.workers)
        print(f"Connected to a LocalCluster with {n_workers} workers, 1 thread each (settings from 0302).")

        # SECTION: submit is eager -- tasks run while you do other things
        print("\nclient.submit ships the call to a worker IMMEDIATELY and returns a Future:")
        start = time.perf_counter()
        fut_a = client.submit(slow_square, 6)
        fut_b = client.submit(slow_square, 7)
        print(f"  right after submit: fut_a.status={fut_a.status!r}, fut_b.status={fut_b.status!r}")
        print("  ...main thread now does other work while both tasks run on the workers...")
        other_work = sum(i * i for i in range(200_000))
        a, b = fut_a.result(), fut_b.result()
        elapsed = time.perf_counter() - start
        print(f"  other work done (checksum {other_work}), then result() blocked only for the remainder")
        print(f"  after result(): fut_a.status={fut_a.status!r}, results a={a}, b={b}")
        print(f"  wall time {elapsed:.2f} s for two {TASK_DELAY} s tasks -> they overlapped on {n_workers} workers")

        # SECTION: client.map + gather for fan-out
        print("\nclient.map submits one task per input; client.gather collects them in one round trip:")
        inputs = list(range(8))
        start = time.perf_counter()
        futures = client.map(slow_square, inputs)
        results = cast(list[int], client.gather(futures))  # the stub's return union is wrong for list input
        elapsed = time.perf_counter() - start
        serial = TASK_DELAY * len(inputs)
        print(f"  inputs:  {inputs}")
        print(f"  results: {results}")
        print(
            f"  wall time {elapsed:.2f} s vs {serial:.1f} s serial -> ~{serial / elapsed:.1f}x with {n_workers} workers"
        )

        # SECTION: eager futures vs lazy collections
        print("\nContrast: futures are eager and imperative; collections are lazy and declarative.")
        eager = client.submit(slow_square, 9)
        lazy = delayed(slow_square)(9)
        time.sleep(TASK_DELAY + 0.3)
        print(f"  future after {TASK_DELAY + 0.3:.1f} s of doing nothing: status={eager.status!r} -- it ran anyway")
        print(f"  delayed after the same wait: {lazy!r} -- still just a graph, nothing ran")
        print(f"  the delayed only executes on request: compute() = {lazy.compute()}")
        print("  futures: 'run THIS now, I will decide what next based on results' (imperative)")
        print("  collections: 'here is the WHOLE recipe, optimize and run it once' (declarative)")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- client.submit fires a task immediately and returns a Future handle")
    print("- futures run in the background; result() blocks only for what is left")
    print("- client.map fans one function over many inputs; gather collects all results")
    print("- futures = eager + imperative; delayed/array/dataframe = lazy + declarative")
    print("- use collections for fixed pipelines, futures for dynamic, decide-as-you-go work")
    print("- context managers closed the client and cluster, so the process exits cleanly")


if __name__ == "__main__":
    main()
