"""Scatter and gather: publish data once instead of once per task.

What: runs the same batch of tasks twice -- first passing a large array
directly to every task, then scattering it once and passing the resulting
future -- and compares the time and the bytes moved.

Why: passing a big argument to N tasks serializes and ships it N times. This
is the classic distributed performance bug, and it hides in innocent-looking
code like ``[client.submit(f, big_df, i) for i in range(100)]``. scatter()
puts the data on the workers once and hands you a handle to reuse.

Run: make run EXAMPLE=0202_scatter_gather
"""

import time

import numpy as np

from climate_stack_dask_distributed import connect

N_TASKS = 12
ROWS = 800
COLS = 800


def column_mean(array: np.ndarray, index: int) -> float:
    """Return the mean of one column of a shared array.

    The array argument is the same object for every task, which is exactly the
    situation scatter() exists for.

    Args:
        array: The shared array.
        index: Which column to average.

    Returns:
        The mean of the selected column.
    """
    return float(array[:, index % array.shape[1]].mean())


def main() -> None:
    """Compare per-task shipping against a single scatter."""
    with connect() as session:
        client = session.client
        print(session.banner())

        rng = np.random.default_rng(0)
        array = rng.random((ROWS, COLS))
        payload_mb = array.nbytes / 1e6

        print(f"\nA {payload_mb:.1f} MB array, needed by {N_TASKS} separate tasks.")

        # SECTION: the naive version
        print("\nVersion 1 -- pass the array to every task:")
        started = time.perf_counter()
        naive_futures = [client.submit(column_mean, array, i) for i in range(N_TASKS)]
        naive_results = client.gather(naive_futures)
        naive = time.perf_counter() - started
        print(f"  {naive * 1000:.0f} ms; up to {payload_mb * N_TASKS:.0f} MB of serialization work")
        print("  dask deduplicates identical arguments to a degree, but it still hashes")
        print("  and tracks the payload once per submit call.")

        # SECTION: scatter once
        print("\nVersion 2 -- scatter the array once, then pass the future:")
        started = time.perf_counter()
        remote = client.scatter(array, broadcast=True)
        scatter_time = time.perf_counter() - started
        scattered_futures = [client.submit(column_mean, remote, i) for i in range(N_TASKS)]
        scattered_results = client.gather(scattered_futures)
        scattered = time.perf_counter() - started
        print(f"  scatter itself: {scatter_time * 1000:.0f} ms ({payload_mb:.1f} MB sent once)")
        print(f"  total:          {scattered * 1000:.0f} ms")

        # SECTION: the comparison
        same = all(abs(a - b) < 1e-12 for a, b in zip(naive_results, scattered_results, strict=True))
        print(f"\n  identical results: {same}")
        if session.is_compose:
            faster = naive / scattered if scattered else float("inf")
            print(f"  scatter version was {faster:.1f}x the speed of the naive one")
        else:
            print("  In the fallback there is no wire, so both versions cost about the same.")
            print("  Run 'make up' to see the difference the network makes.")

        # SECTION: the handle is a Future
        print(f"\nscatter returns a Future: {type(remote).__name__}, status={remote.status}")
        print("Passing it to submit() tells the scheduler 'the data is already out there'.")
        print("broadcast=True copies to every worker; without it the data lands on one")
        print("and gets moved on demand -- better when only some tasks need it.")

        # SECTION: when not to scatter
        print("\nWhen NOT to scatter:")
        print("  - small data: the round trip costs more than just sending it")
        print("  - data used by exactly one task: there is nothing to amortize")
        print("  - data a worker could load itself from shared storage: skip the client entirely")

        # SECTION: summary
        print("\n=== Summary ===")
        print("- passing a big object to N tasks pays the serialization cost repeatedly")
        print("- client.scatter publishes it once and returns a Future to reuse")
        print("- broadcast=True puts a copy on every worker up front")
        print("- gather() is the inverse: it pulls results back to the client")
        print("- scatter small data and you make things slower, not faster")


if __name__ == "__main__":
    main()
