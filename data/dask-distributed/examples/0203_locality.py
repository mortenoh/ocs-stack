"""Data locality: the scheduler moves compute to data, not data to compute.

What: persists chunks on the cluster, inspects which worker holds what with
who_has/has_what, and shows that follow-up work is scheduled where its input
already lives -- then forces a bad placement to show what that costs.

Why: this is the scheduler's central optimization, and understanding it
explains most "why is my cluster slow" questions. A task pinned to the wrong
worker turns a free local read into a network transfer.

Run: make run EXAMPLE=0203_locality
"""

import time
from collections import Counter
from typing import Any

import dask.array as da

from playground_data_dask_distributed import connect, describe_workers


def chunk_sum(array: Any) -> float:
    """Sum whatever array-like block the scheduler hands over.

    Args:
        array: A numpy block held by a worker.

    Returns:
        The sum of the block.
    """
    return float(array.sum())


def main() -> None:
    """Show where data lives and where the work that needs it runs."""
    with connect() as session:
        client = session.client
        workers = describe_workers(client)
        print(session.banner())

        # SECTION: put data on the cluster
        print("\npersist() computes chunks and LEAVES them in worker memory.")
        array = da.random.random((4000, 4000), chunks=(1000, 1000))
        persisted = array.persist()
        # Block until every chunk is really resident before asking who has what.
        client.gather(client.futures_of(persisted))
        print(f"  a {array.nbytes / 1e6:.0f} MB array in {persisted.npartitions} chunks is now resident")

        # SECTION: who holds what
        has_what = client.has_what()
        print("\nclient.has_what() -- keys held per worker:")
        for address, keys in sorted(has_what.items()):
            host = address.rsplit("/", 1)[-1]
            print(f"  {host:<24} {len(keys):>2} chunks")

        who_has = client.who_has(persisted)
        holders = Counter(addr for addrs in who_has.values() for addr in addrs)
        print("\nclient.who_has() maps each chunk key to the worker(s) holding it:")
        print(f"  {len(who_has)} chunks spread over {len(holders)} worker(s)")

        # SECTION: follow-up work goes to the data
        print("\nNow submit work on those chunks and see where it runs.")
        started = time.perf_counter()
        total = float(persisted.sum().compute())
        natural = time.perf_counter() - started
        print(f"  sum of the persisted array = {total:,.0f} in {natural * 1000:.0f} ms")
        print("  Each partial sum ran on the worker already holding its chunk:")
        print("  no chunk had to move, so the only traffic was the tiny partial results.")

        # SECTION: forcing a bad placement
        if session.is_compose and len(workers) > 1:
            print("\nForcing the opposite: pin every task to ONE worker with workers=.")
            victim = workers[0]["address"]
            first_keys = list(who_has)[:8]
            started = time.perf_counter()
            pinned = [client.submit(chunk_sum, client.futures_of(persisted)[i], workers=[victim]) for i in range(8)]
            client.gather(pinned)
            forced = time.perf_counter() - started
            print(f"  8 chunks summed on {victim.rsplit('/', 1)[-1]}: {forced * 1000:.0f} ms")
            print(f"  ({len(first_keys)} chunks, most of which had to be shipped to that worker first)")
            print("  Same arithmetic, extra network. Pinning is a tool for correctness")
            print("  (a worker with a GPU, a licence, a mounted disk), not for speed.")
        else:
            print("\nThe fallback shares one memory space, so locality is meaningless there.")
            print("Run 'make up' to see chunks actually pinned to separate containers.")

        # SECTION: releasing
        del persisted
        print("\nDropping the reference lets the scheduler free those chunks;")
        print("worker memory is a resource you manage, not one that manages itself.")

        # SECTION: summary
        print("\n=== Summary ===")
        print("- persist() leaves computed chunks in worker memory across computations")
        print("- has_what()/who_has() answer 'where does this data actually live'")
        print("- the scheduler prefers running a task where its input already is")
        print("- workers= overrides that, and usually makes things slower")
        print("- moving compute to data is the whole reason a distributed scheduler exists")


if __name__ == "__main__":
    main()
