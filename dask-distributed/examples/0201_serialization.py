"""Serialization: everything you send to a worker crosses a wire.

What: submits the same computation three ways -- a tiny argument, a large
array shipped as an argument, and the array built on the worker instead --
and times each so the cost of moving bytes is visible.

Why: on a LocalCluster arguments are passed by reference within one process,
so this cost is exactly zero and the lesson is invisible. On a real cluster
every argument is pickled, pushed through a socket, and unpickled. The single
biggest performance mistake in distributed code is shipping data that the
worker could have produced or loaded itself -- which is why
open-climate-service has workers read zarr from shared storage rather than
receiving arrays from the API process.

Run: make run EXAMPLE=0201_serialization
"""

import logging
import pickle
import time

import numpy as np

from playground_dask_distributed import connect

# 8 MB per array: big enough that the transfer dominates, small enough that
# three workers with 1.5 GiB each never come close to their limit.
ROWS = 1000
COLS = 1000


def sum_array(array: np.ndarray) -> float:
    """Sum an array that was shipped from the client as an argument.

    Args:
        array: The array to reduce, serialized on the client side.

    Returns:
        The sum of every element.
    """
    return float(array.sum())


def build_and_sum(rows: int, cols: int, seed: int) -> float:
    """Build an array on the worker, then sum it.

    Only three integers cross the wire; the megabytes never leave the worker.

    Args:
        rows: Row count of the array to build.
        cols: Column count of the array to build.
        seed: Seed so the result matches the client-side array exactly.

    Returns:
        The sum of every element.
    """
    rng = np.random.default_rng(seed)
    return float(rng.random((rows, cols)).sum())


def add_one(value: int) -> int:
    """Add one to a value, as the cheapest possible payload.

    Args:
        value: The number to increment.

    Returns:
        value + 1.
    """
    return value + 1


def main() -> None:
    """Compare the cost of sending data versus making it on the worker."""
    with connect() as session:
        client = session.client
        print(session.banner())

        rng = np.random.default_rng(0)
        array = rng.random((ROWS, COLS))
        payload_mb = array.nbytes / 1e6
        pickled_mb = len(pickle.dumps(array)) / 1e6

        # SECTION: what a payload actually weighs
        print(f"\nThe array is {ROWS}x{COLS} float64 = {payload_mb:.1f} MB in memory,")
        print(f"and {pickled_mb:.1f} MB once pickled -- that is what travels per task.")

        # SECTION: the cheapest possible task
        started = time.perf_counter()
        client.submit(add_one, 1).result()
        tiny = time.perf_counter() - started
        print(f"\nA task with an int argument round-trips in {tiny * 1000:.1f} ms.")
        print("That is the floor: scheduling overhead with nothing to carry.")

        # SECTION: shipping the array as an argument
        started = time.perf_counter()
        shipped_total = client.submit(sum_array, array).result()
        shipped = time.perf_counter() - started
        print(f"\nSending the {payload_mb:.1f} MB array as an argument: {shipped * 1000:.0f} ms")

        # SECTION: building it on the worker instead
        started = time.perf_counter()
        built_total = client.submit(build_and_sum, ROWS, COLS, 0).result()
        built = time.perf_counter() - started
        print(f"Building the same array on the worker:      {built * 1000:.0f} ms")
        print(f"  identical result: {abs(shipped_total - built_total) < 1e-6} ({shipped_total:.4f})")

        if session.is_compose:
            print(f"\n  Shipping cost about {shipped / built:.1f}x the worker-side build.")
            print("  The work is the same; the difference is bytes on a socket.")
        else:
            print("\n  In the fallback both numbers are similar: one process, no socket,")
            print("  arguments passed by reference. Run 'make up' to see the real gap.")

        # SECTION: what cannot be sent at all
        print("\nNot everything can cross the wire. dask uses cloudpickle, which handles")
        print("lambdas and locally-defined functions, but OS resources cannot be pickled:")
        # distributed logs the full pickle traceback at ERROR before raising.
        # That is the right default in production and pure noise in a lesson,
        # so quiet just that logger while we provoke the failure on purpose.
        protocol_logger = logging.getLogger("distributed.protocol")
        previous_level = protocol_logger.level
        protocol_logger.setLevel(logging.CRITICAL)
        try:
            with open(__file__, "rb") as handle:
                client.submit(sum_array, handle).result()
        except Exception as exc:
            name = type(exc).__name__
            detail = str(exc).splitlines()[0][:70] if str(exc) else "(no message)"
            print(f"  sending an open file handle -> {name}")
            print(f"    {detail}...")
            print("    the root cause: cannot pickle 'BufferedReader' instances")
        finally:
            protocol_logger.setLevel(previous_level)
        print("  Send the path instead and let the worker open it -- if it can see it.")

        # SECTION: summary
        print("\n=== Summary ===")
        print("- every argument to a submitted task is pickled and pushed over a socket")
        print("- prefer sending parameters (a path, a size, a seed) over sending data")
        print("- move compute to the data, not the data to the compute")
        print("- file handles, sockets, and locks cannot be serialized; paths can")
        print("- a LocalCluster hides all of this, which is why it flatters bad code")


if __name__ == "__main__":
    main()
