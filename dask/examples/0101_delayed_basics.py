"""Delayed basics: wrap plain functions, build lazily, execute on compute.

What: wraps ordinary Python functions with dask.delayed and shows that calling
the wrapped function builds a Delayed object without running anything -- proven
with a print side effect and perf_counter timings. Then .compute() executes the
graph, delayed values compose into an inc/add pipeline, and dask.compute(a, b)
evaluates several outputs in one pass.

Why: this is the core dask mental model -- describe work now, execute it later.
open-climate-service runs its openEO process graphs on dask: a process graph is
assembled up front and handed to a scheduler, which is exactly what Delayed does
in miniature. Understand this file and the rest of dask is front-ends.

Run: make run EXAMPLE=0101_delayed_basics
"""

import time
from typing import Any

from dask.base import compute
from dask.delayed import Delayed, delayed

from ocs_stack_dask import task_count

WORK_SECONDS = 0.3
"""Deterministic stand-in cost for a 'real' computation, so timings are reliable."""


def slow_square(x: int) -> int:
    """Square x after a fixed delay, announcing loudly when it actually runs.

    Args:
        x: The value to square.

    Returns:
        x squared.
    """
    print(f"  [side effect] slow_square({x}) is RUNNING now")
    time.sleep(WORK_SECONDS)
    return x * x


def inc(x: int) -> int:
    """Return x + 1.

    Args:
        x: The value to increment.

    Returns:
        x plus one.
    """
    return x + 1


def add(x: int, y: int) -> int:
    """Return x + y.

    Args:
        x: First addend.
        y: Second addend.

    Returns:
        The sum of x and y.
    """
    return x + y


def main() -> None:
    """Wrap functions with dask.delayed, show laziness, then execute the graphs."""
    # SECTION: eager python runs immediately
    print("Plain Python is eager: calling slow_square(3) runs it on the spot.")
    t0 = time.perf_counter()
    eager = slow_square(3)
    eager_s = time.perf_counter() - t0
    print(f"  result={eager}, and the call blocked for the full {WORK_SECONDS} s of work")

    # SECTION: delayed builds instead of running
    print("\ndelayed(slow_square)(4) builds a task instead of running one:")
    t0 = time.perf_counter()
    lazy: Delayed = delayed(slow_square)(4)
    build_s = time.perf_counter() - t0
    print(f"  type={type(lazy).__name__}, repr={lazy!r}")
    print("  no '[side effect]' line printed above -- the function body never ran")
    print(f"  building was ~{eager_s / max(build_s, 1e-9):.0f}x faster than the eager call (it does no work)")

    # SECTION: compute executes the graph
    print("\n.compute() hands the graph to a scheduler, which finally runs it:")
    t0 = time.perf_counter()
    value = lazy.compute()
    compute_s = time.perf_counter() - t0
    print(f"  result={value}")
    print(f"  compute took ~{compute_s / max(build_s, 1e-9):.0f}x longer than building -- the work happens here")

    # SECTION: delayed values compose
    print("\nDelayed values compose: pass them to other delayed calls to grow a graph.")
    a: Delayed = delayed(inc)(1)  # inc(1)        -> 2
    b: Delayed = delayed(inc)(2)  # inc(2)        -> 3
    total: Delayed = delayed(add)(a, b)  # add(a, b) -> 5
    print("  a = delayed(inc)(1); b = delayed(inc)(2); total = delayed(add)(a, b)")
    print(f"  task_count(a)={task_count(a)}, task_count(total)={task_count(total)} (total contains a and b)")
    print(f"  total.compute() = {total.compute()}")

    # SECTION: many outputs, one compute
    print("\ndask.compute(a, b, ...) evaluates several outputs in a single pass:")
    doubled: Delayed = delayed(add)(total, total)
    results: tuple[Any, ...] = compute(a, b, total, doubled)
    print(f"  dask.compute(a, b, total, doubled) = {results}")
    print("  one call, one merged graph -- shared work is not repeated (see 0103)")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- delayed(fn) wraps a plain function; calling it builds a Delayed, runs nothing")
    print("- proof of laziness: no side effect, and building is orders of magnitude faster than running")
    print("- .compute() executes the recorded graph and returns a concrete value")
    print("- Delayed objects compose: passing them into other delayed calls chains tasks")
    print("- dask.compute(a, b) computes multiple outputs from one merged graph")


if __name__ == "__main__":
    main()
