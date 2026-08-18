"""Schedulers: the same graph on threads, processes, and synchronous.

What: builds two graphs -- a numpy-heavy array sum that releases the GIL and a
pure-Python delayed loop that holds it -- then computes each one with
scheduler="synchronous", "threads", and "processes", timing every run to show
why threads win the first workload and lose the second.

Why: open-climate-service executes openEO process graphs on dask, so a slow
pipeline is often a scheduler mismatch, not a graph problem. Knowing how each
scheduler behaves (GIL, serialization, process startup) is how you debug it.

Run: make run EXAMPLE=0301_schedulers
"""

import os
import time
from typing import Any

# dask's __init__ does not re-export these in a stub-visible way; import from the defining modules.
from dask.base import compute
from dask.delayed import delayed

from ocs_stack_dask import chunk_report, random_field, task_count

SCHEDULERS = ("synchronous", "threads", "processes")
# One task per core keeps every worker busy for a full task, so the process
# pool's fixed startup cost is amortized instead of dominating the measurement.
N_LOOP_TASKS = 12
LOOP_SIZE = 30_000_000


def py_loop(n: int) -> int:
    """Sum range(n) with a pure-Python loop that holds the GIL throughout.

    Args:
        n: Upper bound of the range to sum.

    Returns:
        The sum of 0..n-1.
    """
    total = 0
    for i in range(n):
        total += i
    return total


def timed_compute(graph: Any, scheduler: str) -> tuple[float, float]:
    """Compute one dask collection on a named scheduler and time the run.

    Args:
        graph: Any dask collection whose result converts to float.
        scheduler: One of "synchronous", "threads", or "processes".

    Returns:
        A (result, wall_seconds) tuple.
    """
    start = time.perf_counter()
    (result,) = compute(graph, scheduler=scheduler)
    return float(result), time.perf_counter() - start


def run_all_schedulers(label: str, graph: Any) -> dict[str, float]:
    """Run one graph on every scheduler, printing timings and ratios vs sync.

    Args:
        label: Short name of the workload for the printed table.
        graph: The dask collection to compute on each scheduler.

    Returns:
        Mapping of scheduler name to wall seconds.
    """
    print(f"\n{label}: same graph, three executors")
    times: dict[str, float] = {}
    for scheduler in SCHEDULERS:
        result, seconds = timed_compute(graph, scheduler)
        times[scheduler] = seconds
        ratio = seconds / times["synchronous"]
        print(f"  scheduler={scheduler:<12} {seconds:6.2f} s  ({ratio:4.2f}x of sync)  result={result:.4g}")
    return times


def main() -> None:
    """Time a GIL-releasing and a GIL-holding graph on all three schedulers."""
    # SECTION: two graphs, one question
    print(f"One task graph can run on different executors; this machine has {os.cpu_count()} cores.")
    print("Workload A is numpy-heavy (random generation + sum: C code that releases the GIL).")
    print("Workload B is pure Python (a delayed for-loop: bytecode that holds the GIL).")

    field = random_field(days=365, ny=512, nx=512, time_chunk=30, spatial_chunk=256)
    numpy_graph = field.sum()
    print(f"\n  A: {chunk_report(field)}")
    print(f"     graph for sum(): {task_count(numpy_graph)} tasks")

    parts = [delayed(py_loop)(LOOP_SIZE + i) for i in range(N_LOOP_TASKS)]
    python_graph = delayed(sum)(parts)
    print(f"  B: {N_LOOP_TASKS} delayed py_loop({LOOP_SIZE:,}) tasks + 1 combining sum")
    print(f"     graph: {task_count(python_graph)} tasks")

    # SECTION: numpy-heavy workload -- threads should win
    print("\n--- Workload A: numpy-heavy (GIL released) ---")
    print("Each chunk task runs numpy C code, so many threads compute at once with zero-copy shared memory.")
    numpy_times = run_all_schedulers("A", numpy_graph)

    # SECTION: pure-Python workload -- threads cannot help
    print("\n--- Workload B: pure Python (GIL held) ---")
    print("Only one thread can run Python bytecode at a time, so threads degenerate to sequential execution.")
    python_times = run_all_schedulers("B", python_graph)

    # SECTION: reading the scorecard
    print("\n--- Scorecard (wall time relative to synchronous) ---")
    header = f"  {'workload':<22}" + "".join(f"{s:>14}" for s in SCHEDULERS)
    print(header)
    for name, times in (("A numpy-heavy", numpy_times), ("B pure-Python", python_times)):
        cells = "".join(f"{times[s] / times['synchronous']:>13.2f}x" for s in SCHEDULERS)
        print(f"  {name:<22}{cells}")
    print("\nnote: 'processes' pays a one-time cost here to spawn worker processes and")
    print("pickle tasks/results; on graphs this small that startup is a visible share")
    print("of its wall time, so its ratios above understate its win on longer jobs.")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- The graph is fixed; scheduler= only changes the executor")
    print("- synchronous: one thread, no parallelism -- the debugging baseline")
    print("- threads: wins GIL-releasing work (numpy/pandas C code), shared memory, no pickling")
    print("- threads: loses pure-Python work -- the GIL serializes bytecode")
    print("- processes: sidesteps the GIL but pays spawn + serialization overhead")
    print("- rule of thumb: threads for numeric arrays, processes (or distributed) for Python-level code")


if __name__ == "__main__":
    main()
