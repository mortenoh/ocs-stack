"""Blocked algorithms: how a reduction decomposes into per-chunk tasks.

What: counts the tasks dask adds when reducing a chunked (time, y, x) field --
full sum, sums over different axes, and a sum over a slice -- showing the
per-chunk partial + tree-combine structure and how optimization culls the
graph down to only the chunks a slice actually needs.

Why: open-climate-service aggregates chunked zarr stores (means over time,
means over regions) without ever loading the whole array. That works because
dask rewrites "sum this 191 MB array" into "sum each 15 MB chunk, then
combine the 52 small partials" -- and because slicing before reducing lets
the optimizer drop every chunk outside the slice.

Run: make run EXAMPLE=0202_blocked_algorithms
"""

from typing import Any

import numpy as np
from dask.base import optimize

from climate_stack_dask import chunk_report, random_field, task_count


def optimized_task_count(obj: Any) -> int:
    """Return the task count of a collection after dask's graph optimization (culling).

    Args:
        obj: Any dask collection.

    Returns:
        The number of tasks left once unreachable tasks are culled.
    """
    (optimized,) = optimize(obj)
    return task_count(optimized)


def main() -> None:
    """Decompose reductions into per-chunk partials and count the resulting tasks."""
    # SECTION: the starting graph
    print("Baseline: creating the chunked field is already a graph -- one task per chunk.")
    arr = random_field()  # (365, 256, 256), chunks (30, 128, 128)
    print(f"  {chunk_report(arr)}")
    base_tasks = task_count(arr)
    print(f"  task_count(arr) = {base_tasks}  (52 chunks -> 52 creation tasks)")

    # SECTION: a full reduction decomposes per chunk
    print("\narr.sum() does not sum 24.5M elements in one task; it sums each chunk, then combines:")
    total = arr.sum()
    print(f"  task_count(arr.sum()) = {task_count(total)}  (added {task_count(total) - base_tasks} tasks)")
    print("  added tasks = 52 per-chunk partial sums + a tree of combine steps + 1 final aggregate")
    print("  the tree matters: combining the 52 partials a few at a time keeps every task small and parallel")
    npa = arr.compute()
    print(f"  result matches numpy: np.allclose(arr.sum(), npa.sum()) = {np.allclose(total.compute(), npa.sum())}")

    # SECTION: the reduced axes shape the combine tree
    print("\nSumming over different axes builds different combine trees over the same 52 partials:")
    over_time = arr.sum(axis=0)  # (256, 256) map: combine 13 time-blocks per spatial column
    over_space = arr.sum(axis=(1, 2))  # (365,) series: combine 4 spatial blocks per time-row
    print(f"  arr.sum()           -> shape {total.shape},  tasks={task_count(total)}")
    print(f"  arr.sum(axis=0)     -> shape {over_time.shape},  tasks={task_count(over_time)}")
    print(f"  arr.sum(axis=(1,2)) -> shape {over_space.shape},  tasks={task_count(over_space)}")
    print("  every variant starts with the same per-chunk partials; only the combine pattern differs")
    print(f"  output stays chunked too: sum(axis=0) has {over_time.npartitions} blocks, one per spatial column")

    # SECTION: slicing selects only the chunks it needs
    print("\nSlicing before reducing lets the optimizer cull chunks that are never read:")
    day0 = arr[0].sum()  # day 0 touches only the 4 spatial blocks of the first time-chunk
    print("  raw graphs still reference every creation task (nothing is culled until optimize/compute):")
    print(f"    task_count(arr.sum())    = {task_count(total)}")
    print(f"    task_count(arr[0].sum()) = {task_count(day0)}")
    print("  after dask.optimize (what compute() runs):")
    print(f"    optimized arr.sum()    = {optimized_task_count(total)} tasks  (all 52 chunks needed)")
    print(f"    optimized arr[0].sum() = {optimized_task_count(day0)} tasks  (4 chunks + slice + combine)")
    print(f"  value check: np.allclose(arr[0].sum(), npa[0].sum()) = {np.allclose(day0.compute(), npa[0].sum())}")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- A reduction = one partial task per chunk + a tree of combines + a final aggregate")
    print("- The reduced axes decide the combine tree; the per-chunk partials are always the same")
    print("- Raw graphs keep every task; optimize/compute culls to what the output actually needs")
    print("- Slice first, reduce second: arr[0].sum() executes ~9 tasks, arr.sum() executes ~117")
    print("- This is why OCS can aggregate huge zarr stores while reading only the relevant chunks")


if __name__ == "__main__":
    main()
