"""Chunk sizing: the trade-off between too many tiny chunks and one giant one.

What: runs the same reduction over a (365, 256, 256) field three times — tiny
chunks, sensible chunks, one giant chunk — and prints task counts and wall
times so the overhead is measured, not asserted.

Why: these are the production patterns open-climate-service encodes. Every
chunk costs a task, and every task costs the scheduler a fixed overhead, so
thousands of tiny chunks turn a fast reduction into pure bookkeeping; one
giant chunk removes the overhead but also all parallelism. The sweet spot is
a modest number of chunks around ~100 MB each.

Run: make run EXAMPLE=0602_chunk_sizing
"""

import time

import dask.array as da

from ocs_stack_dask.helpers import chunk_report, random_field, task_count


def timed_compute(expr: da.Array) -> float:
    """Compute a dask expression and return the elapsed wall time in seconds.

    Args:
        expr: Any lazy dask array expression.

    Returns:
        Wall-clock seconds spent in ``compute()``.
    """
    t0 = time.perf_counter()
    expr.compute()
    return time.perf_counter() - t0


def main() -> None:
    """Measure the same computation under tiny, sensible, and giant chunk layouts."""
    # SECTION: three layouts of the same array
    print("Same data, same computation -- (2*x + 1).mean() over (365, 256, 256), ~191 MB.")
    print("Only the chunk layout changes:\n")
    layouts = [
        ("tiny    (5, 32, 32)", 5, 32),
        ("sensible (90, 128, 128)", 90, 128),
        ("giant   (365, 256, 256)", 365, 256),
    ]
    results: dict[str, tuple[int, float]] = {}
    for label, time_chunk, spatial_chunk in layouts:
        arr = random_field(days=365, ny=256, nx=256, time_chunk=time_chunk, spatial_chunk=spatial_chunk, seed=0)
        expr = (2.0 * arr + 1.0).mean()
        tasks = task_count(expr)
        seconds = timed_compute(expr)
        results[label] = (tasks, seconds)
        print(f"  {label}")
        print(f"    {chunk_report(arr)}")
        print(f"    tasks={tasks}, wall={seconds:.2f}s")

    # SECTION: the ratios are the lesson
    print("\nRatios against the sensible layout:")
    sensible_tasks, sensible_seconds = results["sensible (90, 128, 128)"]
    for label, (tasks, seconds) in results.items():
        print(f"  {label}: {tasks / sensible_tasks:6.1f}x tasks, {seconds / sensible_seconds:5.1f}x wall time")
    print("\nTiny chunks: thousands of tasks, each doing microseconds of work --")
    print("scheduler overhead dominates. Giant chunk: one task, zero parallelism,")
    print("and the whole array must fit in memory at once.")

    # SECTION: the rule of thumb and how OCS applies it
    print("\nRule of thumb: aim for chunks around ~100 MB (tens of MB to a few")
    print("hundred), and enough chunks to keep every core busy -- but not many more.")
    print("open-climate-service derives chunk sizes from the data itself:")
    print("  - time chunks from the temporal resolution (time_chunk_for_iso_step):")
    print("    ~a week of sub-daily steps, ~a month of daily steps, ~a year of weekly+")
    print("  - spatial chunks capped at 512 pixels per side (its pyramid tile size),")
    print("    so a map client never fetches a huge chunk to fill a small screen tile")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- every chunk is at least one task; every task costs fixed scheduler overhead")
    print("- tiny chunks: task count explodes and overhead dwarfs the real work")
    print("- one giant chunk: no parallelism and full-array memory pressure")
    print("- target ~100 MB per chunk, with enough chunks to occupy all cores")
    print("- OCS: time chunks follow data resolution, spatial chunks cap at 512")


if __name__ == "__main__":
    main()
