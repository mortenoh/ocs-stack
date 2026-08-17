"""Rechunking: the cost of changing a chunk layout after creation.

What: rechunks a (time, y, x) field between time-oriented and space-oriented
layouts, counting the tasks an all-to-all rechunk adds versus a cheap
boundary-aligned merge, then compares access patterns -- point time-series
reads vs single-day map reads -- across layouts to show why the layout must
match the workload.

Why: this is exactly the trade open-climate-service tunes when writing zarr:
it caps spatial chunks at 512x512 and derives the time chunk from the dataset
resolution, because the chunk layout frozen into the store decides whether a
time-series query or a map query is one read or hundreds. Choosing chunks at
creation is cheap; rechunking later costs an all-to-all shuffle.

Run: make run EXAMPLE=0204_rechunking
"""

from typing import Any

from dask.base import optimize

from playground_data_dask import chunk_report, random_field, task_count


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
    """Measure rechunk costs and how chunk layout matches (or fights) access patterns."""
    # SECTION: the starting layout
    print("Start from the balanced OCS-style layout: 30-day time chunks, 128x128 spatial tiles.")
    base = random_field()  # (365, 256, 256), chunks (30, 128, 128)
    print(f"  base: {chunk_report(base)}, tasks={task_count(base)}")

    # SECTION: an all-to-all rechunk is expensive
    print("\nRechunk to a time-series layout (full time, 32x32 tiles) -- chunk boundaries cross:")
    # dask's stub for rechunk declares chunks as str only; tuples are valid at runtime.
    ts_layout: Any = (365, 32, 32)
    reshaped = base.rechunk(ts_layout)
    print(f"  result: {chunk_report(reshaped)}")
    print(f"  tasks: {task_count(base)} -> {task_count(reshaped)}  (+{task_count(reshaped) - task_count(base)})")
    print("  every new chunk needs pieces of many old chunks: split tasks + concat tasks,")
    print("  an all-to-all data movement -- and at scale, memory pressure while both layouts coexist")

    # SECTION: aligned rechunks are cheap
    print("\nRechunk to (60, 128, 128) -- old 30-day boundaries line up with new 60-day ones:")
    merged_layout: Any = (60, 128, 128)  # tuple chunks again, past the str-only stub
    merged = base.rechunk(merged_layout)
    print(f"  result: {chunk_report(merged)}")
    print(f"  tasks: {task_count(base)} -> {task_count(merged)}  (+{task_count(merged) - task_count(base)})")
    print("  aligned boundaries just concatenate neighbours: one merge task per new chunk")
    same_layout: Any = (30, 128, 128)  # tuple chunks again, past the str-only stub
    same = base.rechunk(same_layout)
    print(f"  rechunk to the current layout is free: returns the same array ({same is base}), tasks={task_count(same)}")

    # SECTION: creation beats rechunking
    print("\nChoosing the layout at creation skips the shuffle entirely:")
    fresh = random_field(time_chunk=365, spatial_chunk=32)
    print(f"  created as (365, 32, 32) directly:   tasks={task_count(fresh)}")
    print(f"  created balanced, then rechunked:    tasks={task_count(reshaped)}")
    print("  same final layout, ~3x the tasks plus all-to-all data movement -- this is why")
    print("  OCS decides zarr chunks once, at write time, not per query")

    # SECTION: layout vs access pattern
    print("\nWhy anyone rechunks at all: no single layout serves every read. Compare three")
    print("layouts on two OCS-style queries (optimized task counts = chunks actually touched):")
    balanced = base
    time_opt = random_field(time_chunk=365, spatial_chunk=32)  # full time series per tile
    map_opt = random_field(time_chunk=1, spatial_chunk=256)  # one whole map per day
    header = f"  {'layout':22s} {'time series arr[:, 7, 7]':>26s} {'day map arr[100]':>18s}"
    print(header)
    for name, arr in (
        ("balanced (30,128,128)", balanced),
        ("time-opt (365,32,32)", time_opt),
        ("map-opt  (1,256,256)", map_opt),
    ):
        ts_tasks = optimized_task_count(arr[:, 7, 7])
        map_tasks = optimized_task_count(arr[100])
        print(f"  {name:22s} {ts_tasks:>20d} tasks {map_tasks:>12d} tasks")
    print("  time-chunked layouts make map reads touch every time chunk; space-chunked layouts")
    print("  make time-series reads touch every day. The balanced layout pays a modest cost on")
    print("  both -- OCS's 512x512 spatial cap with resolution-derived time chunks is that middle")
    print("  ground, sized so both query shapes stay a handful of chunk reads")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- rechunk() builds a new layout from the old one: crossing boundaries means an")
    print("  all-to-all split+concat; aligned boundaries mean cheap merges; same layout is free")
    print("- Creating the right layout up front costs a fraction of rechunking into it later")
    print("- Time-chunked vs space-chunked is a real trade: each makes the other's query expensive")
    print("- Stores frozen to disk (zarr) must pick one layout -- OCS tunes it (<=512x512 spatial,")
    print("  resolution-derived time chunks) so common reads stay cheap without rechunking")


if __name__ == "__main__":
    main()
