"""Graph hygiene: build small graphs, build them once, reuse what you computed.

What: measures three habits that keep dask graphs healthy — slicing before
computing, replacing Python loops of ``compute()`` calls with one grouped
compute, and sharing or persisting intermediates instead of recomputing them.

Why: these are the production patterns open-climate-service encodes. OCS runs
openEO process graphs on dask, where the graph IS the program: work you never
put in the graph is never scheduled, a loop of computes rebuilds and re-runs
overlapping graphs, and an unshared intermediate is silently computed twice.

Run: make run EXAMPLE=0603_graph_hygiene
"""

import time
from collections.abc import Callable

import dask.array as da
import numpy as np
import pandas as pd
import xarray as xr

# dask.compute is public API, but dask's __init__ re-export is invisible to pyright; import from dask.base.
from dask.base import compute as dask_compute

from playground_data_dask.helpers import chunk_report, random_field, task_count


def timed(fn: Callable[[], object]) -> float:
    """Run a callable once and return the elapsed wall time in seconds.

    Args:
        fn: Zero-argument callable to time.

    Returns:
        Wall-clock seconds spent in the call.
    """
    t0 = time.perf_counter()
    fn()
    return time.perf_counter() - t0


def main() -> None:
    """Measure slicing early, batching computes, and reusing intermediates."""
    arr = random_field(seed=0)
    heavy = da.sin(arr) * da.cos(arr) + da.sqrt(arr)  # non-trivial per-chunk work on top of the source
    da.zeros(1).sum().compute()  # warm the thread pool so timings compare graphs, not startup cost
    print("Working array (heavy = sin(x)*cos(x) + sqrt(x) on top of it):")
    print(f"  {chunk_report(arr)}\n")

    # SECTION: slice before computing
    print("Habit 1 -- slice BEFORE computing. Goal: mean of the first 10 days of heavy.")
    lazy_sliced = heavy[:10].mean()
    lazy_full = heavy.mean()
    print(f"  task_count(heavy[:10].mean()) = {task_count(lazy_sliced):4d}  (only chunks touching days 0-9)")
    print(f"  task_count(heavy.mean())      = {task_count(lazy_full):4d}  (every chunk)")
    t_good = timed(lambda: lazy_sliced.compute())
    t_bad = timed(lambda: heavy.compute()[:10].mean())
    print(f"  compute sliced graph:            {t_good:.2f}s")
    print(f"  compute FULL array, then slice:  {t_bad:.2f}s  ({t_bad / t_good:.1f}x, and ~191 MB materialized)")
    print("  Slicing a lazy array prunes the graph; slicing a computed result prunes nothing.")

    # SECTION: one grouped compute instead of a loop of computes
    print("\nHabit 2 -- one grouped compute beats a Python loop of computes.")
    print("Goal: 12 monthly mean maps from a year of daily data.")
    field = xr.DataArray(
        arr,
        dims=("time", "y", "x"),
        coords={"time": pd.date_range("2023-01-01", periods=365, freq="D")},
        name="t2m",
    )
    loop_results: list[np.ndarray] = []  # element type from .values is partial in stubs

    def monthly_loop() -> None:
        """Compute each monthly mean in its own compute() call (the anti-pattern)."""
        loop_results.clear()
        for month in range(1, 13):
            sel = field.sel(time=slice(f"2023-{month:02d}", f"2023-{month:02d}"))
            loop_results.append(sel.mean("time").compute().values)

    grouped = field.groupby("time.month").mean("time")
    t_loop = timed(monthly_loop)
    grouped_values: np.ndarray | None = None

    def grouped_once() -> None:
        """Compute all 12 monthly means in a single graph execution."""
        nonlocal grouped_values
        grouped_values = grouped.compute().values

    t_group = timed(grouped_once)
    assert grouped_values is not None
    same = bool(np.allclose(np.stack(loop_results), grouped_values))
    print(f"  loop of 12 .compute() calls: {t_loop:.2f}s  (12 graph builds; boundary chunks regenerated)")
    print(f"  one groupby('time.month'):   {t_group:.2f}s  (one graph; each chunk generated once)")
    print(f"  ratio: {t_loop / t_group:.1f}x -- results identical: {same}")

    # SECTION: reuse intermediates instead of recomputing
    print("\nHabit 3 -- share or persist intermediates. Goal: min and max of a")
    print("normalized field norm = (arr - mean) / std, an expensive shared subgraph.")
    norm = (arr - arr.mean()) / arr.std()
    r_min, r_max = norm.min(), norm.max()
    t_twice = timed(lambda: (r_min.compute(), r_max.compute()))
    t_shared = timed(lambda: dask_compute(r_min, r_max))
    t0 = time.perf_counter()
    kept = norm.persist()
    t_persist_build = time.perf_counter() - t0
    t_persist_use = timed(lambda: (kept.min().compute(), kept.max().compute()))
    print(f"  two separate .compute() calls:    {t_twice:.2f}s  (norm computed TWICE)")
    print(f"  one dask.compute(r_min, r_max):   {t_shared:.2f}s  (norm computed once)")
    print(f"  persist norm, then both computes: {t_persist_build:.2f}s + {t_persist_use:.2f}s per extra pass")
    print("  Persist trades memory (~191 MB held) for cheap reuse across many later computes;")
    print("  a single dask.compute(...) is the lighter fix when the results are needed together.")

    # SECTION: the checklist
    print("\nGraph hygiene checklist:")
    print("  1. Subset lazily (slice/sel) before compute -- prune the graph, not the result")
    print("  2. Never call .compute() in a loop -- batch with groupby/resample or dask.compute(*many)")
    print("  3. Compute related results together so shared subgraphs run once")
    print("  4. Persist an intermediate only when it is reused across several later computes")
    print("  5. Keep pipelines as chained lazy ops -- fuse-friendly and schedulable as one graph")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- the graph is the program: work excluded from the graph is never scheduled")
    print("- slicing before compute cut tasks and wall time; slicing after saved nothing")
    print(f"- one grouped compute beat 12 looped computes by {t_loop / t_group:.1f}x on identical results")
    print("- dask.compute(a, b) runs shared subgraphs once; separate computes run them per call")
    print("- persist holds an intermediate in memory so later computes start from it")


if __name__ == "__main__":
    main()
