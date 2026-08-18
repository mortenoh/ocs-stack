"""Local diagnostics: ProgressBar, Profiler, and ResourceProfiler.

What: wraps computations on the default threaded scheduler with the local
diagnostics from dask.diagnostics -- a live ProgressBar, then Profiler and
ResourceProfiler -- and prints a text summary of what they captured (task
counts, per-task-name time, peak memory and CPU).

Why: open-climate-service executes openEO process graphs on dask; when a graph
is slow you need to see where time and memory go. These tools cover the local
schedulers; the distributed scheduler has the dashboard and performance_report.

Run: make run EXAMPLE=0304_diagnostics
"""

from collections import defaultdict
from typing import Any

# the dask.diagnostics re-exports are not stub-visible; import from the defining modules.
from dask.diagnostics.profile import Profiler, ResourceProfiler
from dask.diagnostics.progress import ProgressBar

from ocs_stack_dask import chunk_report, random_field, task_count


def key_prefix(key: Any) -> str:
    """Return the human-readable task-name prefix of a dask graph key.

    Args:
        key: A graph key, either a string like "name-token" or a tuple whose
            first element is such a string.

    Returns:
        The name part of the key with its trailing token stripped.
    """
    name = key[0] if isinstance(key, tuple) else key
    text = name if isinstance(name, str) else str(name)
    return text.rsplit("-", 1)[0]


def main() -> None:
    """Profile threaded computations with dask's local diagnostic tools."""
    # SECTION: the workload
    print("These diagnostics instrument the LOCAL schedulers (threads/processes/sync),")
    print("not distributed clusters. The workload is a chunked reduction:")
    field = random_field(days=365, ny=512, nx=512, time_chunk=30, spatial_chunk=256)
    graph = ((field - 0.5) ** 2).mean()
    print(f"  data:  {chunk_report(field)}")
    print(f"  graph: ((x - 0.5)**2).mean() = {task_count(graph)} tasks")

    # SECTION: ProgressBar -- am I making progress at all?
    print("\nProgressBar hooks the scheduler and draws a live bar during compute():")
    with ProgressBar():
        result = float(graph.compute())
    print(f"  result: {result:.6f} (variance of uniform [0, 1) is 1/12 = {1 / 12:.6f})")

    # SECTION: Profiler + ResourceProfiler -- where did time and memory go?
    print("\nProfiler records every task's start/end/thread; ResourceProfiler samples")
    print("process memory and CPU on an interval. Both are context managers:")
    sample_dt: Any = 0.05  # runtime accepts float seconds; the annotation says int
    with Profiler() as prof, ResourceProfiler(dt=sample_dt) as rprof:
        result = float(graph.compute())
    rprof.close()  # stop the background sampling process
    print(f"  result again: {result:.6f}")

    # SECTION: text summary of the profiler data
    print("\nWhat the Profiler captured (prof.results, one record per task):")
    records: list[Any] = list(prof.results)  # TaskData namedtuples; stubs type them loosely
    span = max(r.end_time for r in records) - min(r.start_time for r in records)
    busy: dict[str, float] = defaultdict(float)
    counts: dict[str, int] = defaultdict(int)
    for record in records:
        prefix = key_prefix(record.key)
        busy[prefix] += record.end_time - record.start_time
        counts[prefix] += 1
    print(f"  tasks executed: {len(records)} in {span:.2f} s wall time")
    print(f"  busy time across threads: {sum(busy.values()):.2f} s -> parallelism paid off")
    print("  time by task name (top 4):")
    for prefix, seconds in sorted(busy.items(), key=lambda kv: kv[1], reverse=True)[:4]:
        print(f"    {prefix:<28} {counts[prefix]:>3} tasks  {seconds:6.2f} s busy")

    print("\nWhat the ResourceProfiler captured (rprof.results, one sample per 0.05 s):")
    samples: list[Any] = list(rprof.results)  # ResourceData namedtuples: (time, mem, cpu)
    if samples:
        max_mem = max(s.mem for s in samples)
        max_cpu = max(s.cpu for s in samples)
        print(f"  samples: {len(samples)}, peak memory: {max_mem:.0f} MB, peak CPU: {max_cpu:.0f}%")
        print("  (>100% CPU means multiple threads were computing at once)")
        print("  peak memory stays far below the ~700 MB of total data: chunks stream through")
    else:
        print("  no samples collected: the compute finished within one sampling interval")

    # SECTION: the distributed counterpart
    print("\nOn the DISTRIBUTED scheduler these tools do not apply; there you use the live")
    print("dashboard (example 0302) or distributed.performance_report, a context manager")
    print("that saves the same task-stream/profile views as a standalone HTML file.")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- dask.diagnostics instruments the local schedulers only")
    print("- ProgressBar: cheap liveness check around any compute()")
    print("- Profiler: per-task start/end times -> where wall time went, by task name")
    print("- ResourceProfiler: sampled memory/CPU -> peak footprint, parallel efficiency")
    print("- both can render bokeh plots via .visualize(); the raw .results are plain records")
    print("- distributed equivalent: the dashboard and performance_report(filename=...)")


if __name__ == "__main__":
    main()
