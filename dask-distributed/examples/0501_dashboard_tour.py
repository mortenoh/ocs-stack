"""The dashboard, read from Python instead of a browser.

What: walks the panels of the scheduler dashboard and prints, for each one,
the same numbers it displays -- fetched from the scheduler API so the panels
stop being decoration and start being data you can assert on.

Why: the dashboard is the first thing to open when a cluster misbehaves, but
"it looked busy" is not a diagnosis. Every panel is backed by a scheduler
endpoint you can query, which is how you turn a hunch into a number, or a
health check into code.

Run: make run EXAMPLE=0501_dashboard_tour
"""

import time
from collections import Counter, defaultdict
from typing import Any

import dask.array as da
from distributed import get_task_stream

from playground_dask_distributed import connect


def sample_workload() -> Any:
    """Return a lazy array whose computation is worth watching.

    Sized so real arithmetic, not scheduling overhead, dominates the wall
    time -- otherwise the efficiency number below measures the wrong thing.

    Returns:
        A dask array with enough chunks to keep every worker busy.
    """
    array = da.random.random((8000, 8000), chunks=(1000, 1000))
    return ((array**2 + array).mean(axis=0) ** 0.5).sum()


def task_name(key: Any) -> str:
    """Reduce a dask task key to the bare function name.

    Keys arrive as ``"('mean_chunk-<token>', 0, 1)"`` or as tuples; both
    reduce to ``mean_chunk``, which is what the dashboard colors by.

    Args:
        key: A task key from a task-stream record.

    Returns:
        The task's function name.
    """
    if isinstance(key, tuple):
        key = key[0] if key else ""
    text = str(key).lstrip("('\"")
    return text.split("-")[0]


def main() -> None:
    """Print each dashboard panel's underlying numbers."""
    with connect() as session:
        client = session.client
        print(session.banner())

        link = client.dashboard_link
        if session.is_compose:
            print(f"\nDashboard: {link}")
            print("Everything below is what those panels are showing, fetched over the API.")
        else:
            print("\nThe fallback cluster runs without a dashboard, but the same scheduler")
            print("endpoints answer, so every panel below still has real numbers behind it.")

        # SECTION: the worker table
        print("\n--- Panel: Workers ---")
        print("Per-worker threads, memory, and how hard each one is working.")
        info = client.scheduler_info()
        workers: dict[str, Any] = info.get("workers", {})
        print(f"  {'worker':<24} {'threads':>7} {'limit GiB':>10} {'managed MB':>11} {'cpu %':>6}")
        for address, meta in sorted(workers.items()):
            metrics = meta.get("metrics", {})
            print(
                f"  {address.rsplit('/', 1)[-1]:<24} "
                f"{meta.get('nthreads', 0):>7} "
                f"{meta.get('memory_limit', 0) / 2**30:>10.2f} "
                f"{metrics.get('managed_bytes', 0) / 1e6:>11.1f} "
                f"{metrics.get('cpu', 0):>6.1f}"
            )
        print("  Red memory bars in the browser mean a worker near its spill threshold;")
        print("  here that is managed MB approaching limit GiB.")

        # SECTION: task stream and progress
        print("\n--- Panels: Task Stream and Progress ---")
        print("Running a workload and capturing what the task stream would draw.")
        with get_task_stream() as stream:
            started = time.perf_counter()
            result = float(sample_workload().compute())
            elapsed = time.perf_counter() - started

        # get_task_stream is typed as async; in a sync context it returns the
        # recorder object, whose .data holds the captured records.
        records: list[dict[str, Any]] = list(stream.data)  # pyright: ignore[reportAttributeAccessIssue]
        print(f"  result {result:,.2f} computed in {elapsed:.2f}s from {len(records)} tasks")

        by_kind = Counter(task_name(record.get("key", "?")) for record in records)
        print("  task types (the colored bars in the stream):")
        for kind, count in by_kind.most_common(5):
            print(f"    {kind:<28} {count:>4}")

        by_worker = Counter(str(record.get("worker", "?")) for record in records)
        print("  tasks per worker (the stream's rows):")
        for worker, count in sorted(by_worker.items()):
            print(f"    {worker.rsplit('/', 1)[-1]:<24} {count:>4}")

        # SECTION: where the time went
        print("\n--- Panel: Task Stream colors ---")
        print("Each bar is colored by what the worker was doing. The categories:")
        # Each record carries startstops: a list of
        # {"action": ..., "start": ..., "stop": ...} spans, one per phase the
        # worker went through for that task.
        # defaultdict(float) rather than Counter: these are durations, and
        # Counter is typed for integer counts.
        totals: defaultdict[str, float] = defaultdict(float)
        for record in records:
            for span in record.get("startstops", []):
                totals[str(span["action"])] += float(span["stop"]) - float(span["start"])
        grand = sum(totals.values()) or 1.0
        for action, seconds in sorted(totals.items(), key=lambda item: -item[1]):
            print(f"    {action:<16} {seconds:>7.3f}s  ({seconds / grand * 100:>5.1f}%)")
        print("  'compute' is the work you wanted. 'transfer' and 'disk-read' are")
        print("  overhead: a stream dominated by them means data is in the wrong place.")

        # SECTION: efficiency
        compute_seconds = totals.get("compute", 0.0)
        slots = sum(int(meta.get("nthreads", 0)) for meta in workers.values()) or 1
        efficiency = compute_seconds / (elapsed * slots) * 100 if elapsed else 0.0
        print("\n--- Reading it as a health check ---")
        print(f"  {compute_seconds:.2f}s of compute across {slots} slots in {elapsed:.2f}s wall")
        print(f"  parallel efficiency: {efficiency:.0f}% of available slot-time was real work")
        print("  Low efficiency with idle workers means the graph is too serial or")
        print("  the chunks are too big; low efficiency with busy workers means overhead.")

        # SECTION: summary
        print("\n=== Summary ===")
        print("- every dashboard panel is backed by a scheduler endpoint you can query")
        print("- scheduler_info() drives the worker table: threads, memory, cpu")
        print("- get_task_stream() returns the records the task stream draws")
        print("- startstops break each task into compute, transfer, and disk time")
        print("- 'busy' is not a diagnosis; slot-time efficiency is a number")


if __name__ == "__main__":
    main()
