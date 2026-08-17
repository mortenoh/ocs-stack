"""Task stream analysis: finding the bottleneck from the records.

What: runs a deliberately unbalanced workload -- a few long tasks among many
short ones -- captures the task stream, and analyses it in code to locate the
straggler and quantify how much it cost.

Why: "the job was slow" becomes actionable only when you can say which tasks,
on which worker, and how much of the wall time they held. The dashboard shows
this as a picture; the same records as data let you assert on it, alert on it,
or compare two runs.

Run: make run EXAMPLE=0503_task_stream
"""

import time
from collections import Counter, defaultdict
from typing import Any

from distributed import get_task_stream

from playground_data_dask_distributed import connect

N_FAST = 40
N_SLOW = 3
FAST_SECONDS = 0.05
SLOW_SECONDS = 1.5


def fast_task(index: int) -> int:
    """A short task, representing the bulk of a normal workload.

    Args:
        index: Task identifier, returned unchanged.

    Returns:
        The index it was given.
    """
    time.sleep(FAST_SECONDS)
    return index


def slow_task(index: int) -> int:
    """A long task, representing the straggler this example hunts for.

    Args:
        index: Task identifier, returned unchanged.

    Returns:
        The index it was given.
    """
    time.sleep(SLOW_SECONDS)
    return index


def task_name(key: Any) -> str:
    """Reduce a task key to its bare function name.

    Args:
        key: A task key from a task-stream record.

    Returns:
        The function name portion of the key.
    """
    if isinstance(key, tuple):
        key = key[0] if key else ""
    return str(key).lstrip("('\"").split("-")[0]


def span_seconds(record: dict[str, Any], action: str) -> float:
    """Total the time a task spent in one phase.

    Args:
        record: One task-stream record.
        action: The phase to total, such as ``"compute"`` or ``"transfer"``.

    Returns:
        Seconds spent in that phase.
    """
    return sum(
        float(span["stop"]) - float(span["start"]) for span in record.get("startstops", []) if span["action"] == action
    )


def main() -> None:
    """Capture a task stream and analyse where the time went."""
    with connect() as session:
        client = session.client
        print(session.banner())

        slots = sum(int(w.get("nthreads", 0)) for w in client.scheduler_info()["workers"].values()) or 1
        print(f"\nSubmitting {N_FAST} tasks of {FAST_SECONDS}s and {N_SLOW} of {SLOW_SECONDS}s")
        print(f"into {slots} slots. Total work: {N_FAST * FAST_SECONDS + N_SLOW * SLOW_SECONDS:.1f}s.")

        # SECTION: capture
        with get_task_stream() as stream:
            started = time.perf_counter()
            futures = [client.submit(slow_task, i, pure=False) for i in range(N_SLOW)]
            futures += [client.submit(fast_task, i, pure=False) for i in range(N_FAST)]
            client.gather(futures)
            elapsed = time.perf_counter() - started

        # get_task_stream is typed as async; in a sync context it returns the
        # recorder object, whose .data holds the captured records.
        records: list[dict[str, Any]] = list(stream.data)  # pyright: ignore[reportAttributeAccessIssue]
        print(f"\nWall time {elapsed:.2f}s across {len(records)} recorded tasks.")

        # SECTION: where the time went, by task type
        compute_by_kind: defaultdict[str, float] = defaultdict(float)
        count_by_kind: Counter[str] = Counter()
        for record in records:
            kind = task_name(record.get("key", "?"))
            compute_by_kind[kind] += span_seconds(record, "compute")
            count_by_kind[kind] += 1

        print("\nCompute time by task type:")
        total_compute = sum(compute_by_kind.values()) or 1.0
        for kind, seconds in sorted(compute_by_kind.items(), key=lambda item: -item[1]):
            share = seconds / total_compute * 100
            print(f"  {kind:<14} {count_by_kind[kind]:>3} tasks  {seconds:>6.2f}s  ({share:>5.1f}% of compute)")

        # SECTION: the stragglers
        ranked = sorted(records, key=lambda r: span_seconds(r, "compute"), reverse=True)
        print("\nSlowest individual tasks:")
        for record in ranked[:5]:
            seconds = span_seconds(record, "compute")
            worker = str(record.get("worker", "?")).rsplit("/", 1)[-1]
            print(f"  {task_name(record.get('key', '?')):<14} {seconds:>6.2f}s on {worker}")

        # SECTION: per-worker balance
        by_worker: defaultdict[str, float] = defaultdict(float)
        tasks_per_worker: Counter[str] = Counter()
        for record in records:
            worker = str(record.get("worker", "?")).rsplit("/", 1)[-1]
            by_worker[worker] += span_seconds(record, "compute")
            tasks_per_worker[worker] += 1

        print("\nLoad per worker:")
        for worker, seconds in sorted(by_worker.items()):
            bar = "#" * int(seconds * 4)
            print(f"  {worker:<22} {tasks_per_worker[worker]:>3} tasks {seconds:>6.2f}s {bar}")

        spread = max(by_worker.values()) - min(by_worker.values()) if by_worker else 0.0
        print(f"  busiest minus idlest: {spread:.2f}s of compute")

        # SECTION: the diagnosis
        efficiency = total_compute / (elapsed * slots) * 100 if elapsed else 0.0
        floor = total_compute / slots
        print("\n--- Diagnosis ---")
        print(f"  total compute {total_compute:.2f}s over {slots} slots -> a {floor:.2f}s floor")
        print(f"  actual wall time {elapsed:.2f}s, parallel efficiency {efficiency:.0f}%")
        longest = span_seconds(ranked[0], "compute") if ranked else 0.0
        print(f"  the single longest task took {longest:.2f}s")
        if longest > floor:
            print(f"  That one task alone exceeds the {floor:.2f}s floor: no amount of extra")
            print("  workers can finish this batch faster than its slowest single task.")
            print("  The fix is splitting that task, not growing the cluster.")
        else:
            print("  No single task dominates; the batch is limited by total capacity.")

        # SECTION: summary
        print("\n=== Summary ===")
        print("- get_task_stream() gives the same records the dashboard draws")
        print("- startstops splits each task into compute, transfer, and disk phases")
        print("- grouping by task type finds which operation is expensive")
        print("- grouping by worker finds imbalance; ranking finds stragglers")
        print("- a batch can never beat its slowest single task, however many workers you add")


if __name__ == "__main__":
    main()
