"""Task graphs: the dict of keys and dependencies behind every collection.

What: builds a small delayed pipeline and a chunked dask array, then inspects
the graph both carry: obj.__dask_graph__(), key counts via task_count, how keys
are named, and a printed adjacency listing produced by walking the graph dict.
Ends with why "build then execute" enables parallelism and optimization.

Why: a dask graph is just tasks plus dependencies, and so is an openEO process
graph -- open-climate-service translates one into the other and lets dask's
scheduler run it. Being able to read a graph is how you reason about what OCS
actually executes.

Run: make run EXAMPLE=0102_task_graphs
"""

from collections.abc import Mapping
from typing import Any

from dask.base import optimize
from dask.core import get_dependencies
from dask.delayed import Delayed, delayed
from dask.typing import Key

from playground_data_dask import chunk_report, random_field, task_count


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


def short(key: Key) -> str:
    """Return a graph key with its hash token trimmed for readable printing.

    Args:
        key: A graph key -- a "name-token" string, or a tuple for array chunks.

    Returns:
        The key as a string with each token shortened to its first 6 characters.
    """
    if isinstance(key, tuple):
        return "(" + ", ".join(short(part) for part in key) + ")"
    text = str(key)
    name, sep, token = text.partition("-")
    return f"{name}{sep}{token[:6]}" if sep else text


def print_adjacency(dsk: Mapping[Key, Any]) -> None:
    """Print one line per task: its key and the keys it depends on.

    Args:
        dsk: A materialized dask graph mapping keys to task definitions.
    """
    for key in sorted(dsk, key=str):
        deps = sorted(get_dependencies(dsk, key), key=str)
        arrow = " <- " + ", ".join(short(d) for d in deps) if deps else "   (leaf: no dependencies)"
        print(f"    {short(key)}{arrow}")


def main() -> None:
    """Inspect the task graphs behind a Delayed pipeline and a chunked array."""
    # SECTION: a delayed pipeline carries a graph
    print("Build a tiny pipeline: a = inc(1); b = inc(2); total = add(a, b) -- all delayed.")
    a: Delayed = delayed(inc)(1)
    b: Delayed = delayed(inc)(2)
    total: Delayed = delayed(add)(a, b)
    graph = total.__dask_graph__()
    print(f"  total.__dask_graph__() -> {type(graph).__name__}")
    print(f"  task_count(total) = {task_count(total)} (one task per delayed call: inc, inc, add)")

    # SECTION: how keys are named
    print("\nEvery task has a key: '<function name>-<hash token>' for delayed calls.")
    print(f"  total.key = {total.key!r}")
    print("  all keys in the graph:")
    for key in sorted(dict(graph), key=str):
        print(f"    {key}")
    print("  the token makes keys unique; the prefix tells you which function runs")

    # SECTION: dependencies as an adjacency listing
    print("\nThe graph is tasks plus dependencies -- printed as an adjacency listing:")
    print_adjacency(dict(graph))
    print("  read it bottom-up: add needs both incs; the incs are independent leaves")

    # SECTION: arrays carry graphs too
    print("\nA dask array is the same thing at scale -- one task per chunk operation:")
    field = random_field(days=60, ny=256, nx=256, time_chunk=30, spatial_chunk=128)
    print(f"  {chunk_report(field)}")
    print(f"  task_count(field) = {task_count(field)} (one random-block task per chunk)")
    mean = field.mean()
    print(f"  task_count(field.mean()) = {task_count(mean)} (per-chunk partials + combine steps)")
    first_key = sorted(dict(field.__dask_graph__()), key=str)[0]
    print(f"  array keys are tuples: {short(first_key)} = (name, block index per dim)")

    # SECTION: why build-then-execute pays off
    print("\nWhy record a graph instead of just running? The scheduler sees the whole plan:")
    print("  - parallelism: independent tasks (the two incs, every chunk) can run at once")
    print("  - optimization: dask rewrites the graph before executing, e.g. culling unused chunks")
    day0 = field[0].mean()  # reads only the 4 spatial blocks of the first time-chunk
    optimized: Any = optimize(day0)[0]  # dask.base.optimize is untyped; result is a dask array
    print(f"  field[0].mean() graph: {task_count(day0)} raw tasks -> {task_count(optimized)} after dask.optimize")
    print("  the optimizer dropped chunks the slice never reads -- only possible because")
    print("  the whole graph exists before anything runs")
    print(
        f"  same answer either way: day-0 mean = {float(day0.compute()):.4f}, whole mean = {float(mean.compute()):.4f}"
    )

    # SECTION: summary
    print("\n=== Summary ===")
    print("- every dask collection carries a graph, exposed by obj.__dask_graph__()")
    print("- a graph is a dict: key -> task; dependencies are the edges between keys")
    print("- delayed keys are 'name-token' strings; array keys are (name, i, j, ...) tuples per chunk")
    print("- task_count(obj) = number of keys = number of scheduled tasks")
    print("- build-then-execute lets the scheduler parallelize independent tasks and optimize first")


if __name__ == "__main__":
    main()
