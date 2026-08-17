"""Sharing intermediates: one compute for many outputs runs common work once.

What: derives two results from one expensive shared step. Separate .compute()
calls execute the shared step twice; a single dask.compute(x, y) merges the
graphs and runs it once. Both a call counter inside the delayed function and
perf_counter timings quantify the difference.

Why: an openEO process graph in open-climate-service routinely fans out --
one loaded datacube feeds many derived products. Because dask deduplicates
shared keys within one compute, the load happens once; computing outputs
separately silently repeats it. This is the habit that keeps pipelines cheap.

Run: make run EXAMPLE=0103_sharing_intermediates
"""

import time
from collections import Counter
from typing import Any

from dask.base import compute
from dask.delayed import Delayed, delayed

from playground_data_dask import task_count

EXPENSIVE_SECONDS = 0.3
"""Deterministic cost of the shared step, so the timing comparison is reliable."""

CALLS: Counter[str] = Counter()
"""Counts real executions per function; shared by all threads of the threaded scheduler."""


def load_field() -> float:
    """Pretend to load a datacube: the expensive step both outputs depend on.

    Returns:
        A stand-in value for the loaded data.
    """
    CALLS["load_field"] += 1
    time.sleep(EXPENSIVE_SECONDS)
    return 42.0


def scale(value: float) -> float:
    """Cheap derived output number one.

    Args:
        value: The shared intermediate value.

    Returns:
        The value scaled by 10.
    """
    CALLS["scale"] += 1
    return value * 10.0


def offset(value: float) -> float:
    """Cheap derived output number two.

    Args:
        value: The shared intermediate value.

    Returns:
        The value shifted by 1.
    """
    CALLS["offset"] += 1
    return value + 1.0


def main() -> None:
    """Compute two results sharing an expensive step: separately, then together."""
    # SECTION: two outputs, one shared intermediate
    print("Build two outputs from ONE shared Delayed: base = load_field();")
    print("x = scale(base); y = offset(base). Sharing means reusing the same object.")
    base: Delayed = delayed(load_field)()
    x: Delayed = delayed(scale)(base)
    y: Delayed = delayed(offset)(base)
    print(f"  task_count(x) = {task_count(x)}, task_count(y) = {task_count(y)} (each: load + its own step)")
    merged = dict(x.__dask_graph__()) | dict(y.__dask_graph__())
    print(f"  merged graphs hold {len(merged)} keys, not 4 -- base has ONE key, {str(base.key)[:20]}...")

    # SECTION: separate computes repeat the shared work
    print("\nSeparate calls: x.compute() then y.compute() -- two graphs, two scheduler runs.")
    CALLS.clear()
    t0 = time.perf_counter()
    rx: Any = x.compute(scheduler="threads")
    ry: Any = y.compute(scheduler="threads")
    separate_s = time.perf_counter() - t0
    print(f"  results: x={rx}, y={ry}")
    print(f"  call counts: {dict(CALLS)}")
    print("  load_field ran TWICE -- each compute() only sees its own graph")

    # SECTION: one compute shares the work
    print("\nOne call: dask.compute(x, y) -- graphs merge, the shared key runs once.")
    CALLS.clear()
    t0 = time.perf_counter()
    rx, ry = compute(x, y, scheduler="threads")
    together_s = time.perf_counter() - t0
    print(f"  results: x={rx}, y={ry}")
    print(f"  call counts: {dict(CALLS)}")
    print("  load_field ran ONCE -- identical keys are deduplicated within a single compute")

    # SECTION: the timing evidence
    print("\nTimings (shared step costs a fixed {:.1f} s):".format(EXPENSIVE_SECONDS))
    ratio = separate_s / together_s
    print(f"  separate computes were ~{ratio:.1f}x slower than one dask.compute (expected ~2x)")
    print("  the ratio is the number of times the expensive step was repeated")

    # SECTION: the caveat that makes sharing work
    print("\nCaveat: sharing requires the SAME Delayed object (same key).")
    base2: Delayed = delayed(load_field)()
    print(f"  a second delayed(load_field)() gets a fresh key: {str(base.key)[-6:]} vs {str(base2.key)[-6:]}")
    print("  build once, reuse the variable -- do not re-wrap the call per output")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- reuse one Delayed object so every output points at the same graph key")
    print("- separate .compute() calls each run their whole graph: shared work repeats")
    print("- dask.compute(x, y) merges graphs first: each key, shared or not, runs once")
    print("- evidence: call counter 2 -> 1, and ~2x faster wall time for the joint compute")
    print("- rule of thumb: batch related outputs into a single compute call")


if __name__ == "__main__":
    main()
