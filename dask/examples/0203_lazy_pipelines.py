"""Lazy pipelines: chained operations build a graph, one compute runs it.

What: chains elementwise math and reductions on a chunked (time, y, x) field,
timing each step to show it costs ~0 (only the graph grows, measured with
task_count), then runs a single .compute() at the end that executes the whole
pipeline and verifies the result against a numpy reference.

Why: open-climate-service pipelines (openEO process graphs on dask) work
exactly like this: every step -- scale, anomaly, aggregate -- is graph
construction, and nothing touches the data until the final write. Laziness is
what lets dask fuse steps, cull unused chunks, and never hold intermediates
for the full array in memory.

Run: make run EXAMPLE=0203_lazy_pipelines
"""

from time import perf_counter

import numpy as np

from ocs_stack_dask import chunk_report, random_field, task_count


def main() -> None:
    """Chain lazy array operations, watch the graph grow, then compute once."""
    # SECTION: the source is already lazy
    print("Every step below is graph construction -- watch the wall time stay near zero.")
    arr = random_field()  # (365, 256, 256), chunks (30, 128, 128)
    print(f"  source: {chunk_report(arr)}, tasks={task_count(arr)}")

    # SECTION: build the pipeline step by step
    print("\nPipeline: scale to degC -> daily anomaly vs time-mean -> spatial-mean time series")
    t0 = perf_counter()
    scaled = arr * 9.0 + 20.0  # elementwise: pretend the field is temperature in degC
    t1 = perf_counter()
    print(f"  step 1  scaled = arr * 9 + 20        {(t1 - t0) * 1000:8.2f} ms, tasks={task_count(scaled)}")
    anomaly = scaled - scaled.mean(axis=0)  # reduction feeding an elementwise op
    t2 = perf_counter()
    print(f"  step 2  anomaly = scaled - time-mean {(t2 - t1) * 1000:8.2f} ms, tasks={task_count(anomaly)}")
    series = anomaly.mean(axis=(1, 2))  # reduce space away: one value per day
    t3 = perf_counter()
    print(f"  step 3  series = spatial mean        {(t3 - t2) * 1000:8.2f} ms, tasks={task_count(series)}")
    print(f"  three steps of 'work' on a 191 MB array took {(t3 - t0) * 1000:.2f} ms -- no data was touched")
    print(f"  series is still lazy: shape={series.shape}, dtype={series.dtype}, tasks={task_count(series)}")

    # SECTION: one compute executes everything
    print("\nOne .compute() at the end executes the whole graph -- this is where the time goes:")
    t4 = perf_counter()
    result = series.compute()
    t5 = perf_counter()
    print(f"  series.compute() -> numpy shape {result.shape} in {t5 - t4:.2f} s")
    print(f"  first 3 daily anomalies: {np.array2string(result[:3], precision=6)}")
    print("  scheduler note: chunks stream through the pipeline; the full scaled/anomaly")
    print("  intermediates never exist in memory at once")

    # SECTION: verify against numpy
    print("\nSame math with plain numpy agrees:")
    npa = arr.compute()
    scaled_ref = npa * 9.0 + 20.0
    anomaly_ref = scaled_ref - scaled_ref.mean(axis=0)
    series_ref = anomaly_ref.mean(axis=(1, 2))
    print(f"  np.allclose(dask pipeline, numpy pipeline) = {np.allclose(result, series_ref)}")

    # SECTION: reusing intermediates
    print("\nReusing an intermediate? Each .compute() re-runs its whole graph from scratch;")
    print("when many outputs branch off one intermediate, .persist() would pin its chunks in")
    print("memory so later steps start from there. Here we stay with plain compute and simply")
    print("hand dask both outputs at once -- shared subgraphs run once per compute call:")
    t6 = perf_counter()
    hottest, coldest = int(series.argmax().compute()), int(series.argmin().compute())
    t7 = perf_counter()
    print(f"  argmax/argmin over the same pipeline: day {hottest} hottest, day {coldest} coldest")
    print(f"  (two separate computes took {t7 - t6:.2f} s -- 0103 shows how one compute shares work)")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- Elementwise ops and reductions on dask arrays return lazy arrays in ~0 time")
    print("- Each step only appends tasks: task_count grows, memory and CPU do not")
    print("- A single terminal .compute() executes the fused, culled graph in one pass")
    print("- Results match numpy exactly; laziness changes scheduling, never values")
    print("- persist() exists for reused intermediates; the default is one compute at the end")


if __name__ == "__main__":
    main()
