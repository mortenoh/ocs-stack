"""Chunked arrays: dask.array is blocked numpy.

What: builds a lazy (time, y, x) climate field and inspects its block
structure -- .chunks, .blocks, .numblocks, npartitions-style counts via
chunk_report -- then proves the values are plain numpy underneath and shows
how the chunk shape chosen at creation drives chunk count and chunk size.

Why: open-climate-service stores every dataset as a (time, y, x) zarr store,
and the chunk layout picked at write time decides how many tasks every later
read or computation costs. A dask array is nothing more than a grid of numpy
blocks plus a task graph that knows how to produce each block.

Run: make run EXAMPLE=0201_chunked_arrays
"""

from typing import Any

import numpy as np

from playground_dask import chunk_report, random_field, task_count


def main() -> None:
    """Inspect the block structure of a chunked array and its creation-time trade-offs."""
    # SECTION: one array, many chunks
    print("A dask array is one logical array split into a grid of numpy blocks (chunks).")
    print("random_field() mimics an OCS climate store: daily data, (time, y, x), zarr-style chunks.")
    arr = random_field()  # (365, 256, 256), chunks (30, 128, 128), seed 0
    print(f"  {chunk_report(arr)}")
    print(f"  arr.chunks[0] (per-chunk sizes along time) = {arr.chunks[0]}")
    print("    -> 12 full 30-day chunks plus a 5-day remainder; chunks need not be uniform")
    print(f"  arr.numblocks = {arr.numblocks}  (blocks per axis: 13 x 2 x 2)")
    # dask's Array stub omits .npartitions and .blocks; reach them through a narrowly-typed Any.
    arr_dyn: Any = arr
    print(f"  arr.npartitions = {arr_dyn.npartitions}  (total blocks, the dataframe world calls these partitions)")
    print(f"  arr.blocks.shape = {arr_dyn.blocks.shape}  (.blocks indexes the grid of blocks)")
    one_block = arr_dyn.blocks[0, 0, 0]
    print(f"  arr.blocks[0, 0, 0] is itself a dask array: {chunk_report(one_block)}")
    print(f"  task_count(arr) = {task_count(arr)}  -- one task per chunk just to create the data")

    # SECTION: same values as numpy
    print("\nThe blocks hold ordinary numpy data -- computing gives the same values numpy would.")
    npa = arr.compute()  # materialize the whole field once: ~191 MB of float64
    print(f"  arr.compute() -> {type(npa).__name__} with shape {npa.shape}, dtype {npa.dtype}")
    sliced = arr[10:20, :64, :64].compute()
    print(f"  slice matches numpy slice:       np.allclose = {np.allclose(sliced, npa[10:20, :64, :64])}")
    scaled = (arr * 9.0 + 20.0)[:5].compute()
    print(f"  elementwise math matches numpy:  np.allclose = {np.allclose(scaled, npa[:5] * 9.0 + 20.0)}")
    print(f"  arr.mean() vs npa.mean():        np.allclose = {np.allclose(arr.mean().compute(), npa.mean())}")

    # SECTION: chunk shape is chosen at creation
    print("\nThe chunk shape is a creation-time decision; same data, three different layouts:")
    balanced = random_field(time_chunk=30, spatial_chunk=128)
    tiny = random_field(time_chunk=1, spatial_chunk=32)
    single = random_field(time_chunk=365, spatial_chunk=256)
    for name, layout in (("balanced", balanced), ("tiny", tiny), ("single", single)):
        print(f"  {name:8s}: {chunk_report(layout)}, tasks={task_count(layout)}")
    print("  tiny chunks  -> tens of thousands of tasks; scheduler overhead dominates real work")
    print("  one chunk    -> a single task; no parallelism and the whole array must fit in memory")
    print("  balanced     -> ~4 MB blocks; enough tasks to parallelize, few enough to schedule cheaply")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- A dask array = a grid of numpy blocks + a task graph; one task per block operation")
    print("- .chunks gives per-axis block sizes, .numblocks/.npartitions count them, .blocks indexes them")
    print("- Computed values are identical to numpy's -- dask only changes when and where work happens")
    print("- Chunk shape is fixed at creation and sets the task count for everything downstream")
    print("- OCS picks zarr chunk shapes at write time for exactly this reason (see 0204)")


if __name__ == "__main__":
    main()
