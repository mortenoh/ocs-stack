"""map_blocks and map_overlap: custom numpy functions over chunks, with and without halos.

What: runs a per-block numpy function (block-local normalization) with
da.map_blocks, then a 3x3 spatial mean filter with da.map_overlap, showing
that stencils need a one-cell halo from neighbouring chunks -- map_blocks
alone produces seams at chunk edges -- and verifying both against plain
numpy references.

Why: real climate processing on (time, y, x) stores is full of neighbourhood
operations -- smoothing, gradients, regridding kernels -- and open-climate-
service runs them chunk by chunk over zarr. map_blocks is the escape hatch
for any numpy code; map_overlap is the same thing plus the halo exchange that
makes stencils correct across the chunk boundaries the store's layout created.

Run: make run EXAMPLE=0205_map_blocks_overlap
"""

from typing import Any

import dask.array as da
import numpy as np
from numpy.lib.stride_tricks import sliding_window_view

from climate_stack_dask import chunk_report, random_field, task_count


def normalize_block(block: np.ndarray) -> np.ndarray:
    """Normalize a block to zero mean and unit variance using only its own values.

    Args:
        block: A numpy block of any shape.

    Returns:
        The block standardized with its own mean and standard deviation.
    """
    result: np.ndarray = (block - block.mean()) / block.std()
    return result


def mean3x3(field: np.ndarray) -> np.ndarray:
    """Apply a 3x3 mean filter over the trailing (y, x) axes of a (time, y, x) array.

    Edges are handled by replicating the border values, so the output has the
    same shape as the input. Applied to a full array this is the ground truth;
    applied per block it only sees that block's values.

    Args:
        field: A (time, y, x) numpy array.

    Returns:
        The filtered array, same shape and dtype as the input.
    """
    padded = np.pad(field, ((0, 0), (1, 1), (1, 1)), mode="edge")
    windows = sliding_window_view(padded, (3, 3), axis=(1, 2))
    result: np.ndarray = windows.mean(axis=(-2, -1))
    return result


def main() -> None:
    """Run per-block numpy functions with map_blocks and a halo-aware stencil with map_overlap."""
    # SECTION: the chunked field
    print("A small OCS-shaped field, chunked so every axis has interior chunk boundaries:")
    arr = random_field(days=6, ny=64, nx=64, time_chunk=3, spatial_chunk=32)
    print(f"  {chunk_report(arr)}, tasks={task_count(arr)}")
    npa = arr.compute()  # numpy copy for ground-truth references
    # mypy mis-binds dask's decorator-wrapped Array.map_blocks method; call it through a narrowly-typed Any.
    map_blocks: Any = arr.map_blocks

    # SECTION: map_blocks runs your numpy function once per block
    print("\nmap_blocks(normalize_block): each of the 8 blocks is standardized independently.")
    normalized: da.Array = map_blocks(normalize_block)
    print(f"  lazy result: {chunk_report(normalized)}, tasks={task_count(normalized)} (one task per block)")
    normalized_dyn: Any = normalized  # dask's Array stub omits .blocks; index it through a narrowly-typed Any
    got = normalized_dyn.blocks[0, 0, 0].compute()
    want = normalize_block(npa[0:3, 0:32, 0:32])
    print(f"  block (0,0,0) equals normalizing that numpy sub-array alone: {np.allclose(got, want)}")
    globally = (npa - npa.mean()) / npa.std()
    print(f"  ...but differs from a global normalization: allclose = {np.allclose(normalized.compute(), globally)}")
    print("  map_blocks is block-LOCAL: the function never sees values outside its own chunk")

    # SECTION: stencils break at chunk edges without halos
    print("\nA 3x3 mean filter needs each cell's neighbours -- some live in the next chunk over.")
    reference = mean3x3(npa)  # ground truth: filter applied to the whole array at once
    seamed: da.Array = map_blocks(mean3x3)
    seamed_np = seamed.compute()
    bad = int((~np.isclose(seamed_np, reference)).sum())
    print(f"  map_blocks(mean3x3) matches the full-array filter: {np.allclose(seamed_np, reference)}")
    print(f"  mismatched cells: {bad} of {reference.size} -- every one sits on an interior chunk edge")
    edge_col = int((~np.isclose(seamed_np[:, :, 31], reference[:, :, 31])).sum())
    print(f"  e.g. along the x=31|32 seam alone: {edge_col} wrong cells (blocks edge-padded with their own values)")

    # SECTION: map_overlap adds the halo
    print("\nmap_overlap(depth={y:1, x:1}) copies a 1-cell halo from each neighbour before the")
    print("function runs, then trims it from the output -- so every 3x3 window sees real data:")
    filtered = arr.map_overlap(mean3x3, depth={0: 0, 1: 1, 2: 1}, boundary="none")
    print(f"  lazy result: {chunk_report(filtered)}, tasks={task_count(filtered)}")
    print(f"  (vs {task_count(seamed)} for map_blocks: the extra tasks slice, exchange, and trim the halos)")
    print(f"  matches the full-array filter everywhere: {np.allclose(filtered.compute(), reference)}")
    print("  depth is per-axis: 0 along time (the filter is purely spatial), 1 along y and x;")
    print("  boundary='none' leaves the outer array edges to the function's own edge handling")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- map_blocks applies any numpy function independently to each chunk: one task per block")
    print("- It is block-local by construction -- fine for elementwise or per-block stats,")
    print("  wrong for anything that needs neighbours")
    print("- Stencils computed per block develop seams exactly at chunk boundaries")
    print("- map_overlap = map_blocks + halo exchange: depth cells are copied in, computed on,")
    print("  and trimmed out, making the chunked result identical to the whole-array result")
    print("- Halo width follows the kernel (3x3 -> depth 1); bigger stencils need deeper halos")


if __name__ == "__main__":
    main()
