"""Pyramid: multiscale levels by 2x2 coarsening, and why a map viewer needs them.

What: ingests a couple of months, builds the pyramid with pyramid_levels(),
prints each level's dims and size in KB, and checks by hand that a coarse cell
is exactly the mean of its four parents one level down.

Why: a map viewer that renders a zoomed-out country outline must not download
the full-resolution grid to draw it. GeoZarr's multiscale convention -- the one
open-climate-service writes -- stores the same field at halving resolutions so
the client fetches the level that matches the zoom.

Run: make run EXAMPLE=0203_pyramid
"""

import tempfile
from typing import Any

import icechunk  # no type stubs; repository handles stay narrowly typed as Any
import xarray as xr

from playground_climate import enumerate_periods, fetch_temperature, ingest, pyramid_levels, store_path

# Silence the Rust-side WARN chatter icechunk emits during ordinary appends.
icechunk.set_logs_filter("error")

GRID = 16
LEVELS = 4


def make_repo(tmp: str, dataset_id: str) -> Any:
    """Create a fresh icechunk repository under the OCS store layout.

    Args:
        tmp: Temporary data directory standing in for an instance data dir.
        dataset_id: Public identifier of the dataset.

    Returns:
        A new icechunk repository.
    """
    path = store_path(tmp, dataset_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    return icechunk.Repository.create(icechunk.local_filesystem_storage(str(path)))


def kilobytes(ds: xr.Dataset, variable: str = "t2m") -> float:
    """Return the size of one variable's array in kibibytes.

    Args:
        ds: The dataset holding the variable.
        variable: Which variable to measure.

    Returns:
        Size in KB, counting values only, not metadata.
    """
    return float(ds[variable].size * ds[variable].dtype.itemsize) / 1024.0


def main() -> None:
    """Build a multiscale pyramid over an ingested dataset and verify the coarsening."""
    with tempfile.TemporaryDirectory() as tmp:
        repo = make_repo(tmp, "temperature")

        # SECTION: ingest a base level
        print(f"Ingest two months onto a {GRID}x{GRID} grid. That grid is level 0 of the")
        print("pyramid: the full-resolution field, exactly as stored.\n")
        ingest(repo, enumerate_periods(2024, 2), lambda p: fetch_temperature(p, ny=GRID, nx=GRID))
        base: xr.Dataset = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
        print(f"  level 0: dims={dict(base.sizes)} units={base['t2m'].attrs['units']!r}")

        # SECTION: the pyramid
        print(f"\npyramid_levels(ds, levels={LEVELS}) coarsens by 2x2 means, repeatedly. Each")
        print("level halves y and x, so its array is a quarter the size of the one before:\n")
        levels = pyramid_levels(base, levels=LEVELS)
        print(f"  {'level':>5}  {'y':>4}  {'x':>4}  {'cells':>8}  {'size KB':>9}  {'vs level 0':>10}")
        base_kb = kilobytes(levels[0])
        for depth, level in enumerate(levels):
            size = kilobytes(level)
            cells = int(level.sizes["y"] * level.sizes["x"])
            print(
                f"  {depth:>5}  {int(level.sizes['y']):>4}  {int(level.sizes['x']):>4}  "
                f"{cells:>8}  {size:>9.1f}  {size / base_kb:>9.3f}x"
            )
        total_kb = sum(kilobytes(level) for level in levels)
        print(f"\n  time is untouched: {int(levels[-1].sizes['time'])} steps at every level -- only space coarsens.")
        print(f"  whole pyramid: {total_kb:.1f} KB against {base_kb:.1f} KB for level 0 alone,")
        print(f"  an overhead of {(total_kb / base_kb - 1) * 100:.0f} percent. The series 1 + 1/4 + 1/16 + ...")
        print("  converges to 4/3, so a full pyramid never costs more than a third extra.")

        # SECTION: the coordinates move too
        print("\nThe coordinates coarsen with the data -- a coarse cell centre sits between")
        print("the fine centres it covers, which is what keeps the grid geographically true:\n")
        fine_y = [float(v) for v in levels[0]["y"].values[:4]]
        coarse_y = [float(v) for v in levels[1]["y"].values[:2]]
        print(f"  level 0 first y centres: {[round(v, 4) for v in fine_y]}")
        print(f"  level 1 first y centres: {[round(v, 4) for v in coarse_y]}")
        print(f"  mean of the first pair : {round((fine_y[0] + fine_y[1]) / 2, 4)}")
        print(f"  level 0 cell height: {abs(fine_y[1] - fine_y[0]):.4f} deg")
        print(f"  level 1 cell height: {abs(coarse_y[1] - coarse_y[0]):.4f} deg  (twice as tall)")

        # SECTION: verify the arithmetic
        print("\nVerify the reduction by hand: one level-1 cell should equal the mean of its")
        print("2x2 block at level 0. Checking the top-left corner on the first day:\n")
        block = levels[0]["t2m"].isel(time=0, y=slice(0, 2), x=slice(0, 2))
        expected = float(block.mean())
        actual = float(levels[1]["t2m"].isel(time=0, y=0, x=0))
        values = [round(float(v), 4) for v in block.values.ravel()]
        print(f"  level 0 block values : {values}")
        print(f"  their mean           : {expected:.6f}")
        print(f"  level 1 cell value   : {actual:.6f}")
        print(f"  equal to 1e-12       : {abs(expected - actual) < 1e-12}")

        deeper = levels[0]["t2m"].isel(time=0, y=slice(0, 4), x=slice(0, 4))
        level_two = float(levels[2]["t2m"].isel(time=0, y=0, x=0))
        print("\n  and two levels down, one cell covers a 4x4 block of level 0:")
        print(f"  mean of the 4x4 block: {float(deeper.mean()):.6f}")
        print(f"  level 2 cell value   : {level_two:.6f}")
        print(f"  equal to 1e-12       : {abs(float(deeper.mean()) - level_two) < 1e-12}")
        print("  A mean of means is only the mean of the whole when the blocks are equal")
        print("  in size, which they are here. On a grid with an odd dimension the last")
        print("  row is trimmed instead of being folded in at half weight:")
        odd = pyramid_levels(base.isel(y=slice(0, GRID - 1)), levels=2)
        print(f"  pyramid over y={int(odd[0].sizes['y'])}: next level is y={int(odd[1].sizes['y'])} (boundary='trim')")

        # SECTION: why a viewer needs this
        print("\nWhy a map viewer cannot do without it:")
        print("  A viewer showing the whole country in a 512-pixel-wide panel has no use")
        print("  for a 4096-wide grid: 63 of every 64 values it downloads get averaged")
        print("  away before a pixel is painted. Reading level 3 instead moves 1/64 of")
        print("  the bytes and produces the same picture.")
        print("  Doing the averaging server-side per request costs the same read every")
        print("  time and does not cache. Precomputing it once, at ingest, turns a")
        print("  zoomed-out map into a small static read -- which is why every tiled map")
        print("  system, from raster tiles to COG overviews, is built this way.")
        for depth, level in enumerate(levels):
            width, height = int(level.sizes["x"]), int(level.sizes["y"])
            print(f"  level {depth}: grid {width}x{height} -- serves any view drawn at {width} pixels or fewer")

        # SECTION: the GeoZarr shape
        print("\nThis is exactly the GeoZarr multiscale convention open-climate-service")
        print("writes. In the store the levels become sibling groups under one dataset:")
        print("  temperature.icechunk/")
        print("    0/  t2m  (time, y, x)   full resolution")
        print("    1/  t2m  (time, y/2, x/2)")
        print("    2/  t2m  (time, y/4, x/4)")
        print("  with a 'multiscales' attribute on the root naming the levels and the")
        print("  factor between them, so a client can pick a level without guessing.")
        print("  pyramid_levels() computes the arrays; example 0301 writes the attributes")
        print("  that make them a valid GeoZarr.")

        # SECTION: summary
        print("\n=== Summary ===")
        print("- pyramid_levels() repeatedly coarsens y and x by 2x2 means; level 0 is the stored grid")
        print("- Each level is a quarter of the previous one, and the whole pyramid costs about 33 percent extra")
        print("- Coordinates coarsen with the data, so a coarse cell centre lands between its parents")
        print("- A coarse cell equals the mean of its 2x2 block exactly; odd dimensions are trimmed, not folded")
        print("- Time is never coarsened here -- only the spatial dims, because zoom is a spatial question")
        print("- A zoomed-out tile reads a small array instead of the full grid: the GeoZarr multiscale convention")


if __name__ == "__main__":
    main()
