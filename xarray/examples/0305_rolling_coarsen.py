"""Rolling and coarsen: window smoothing in time, pyramid downsampling in space.

What: smooths a daily temperature series with rolling(time=7).mean(), then
downsamples the spatial grid with coarsen(y=2, x=2).mean(), verifying that each
output cell is the mean of a 2x2 input block and that shapes halve per level.

Why: coarsen(y=2, x=2).mean() is exactly the operation open-climate-service
uses to build the multiscale levels of its GeoZarr pyramids -- each level is
the previous one mean-downsampled 2x. rolling is the same windowed idea along
time, used for smoothed series like 7-day means.

Run: make run EXAMPLE=0305_rolling_coarsen
"""

import xarray as xr

from climate_stack_xarray import temperature_dataset


def main() -> None:
    """Smooth with rolling windows and build coarsen-based pyramid levels."""
    # SECTION: rolling -- overlapping windows along time
    t2m = temperature_dataset(days=60)["t2m"]
    series = t2m.isel(y=10, x=15)  # one noisy grid cell, 60 days
    print("Daily temperature at one grid cell (60 days, noisy):")
    print(f"  shape={series.shape}, std={float(series.std()):.3f} degC")

    print("\nrolling(time=7).mean() slides an overlapping 7-day window over the series:")
    smooth = series.rolling(time=7).mean()
    print(f"  output shape={smooth.shape} (unchanged -- windows overlap, one output per input step)")
    print(f"  first 6 values are NaN (incomplete windows): NaN count = {int(smooth.isnull().sum())}")
    print(f"  smoothed std = {float(smooth.std()):.3f} degC -- day-to-day noise averaged away (7-day mean)")
    manual = float(series.isel(time=slice(0, 7)).mean())
    print(f"  check: mean(days 1..7) = {manual:.4f} vs rolling value at day 7 = {float(smooth[6]):.4f}")
    centered = series.rolling(time=7, center=True, min_periods=1).mean()
    print(f"  center=True + min_periods=1 -> label at window center, no NaN: count = {int(centered.isnull().sum())}")

    # SECTION: coarsen -- non-overlapping blocks in space
    field = temperature_dataset(days=10, ny=16, nx=32)["t2m"]
    print("\ncoarsen(y=2, x=2).mean() averages NON-overlapping 2x2 blocks: the pyramid downsampler.")
    print(f"  level 0 (native): shape={field.shape}")

    # SECTION: shape halving across pyramid levels
    # Coarsen reduction methods are injected at runtime, so type checkers cannot see .mean().
    level1: xr.DataArray = field.coarsen(y=2, x=2).mean()  # type: ignore[attr-defined]
    level2: xr.DataArray = level1.coarsen(y=2, x=2).mean()  # type: ignore[attr-defined]
    print("\nEach level halves y and x (time untouched) -- OCS GeoZarr multiscale levels:")
    print(f"  level 1: shape={level1.shape}  (16x32 -> 8x16, 4x fewer pixels)")
    print(f"  level 2: shape={level2.shape}  (8x16 -> 4x8, 16x fewer than native)")

    # SECTION: value averaging, verified cell by cell
    print("\nEach coarse cell is the mean of its 2x2 source block (identical up to float round-off):")
    block = field.isel(time=0, y=slice(0, 2), x=slice(0, 2))
    manual_block = float(block.mean())
    coarse_cell = float(level1.isel(time=0, y=0, x=0))
    print(f"  source block values: {[round(float(v), 3) for v in block.values.ravel()]}")
    print(
        f"  block mean = {manual_block:.6f}, level-1 cell [0, 0] = {coarse_cell:.6f}, "
        f"abs diff = {abs(manual_block - coarse_cell):.1e}"
    )
    print("  coords are coarsened too -- new cell centers are block means of the old ones:")
    print(f"  y[0:2] = {[round(float(v), 3) for v in field.y.values[:2]]} -> level-1 y[0] = {float(level1.y[0]):.3f}")
    print(
        f"  means preserved across levels: {float(field.mean()):.4f} -> {float(level1.mean()):.4f} "
        f"-> {float(level2.mean()):.4f}"
    )

    # SECTION: summary
    print("\n=== Summary ===")
    print("- rolling = overlapping windows along a dim; same length out, NaN until a full window")
    print("- rolling(time=7).mean() is the standard smoothed-series recipe (see center=, min_periods=)")
    print("- coarsen = non-overlapping blocks; coarsen(y=2, x=2).mean() halves each spatial dim")
    print("- each coarse value is exactly the mean of its source block; coords coarsen along with data")
    print("- repeated coarsen calls are precisely how OCS builds GeoZarr pyramid levels")


if __name__ == "__main__":
    main()
