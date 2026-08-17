"""Nearest-neighbour selection, tolerance, and interpolating to a new grid.

What: selects off-grid coordinates with sel(method="nearest"), bounds the
allowed snap distance with tolerance (catching the KeyError when it is
exceeded), regrids to a finer coordinate grid, and shows what interp() computes.

Why: open-climate-service constantly reconciles grids that almost line up —
a request for lon=-12.0 must snap to the nearest stored x column, but only
within a sensible distance, and serving a different resolution means mapping
values onto a new (y, x) grid. nearest/tolerance/interp are those exact moves.

Run: make run EXAMPLE=0202_nearest_and_interp
"""

import numpy as np

from playground_data_xarray import temperature_dataset


def main() -> None:
    """Snap to nearest labels, enforce a tolerance, and regrid coordinates."""
    # SECTION: a grid whose labels are awkward floats
    print("Temperature on the default grid -- coordinate labels are ugly floats:")
    ds = temperature_dataset(days=5)
    x_step = float(ds.x.values[1] - ds.x.values[0])
    print(f"  sizes: {dict(ds.sizes)}")
    print(f"  x: {float(ds.x.values[0]):.4f} .. {float(ds.x.values[-1]):.4f}, step ~{x_step:.4f} deg")

    # SECTION: exact sel fails off-grid
    print("\nExact label selection at x=-12.0 (not a stored label) fails:")
    try:
        ds.sel(x=-12.0)
    except KeyError as err:
        print(f"  KeyError: {str(err)[:70]}...")
    print("  float grids almost never contain the exact value a caller asks for")

    # SECTION: method="nearest"
    print('\nsel(method="nearest") snaps to the closest stored label:')
    col = ds.sel(x=-12.0, method="nearest")
    off_by = abs(float(col.x) + 12.0)
    print(f"  ds.sel(x=-12.0, method='nearest') -> chose label x={float(col.x):.4f} (off by {off_by:.4f})")
    point = ds.sel(y=8.0, x=-12.0, method="nearest")
    print(f"  works per-dim: y=8.0, x=-12.0 snapped to (y={float(point.y):.4f}, x={float(point.x):.4f})")

    # SECTION: tolerance bounds the snap distance
    print("\ntolerance= caps how far nearest may snap -- beyond it, KeyError:")
    ok = ds.sel(x=-12.0, method="nearest", tolerance=0.2)
    print(f"  tolerance=0.2 (> half a cell): ok, x={float(ok.x):.4f}")
    try:
        ds.sel(x=-12.0, method="nearest", tolerance=0.01)
    except KeyError as err:
        print(f"  tolerance=0.01 (< distance to any label): KeyError: {str(err)[:60]}")
    print("  use it to reject requests that fall outside the store instead of silently snapping far away")

    # SECTION: nearest onto a whole new grid
    print("\nPassing an ARRAY of targets regrids by nearest neighbour (values are copied, not blended):")
    new_x = np.round(np.linspace(-13.5, -10.3, 9), 2)
    coarse = ds.sel(x=new_x, method="nearest")
    print(f"  ds.sel(x=<9 targets>, method='nearest') -> x size {ds.sizes['x']} -> {coarse.sizes['x']}")
    print(f"  requested: {new_x[:4].tolist()} ...")
    print(f"  snapped:   {[round(float(v), 2) for v in coarse.x.values[:4]]} ... (nearest stored labels)")

    # SECTION: interp() blends values onto a new grid
    print("\ninterp() computes NEW values on the target grid instead of copying the nearest cell:")
    fine_x = ds.x.values[:2].mean()  # midpoint between the first two columns
    try:
        mid = ds.interp(x=[fine_x], method="linear")
        v = float(mid.t2m.isel(time=0, y=0, x=0))
        print(f"  ds.interp(x=[{fine_x:.4f}]) -> t2m at the midpoint = {v:.3f}")
    except (ImportError, ModuleNotFoundError):
        print("  (xarray delegates interp() to scipy, which is not installed in this project --")
        print("   the call is ds.interp(x=new_x, method='linear'); here is the same math via numpy)")
    left = float(ds.t2m.isel(time=0, y=0, x=0))
    right = float(ds.t2m.isel(time=0, y=0, x=1))
    manual = float(np.interp(fine_x, ds.x.values[:2], [left, right]))
    print(f"  neighbours at x[0], x[1]: {left:.3f}, {right:.3f}")
    print(f"  linear value at their midpoint x={float(fine_x):.4f}: {manual:.3f}  (exactly the average)")
    print("  nearest would have returned one of the neighbours unchanged; interp blends by distance")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- exact sel on float grids fails; method='nearest' snaps to the closest label")
    print("- tolerance= turns 'snap anywhere' into 'snap within this distance or KeyError'")
    print("- array targets + nearest = regridding by copying closest cells")
    print("- interp() = regridding by blending neighbours (linear by default; needs scipy)")
    print("- OCS pattern: nearest+tolerance for point lookups, interp for resolution changes")


if __name__ == "__main__":
    main()
