"""GeoZarr root attributes: putting a grid of numbers somewhere on Earth.

What: builds the GeoZarr root attributes for an ingested store, takes every key
apart -- the affine transform, the array-order dimensions and shape, the bbox,
the CRS -- verifies the pixel-registration offset numerically against the
coordinate arrays, then writes the attributes onto the zarr group root and
reads them back to prove they persist.

Why: a zarr store on its own is just numbers in a grid; nothing in it says
where on Earth those numbers are, which way is up, or in what CRS. GeoZarr is
the small set of root attributes that answers all three, and it is what
open-climate-service writes so that a tile server can render a store it has
never seen before. The axis-order trap here is real: naming the dimensions
x-first transposes every tile that is ever rendered from the store.

Run: make run EXAMPLE=0301_geozarr
"""

import tempfile
from typing import Any

import icechunk
import numpy as np
import xarray as xr
import zarr

from playground_climate_pipeline import (
    bounding_box,
    enumerate_periods,
    fetch_temperature,
    geozarr_attrs,
    grid_transform,
    ingest,
    store_path,
)

# icechunk is a Rust extension without type stubs, so its objects stay Any.
icechunk.set_logs_filter("error")

# A deliberately non-square grid: on a square grid an axis-order bug is
# invisible, because the wrong shape happens to still fit.
NY = 12
NX = 16


def build_store(tmpdir: str) -> tuple[Any, xr.Dataset]:
    """Ingest two months of temperature and return the repo and its dataset.

    Args:
        tmpdir: Directory to hold the icechunk store.

    Returns:
        The icechunk repository and the dataset read back from it.
    """
    path = store_path(tmpdir, "temperature")
    path.parent.mkdir(parents=True, exist_ok=True)
    repo: Any = icechunk.Repository.create(icechunk.local_filesystem_storage(str(path)))
    ingest(repo, enumerate_periods(2024, 2), lambda p: fetch_temperature(p, ny=NY, nx=NX))
    return repo, xr.open_zarr(repo.readonly_session("main").store, consolidated=False)


def main() -> None:
    """Derive, explain, verify, and persist the GeoZarr root attributes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # SECTION: a store with no idea where it is
        print("A zarr store is an array of numbers in chunks. It is not a map.")
        repo, ds = build_store(tmpdir)
        print(f"  stored variable: t2m{tuple(ds['t2m'].dims)} shape {tuple(ds['t2m'].shape)}")
        print(f"  root attrs on the freshly written store: {dict(ds.attrs)}")
        print("  Nothing there says which CRS, which way is up, or where cell [0, 0] sits.")
        print("  The y/x coordinate arrays hold degrees, but a tile server rendering a")
        print("  chunk will not read a coordinate array to work out geography -- it wants")
        print("  one affine transform and one CRS, declared at the root.")

        attrs = geozarr_attrs(ds)
        print("\ngeozarr_attrs(ds) returns exactly that, in six root keys:")
        for key, value in attrs.items():
            print(f"  {key:22} = {value}")

        # SECTION: the affine transform, coefficient by coefficient
        print("\n-- spatial:transform --")
        step_x, rot_x, origin_x, rot_y, step_y, origin_y = grid_transform(ds)
        print("  Order is [stepX, rotX, originX, rotY, stepY, originY]:")
        print(f"    stepX   = {step_x:+.6f}  degrees of longitude per column")
        print(f"    rotX    = {rot_x:+.6f}  row rotation, zero for an axis-aligned grid")
        print(f"    originX = {origin_x:+.6f}  west edge of column 0")
        print(f"    rotY    = {rot_y:+.6f}  column rotation, zero here too")
        print(f"    stepY   = {step_y:+.6f}  degrees of latitude per row -- NEGATIVE")
        print(f"    originY = {origin_y:+.6f}  north edge of row 0")

        # SECTION: pixel registration, verified against the coordinates
        print("\n-- pixel registration, checked against the coordinate arrays --")
        x = np.asarray(ds["x"].values, dtype="float64")
        y = np.asarray(ds["y"].values, dtype="float64")
        print("  The coordinates are cell CENTRES. The transform origin is a cell EDGE:")
        print(f"    x[0] (centre of column 0) = {x[0]:+.6f}")
        print(f"    origin_x                  = {origin_x:+.6f}")
        print(f"    x[0] - stepX / 2          = {x[0] - step_x / 2:+.6f}  <- matches origin_x")
        assert np.isclose(origin_x, x[0] - step_x / 2)
        assert np.isclose(origin_y, y[0] - step_y / 2)
        print(f"  The offset is exactly half a cell: {abs(origin_x - x[0]):.6f} = stepX/2 = {abs(step_x) / 2:.6f}")
        far_x = origin_x + step_x * ds.sizes["x"]
        print(f"  Walking the full width lands on the far edge: origin_x + stepX*nx = {far_x:+.6f}")
        print(f"  which is half a cell past the last centre x[-1] = {x[-1]:+.6f}.")
        assert np.isclose(far_x, x[-1] + step_x / 2)
        print("  Treat the origin as a centre instead and every rendered tile shifts by")
        print("  half a pixel -- small enough to ship, big enough to misplace a clinic.")

        # SECTION: why stepY is negative
        print("\n-- why stepY is negative --")
        print(f"  y[0] = {y[0]:+.6f} (north)   y[-1] = {y[-1]:+.6f} (south)")
        print("  Row 0 is the NORTHERNMOST row, so walking down rows walks south, so the")
        print("  y step is negative. That is the north-up convention every raster renderer")
        print("  assumes. A positive stepY on this store would draw the country upside down.")

        # SECTION: array order, and what transposing costs
        print("\n-- spatial:dimensions and spatial:shape are in ARRAY order (y, x) --")
        print(f"  spatial:dimensions = {attrs['spatial:dimensions']}")
        print(f"  spatial:shape      = {attrs['spatial:shape']}  (ny={NY} rows, nx={NX} columns)")
        print("  A client reads these positionally: first entry indexes the slow axis.")
        plane = np.asarray(ds["t2m"].isel(time=0).values, dtype="float64")
        wrong = plane.ravel().reshape(NX, NY)
        row, col = 3, 5
        flat_index = row * NX + col
        print(f"  Correct read, shape {plane.shape}: cell [{row}, {col}] = {plane[row, col]:.3f} degC")
        print(f"  x-first read, shape {wrong.shape}: cell [{row}, {col}] = {wrong[row, col]:.3f} degC")
        print(f"  The real cell [{row}, {col}] is element {flat_index} of the buffer, which the")
        print(f"  x-first reader places at [{flat_index // NY}, {flat_index % NY}] instead.")
        print("  Same bytes, same chunks, different geography: only the stride the client")
        print("  walks the buffer with changed, and every pixel landed somewhere else.")
        print(f"  On a square grid ({NY}x{NY}) the transposed shape still fits, so the bug")
        print("  renders silently -- it survives until someone compares a tile to a map.")

        # SECTION: the bounding box
        print("\n-- spatial:bbox --")
        west, south, east, north = bounding_box(ds)
        print(f"  [west, south, east, north] = [{west:.4f}, {south:.4f}, {east:.4f}, {north:.4f}]")
        print("  These are OUTER edges derived from the transform, not centres, so the box")
        print("  is exactly the ground the raster covers. STAC reuses the same numbers, so")
        print("  a catalogue search and a rendered tile agree about the footprint.")

        # SECTION: the CRS
        print("\n-- proj:code --")
        print(f"  {attrs['proj:code']} -- degrees of latitude and longitude on WGS 84.")
        print(f"  Without it, the transform is unitless: {step_x:.2f} of what, measured from where?")

        # SECTION: writing the attributes onto the store root
        print("\n-- persisting the attributes onto the zarr group root --")
        session: Any = repo.writable_session("main")
        group = zarr.open_group(session.store, mode="a")
        group.attrs.update(attrs)
        session.commit("add geozarr root attributes")
        print(f"  wrote {len(attrs)} keys onto the root group and committed them")

        reopened = zarr.open_group(repo.readonly_session("main").store, mode="r")
        # zarr types attribute values as a JSON union; narrow to Any to format them.
        persisted: dict[str, Any] = dict(reopened.attrs)
        print(f"  read back from a fresh session: {len(persisted)} keys")
        print(f"    spatial:shape     -> {persisted['spatial:shape']}")
        print(f"    spatial:transform -> [{', '.join(f'{v:.4f}' for v in persisted['spatial:transform'])}]")
        print(f"    proj:code         -> {persisted['proj:code']}")
        assert persisted["spatial:shape"] == attrs["spatial:shape"]
        assert persisted["proj:code"] == attrs["proj:code"]
        after = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
        print(f"  xarray surfaces the same keys as ds.attrs: {sorted(after.attrs)[:3]} ...")

        # SECTION: summary
        print("\n=== Summary ===")
        print("- A plain zarr store carries no geography; GeoZarr adds it at the root")
        print("- spatial:transform is [stepX, rotX, originX, rotY, stepY, originY]")
        print("- The origin is pixel-registered: half a cell outside the first centre")
        print("- stepY is negative because row 0 is the northernmost row (north-up)")
        print("- spatial:dimensions and spatial:shape are array order (y, x); naming them")
        print("  x-first transposes every tile rendered from the store")
        print("- spatial:bbox is outer edges, shared verbatim with the STAC extent")
        print("- proj:code is what makes the other numbers mean degrees on WGS 84")
        print("- The attributes live in a commit like any other change to the store")


if __name__ == "__main__":
    main()
