"""The whole pipeline: raw source to published product in one run.

What: runs every stage of the miniature climate service over temperature --
enumerate periods, inspect a raw source, ingest twelve months one commit at a
time, read the commit history back, derive a climatology and a hot-days index,
build a multiscale pyramid, and publish GeoZarr attributes plus a STAC
collection -- printing the evidence at each step.

Why: this is the shape of open-climate-service. Every earlier example took one
stage apart on its own; the point of the service is that the stages compose --
messy source in one end, a discoverable, placed, versioned product out the
other, with nothing hand-maintained in between. This example is the tour.

Run: make run EXAMPLE=0401_full_pipeline
"""

import tempfile
from datetime import UTC, datetime
from typing import Any

import icechunk
import numpy as np
import pandas as pd
import xarray as xr
import zarr

from playground_domain_climate import (
    bounding_box,
    climatological_normal,
    enumerate_periods,
    fetch_temperature,
    geozarr_attrs,
    hot_days,
    ingest,
    pyramid_levels,
    stac_collection,
    store_path,
    temporal_extent,
)

# icechunk is a Rust extension without type stubs, so its objects stay Any.
icechunk.set_logs_filter("error")

DATASET_ID = "temperature-daily-sle"
MONTHS = 12
NY = 16
NX = 16
HOT_THRESHOLD = 30.0
PUBLISHED_AT = datetime(2025, 1, 15, 9, 0, tzinfo=UTC)


def stage(number: int, title: str) -> None:
    """Print a stage banner so the tour is readable end to end.

    Args:
        number: Stage number, counting from one.
        title: Short description of what the stage does.
    """
    print(f"\n{'=' * 74}")
    print(f"STAGE {number} -- {title}")
    print("=" * 74)


def main() -> None:
    """Run source, ingest, derive, and publish over one dataset in a single pass."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # SECTION: stage 1 -- what the source hands over
        stage(1, "the source: periods on offer, and what one of them looks like")
        print("A source does not hand over a dataset; it hands over a list of periods it")
        print("can supply, and the framework fetches them one at a time.")
        periods = enumerate_periods(2024, MONTHS)
        print(f"  enumerate_periods(2024, {MONTHS}) -> {len(periods)} periods")
        print(f"  first: {periods[0].period_id} starting {periods[0].start}, {periods[0].days} days")
        print(f"  leap February: {periods[1].period_id} has {periods[1].days} days")
        print(f"  last:  {periods[-1].period_id} starting {periods[-1].start}, {periods[-1].days} days")
        print(f"  total days on offer: {sum(p.days for p in periods)}")

        print("\nOne raw period, in the source's own awkward conventions:")
        raw = fetch_temperature(periods[0], ny=NY, nx=NX)
        raw_lat = np.asarray(raw["lat"].values, dtype="float64")
        print(f"  variable   : t2m{tuple(raw['t2m'].dims)}  <- lat/lon, not y/x")
        print(f"  units      : {raw['t2m'].attrs['units']}  (mean {float(raw['t2m'].mean()):.2f})")
        print(f"  lat[0]={raw_lat[0]:.3f} .. lat[-1]={raw_lat[-1]:.3f}  <- ASCENDING: south-up")
        print("  Ingest normalizes all three at once: renamed to (time, y, x), Kelvin to")
        print("  degC, and flipped north-up, so everything downstream sees one convention.")

        # SECTION: stage 2 -- streaming ingest
        stage(2, "ingest: twelve periods, one commit each")
        print("Each period is fetched, normalized, appended, and committed on its own.")
        print("A crash therefore leaves a store complete up to the last commit, never")
        print("half a month, and resume is a matter of asking the store what it holds.")
        path = store_path(tmpdir, DATASET_ID)
        path.parent.mkdir(parents=True, exist_ok=True)
        repo: Any = icechunk.Repository.create(icechunk.local_filesystem_storage(str(path)))
        report = ingest(repo, periods, lambda p: fetch_temperature(p, ny=NY, nx=NX))
        print(f"  ingested : {len(report.ingested)} periods -> {report.ingested[0]} .. {report.ingested[-1]}")
        print(f"  skipped  : {len(report.skipped)}   failed: {len(report.failed)}")

        ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
        stamps = pd.DatetimeIndex(ds["time"].values)
        print(f"  store    : {dict(ds.sizes)}, variable t2m in {ds['t2m'].attrs['units']}")
        print(f"  time span: {stamps[0].date()} .. {stamps[-1].date()} ({len(stamps)} days, contiguous)")
        print(f"  monotonic increasing: {stamps.is_monotonic_increasing}, unique: {stamps.is_unique}")
        print(f"  chunks   : {ds['t2m'].chunksizes['time'][:3]}... days per time chunk")

        # SECTION: stage 3 -- the commit history
        stage(3, "history: one snapshot per period, kept forever")
        print("The store is versioned, so the ingest left a readable audit trail. Each")
        print("snapshot is the store as it stood after exactly one month landed.")
        history: list[Any] = list(repo.ancestry(branch="main"))
        print(f"  {len(history)} snapshots = 1 repository init + {MONTHS} ingests")
        for snapshot in reversed(history[-4:]):
            print(f"    {str(snapshot.id)[:10]}  {snapshot.message}")
        print("    ...")
        for snapshot in reversed(history[:2]):
            print(f"    {str(snapshot.id)[:10]}  {snapshot.message}")
        first_month = xr.open_zarr(repo.readonly_session(snapshot_id=history[-2].id).store, consolidated=False)
        print(f"  reading the oldest ingest snapshot back: {first_month.sizes['time']} days")
        print(f"  reading the branch tip:                  {ds.sizes['time']} days")
        print("  Same store, two points in its history -- useful when a published figure")
        print("  has to be reproduced months after the series it came from grew.")

        # SECTION: stage 4 -- derived products
        stage(4, "derive: a climatology and a hot-days index")
        print("Raw temperature is an input, not an answer. The normal says what a month")
        print("usually looks like; the index answers a question someone actually asked.")
        normal = climatological_normal(ds)
        area_mean = normal.mean(dim=["y", "x"])
        print(f"  climatological_normal(ds) -> {tuple(normal.dims)} {tuple(normal.shape)}")
        print("  area-mean normal, degC by month:")
        months = [int(m) for m in np.asarray(normal["month"].values)]
        values = [float(v) for v in np.asarray(area_mean.values, dtype="float64")]
        print("    month " + " ".join(f"{m:>5d}" for m in months))
        print("    degC  " + " ".join(f"{v:>5.1f}" for v in values))

        index = hot_days(ds, threshold=HOT_THRESHOLD)
        counts = np.asarray(index.values, dtype="float64")
        print(f"\n  hot_days(ds, threshold={HOT_THRESHOLD}) -> {tuple(index.dims)} {tuple(index.shape)}")
        print(f"  units {index.attrs['units']}, {index.attrs['long_name']}")
        per_month = counts.mean(axis=(1, 2))
        print("  mean hot days per cell, by month:")
        print("    month " + " ".join(f"{m:>5d}" for m in months))
        print("    days  " + " ".join(f"{v:>5.1f}" for v in per_month))
        hottest = int(np.argmax(per_month))
        print(f"  hottest month: {months[hottest]:02d} at {per_month[hottest]:.1f} days per cell")
        print(f"  range across the whole grid and year: {int(counts.min())} .. {int(counts.max())} days")

        # SECTION: stage 5 -- the pyramid
        stage(5, "pyramid: coarser levels so a zoomed-out map is cheap")
        print("A viewer showing the whole country does not want every cell. Each level")
        print("halves both spatial dims by 2x2 averaging, so a wide view reads a small")
        print("array instead of the full grid.")
        levels = pyramid_levels(ds, levels=4)
        print(f"  {len(levels)} levels:")
        full_cells = levels[0].sizes["y"] * levels[0].sizes["x"]
        for level, part in enumerate(levels):
            cells = part.sizes["y"] * part.sizes["x"]
            print(
                f"    level {level}: {part.sizes['y']:2d} x {part.sizes['x']:2d} = {cells:4d} cells per "
                f"timestep ({cells / full_cells:5.1%} of level 0)"
            )
        print(f"  level 0 area mean {float(levels[0]['t2m'].mean()):.4f} degC")
        print(f"  level {len(levels) - 1} area mean {float(levels[-1]['t2m'].mean()):.4f} degC")
        print("  Coarsening is a mean, so the field's average survives it; what is lost is")
        print("  detail, which is exactly what a zoomed-out tile cannot show anyway.")

        # SECTION: stage 6 -- publish
        stage(6, "publish: GeoZarr attributes and a STAC collection")
        print("Two documents turn a store into a product: root attributes that place the")
        print("grid on Earth, and a collection document a client can discover it through.")
        attrs = geozarr_attrs(ds)
        session: Any = repo.writable_session("main")
        zarr.open_group(session.store, mode="a").attrs.update(attrs)
        session.commit("publish geozarr root attributes")
        print(f"  wrote {len(attrs)} GeoZarr keys to the root and committed them:")
        print(f"    spatial:transform  = [{', '.join(f'{v:.4f}' for v in attrs['spatial:transform'])}]")
        print(f"    spatial:dimensions = {attrs['spatial:dimensions']} (array order: y is the slow axis)")
        print(f"    spatial:shape      = {attrs['spatial:shape']}")
        print(f"    proj:code          = {attrs['proj:code']}")

        collection = stac_collection(
            ds,
            DATASET_ID,
            title="Daily 2m temperature, Sierra Leone",
            description="Daily mean 2 metre air temperature, normalized to degC on a north-up WGS 84 grid.",
            zarr_href=f"https://climate.example.org/zarr/{DATASET_ID}",
            now=PUBLISHED_AT,
        )
        summary = collection["summaries"]["variables"]["t2m"]
        print(f"\n  STAC collection id {collection['id']!r}:")
        print(f"    extent.spatial.bbox      = {[round(v, 4) for v in collection['extent']['spatial']['bbox'][0]]}")
        print(f"    extent.temporal.interval = {collection['extent']['temporal']['interval'][0]}")
        print(f"    summaries.variables.t2m  = {summary['units']}, {summary['min']} .. {summary['max']}")
        print(f"    assets.zarr.href         = {collection['assets']['zarr']['href']}")
        print("  Both extents are derived from the committed data, so publishing again")
        print("  after next month's ingest needs no edits anywhere.")

        # SECTION: summary
        published_history: list[Any] = list(repo.ancestry(branch="main"))
        west, south, east, north = bounding_box(ds)
        interval = temporal_extent(ds)
        print("\n=== Summary ===")
        rows: list[tuple[str, str]] = [
            ("source periods", f"{len(periods)} months of 2024"),
            ("periods ingested", f"{len(report.ingested)} (failed: {len(report.failed)})"),
            ("days stored", f"{ds.sizes['time']}"),
            ("grid", f"{ds.sizes['y']} x {ds.sizes['x']} cells, north-up, {attrs['proj:code']}"),
            ("snapshots", f"{len(published_history)} (1 init + {MONTHS} ingests + 1 publish)"),
            ("climatology shape", f"{tuple(normal.dims)} {tuple(normal.shape)}"),
            ("index shape", f"{tuple(index.dims)} {tuple(index.shape)} of hot days"),
            ("pyramid levels", " -> ".join(f"{level.sizes['y']}x{level.sizes['x']}" for level in levels)),
            ("bbox", f"[{west:.3f}, {south:.3f}, {east:.3f}, {north:.3f}]"),
            ("time interval", f"{interval[0]} .. {interval[1]}"),
            ("stac collection", f"{collection['id']} (1 asset, 1 variable)"),
        ]
        for label, value in rows:
            print(f"  {label:<18} {value}")
        print("\n- Messy source in, discoverable placed versioned product out")
        print("- Every extent published above was read back off the store, never declared")
        print("- The store is the record: history, resume, and reproduction all read from it")


if __name__ == "__main__":
    main()
