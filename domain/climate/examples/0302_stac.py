"""The STAC collection: how a client discovers a dataset it has never seen.

What: ingests half a year of temperature, builds a STAC Collection over the
resulting store, pretty-prints the document, and walks it field by field --
id, spatial extent, temporal interval, variable summaries, and the zarr asset
-- checking that the extents actually match the data they claim to describe.

Why: storing data is not publishing it. open-climate-service serves one of
these documents per dataset at /stac/collections/{id}, and that document is the
whole contract: a client reads it to learn what an instance holds, over what
ground and what years, in what units, and where to open the store -- without
knowing anything about icechunk, chunk sizes, or the ingest that built it.

Run: make run EXAMPLE=0302_stac
"""

import json
import tempfile
from datetime import UTC, datetime
from typing import Any

import icechunk
import pandas as pd
import xarray as xr

from playground_domain_climate import (
    enumerate_periods,
    fetch_temperature,
    geozarr_attrs,
    ingest,
    stac_collection,
    store_path,
    temporal_extent,
)

# icechunk is a Rust extension without type stubs, so its objects stay Any.
icechunk.set_logs_filter("error")

# Pinned so the document is byte-identical on every run.
PUBLISHED_AT = datetime(2024, 7, 1, 12, 0, tzinfo=UTC)
DATASET_ID = "temperature-daily-sle"
ZARR_HREF = "https://climate.example.org/zarr/temperature-daily-sle"


def print_json(document: dict[str, Any], max_lines: int = 60) -> None:
    """Pretty-print a JSON document, trimming it if it runs long.

    Args:
        document: Any JSON-serializable mapping.
        max_lines: How many lines to show before truncating.
    """
    lines = json.dumps(document, indent=2).splitlines()
    for line in lines[:max_lines]:
        print(f"  {line}")
    if len(lines) > max_lines:
        print(f"  ... ({len(lines) - max_lines} more lines)")


def main() -> None:
    """Publish a STAC collection over an ingested store and verify its claims."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # SECTION: something to publish
        print("A STAC collection describes a real store, so first there has to be one.")
        path = store_path(tmpdir, DATASET_ID)
        path.parent.mkdir(parents=True, exist_ok=True)
        repo: Any = icechunk.Repository.create(icechunk.local_filesystem_storage(str(path)))
        report = ingest(repo, enumerate_periods(2024, 6), lambda p: fetch_temperature(p, ny=12, nx=16))
        ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
        print(f"  ingested {len(report.ingested)} periods -> {dict(ds.sizes)}")
        print(f"  store path: .../downloads/{path.name}")

        # SECTION: the document itself
        print("\nstac_collection(ds, dataset_id, ...) turns that store into a document:")
        collection = stac_collection(
            ds,
            DATASET_ID,
            title="Daily 2m temperature, Sierra Leone",
            description="Daily mean 2 metre air temperature, normalized to degC on a north-up WGS 84 grid.",
            zarr_href=ZARR_HREF,
            now=PUBLISHED_AT,
        )
        print_json(collection)

        # SECTION: identity
        print("\n-- id, title, description --")
        print(f"  id          = {collection['id']}")
        print(f"  title       = {collection['title']}")
        print(f"  license     = {collection['license']}")
        print("  The id is the primary key of the whole service: it names the store on")
        print("  disk, and it is the {id} in /stac/collections/{id}, so a client that")
        print(f"  has seen the id once can come back to it forever ({DATASET_ID}).")

        # SECTION: spatial extent
        print("\n-- extent.spatial.bbox --")
        stac_bbox = list(collection["extent"]["spatial"]["bbox"][0])
        print(f"  {stac_bbox}")
        print("  It is a LIST of boxes: a collection may be scattered over several")
        print("  footprints, so even a single-grid dataset nests its box one level deep.")
        geo_bbox = list(geozarr_attrs(ds)["spatial:bbox"])
        print(f"  GeoZarr spatial:bbox = {geo_bbox}")
        print(f"  identical to the STAC bbox: {stac_bbox == geo_bbox}")
        assert stac_bbox == geo_bbox
        print("  That is the point: catalogue search and rendered tile derive the box from")
        print("  the same transform, so a hit in the catalogue is really over that ground.")

        # SECTION: temporal extent
        print("\n-- extent.temporal.interval --")
        interval = list(collection["extent"]["temporal"]["interval"][0])
        print(f"  {interval}")
        stamps = pd.DatetimeIndex(ds["time"].values)
        print(f"  store's real time axis: {stamps[0].date()} .. {stamps[-1].date()} ({len(stamps)} days)")
        print(f"  temporal_extent(ds)   = {temporal_extent(ds)}")
        print(f"  interval matches the store: {interval == temporal_extent(ds)}")
        assert interval == temporal_extent(ds)
        assert interval[0].startswith(str(stamps[0].date()))
        assert interval[1].startswith(str(stamps[-1].date()))
        print("  Nothing here is declared by hand. The interval is read back off the")
        print("  committed time coordinate, so it cannot drift from what the store holds --")
        print("  re-publish after the next month lands and the end date moves by itself.")

        # SECTION: summaries
        print("\n-- summaries.variables --")
        for name, summary in collection["summaries"]["variables"].items():
            print(f"  {name}: {summary['long_name']} in {summary['units']}, {summary['min']} .. {summary['max']}")
        print(f"  summaries.proj:code = {collection['summaries']['proj:code']}")
        print("  Units are the half of the contract that data alone cannot carry: 27.4 is")
        print("  a plausible temperature in degC and an absurd one in K. The min/max")
        print("  let a client build a colour ramp, or notice a source went wrong, before")
        print("  downloading a single chunk.")

        # SECTION: the asset
        print("\n-- assets.zarr --")
        asset = collection["assets"]["zarr"]
        print(f"  href  = {asset['href']}")
        print(f"  type  = {asset['type']}")
        print(f"  roles = {asset['roles']}")
        print("  This is the door back to the data. Everything above is metadata a client")
        print("  can index and search; this one field says where to actually open the store.")

        # SECTION: how a client uses it
        print("\n-- the discovery loop --")
        print("  1. GET /stac/collections            -> which datasets does this instance hold?")
        print(f"  2. GET /stac/collections/{DATASET_ID}")
        print("     -> this document: extent, variables, units, asset href")
        print("  3. open assets.zarr.href            -> the store, already placed by GeoZarr")
        print("  No step required knowing that the store is icechunk, chunked 30 days at a")
        print("  time, or ingested one month per commit. That is what a discovery layer buys.")

        # SECTION: summary
        print("\n=== Summary ===")
        print(f"- One STAC Collection per dataset, keyed by id ({DATASET_ID})")
        print("- extent.spatial.bbox is a list of boxes; ours matches GeoZarr's exactly")
        print("- extent.temporal.interval is read off the committed time axis, not declared")
        print("- summaries.variables carries units, long names, and value ranges")
        print("- assets.zarr.href is the only field that points back at the bytes")
        print("- Published extents are derived, so re-publishing after an ingest is enough")
        print("  to keep the catalogue honest")


if __name__ == "__main__":
    main()
