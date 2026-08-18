"""The same pipeline over rainfall: what generalizes, and what must not.

What: runs the whole service over precipitation -- normalize metres to
millimetres, ingest twelve months one commit each, derive monthly totals, wet
days, and a standardized index, and publish a second STAC collection -- then
lists both collections the way an instance's catalogue would.

Why: open-climate-service is one pipeline serving many datasets, so the
machinery must not care which variable it carries. It does not: the same
normalize, ingest, and publish calls work unchanged. What does change is the
arithmetic on top. Temperature is intensive and gets averaged; rainfall is
extensive and gets summed. Pick the wrong reduction and nothing fails -- you
just publish a number that means nothing.

Run: make run EXAMPLE=0402_second_dataset
"""

import tempfile
from datetime import UTC, datetime
from typing import Any

import icechunk
import numpy as np
import pandas as pd
import xarray as xr

from ocs_stack_climate_pipeline import (
    Period,
    enumerate_periods,
    fetch_precipitation,
    fetch_temperature,
    ingest,
    monthly_total,
    normalize,
    spi_like,
    stac_collection,
    store_path,
    temporal_extent,
    wet_days,
)

# icechunk is a Rust extension without type stubs, so its objects stay Any.
icechunk.set_logs_filter("error")

TEMPERATURE_ID = "temperature-daily-sle"
PRECIPITATION_ID = "precipitation-daily-sle"
MONTHS = 12
NY = 16
NX = 16
PUBLISHED_AT = datetime(2025, 1, 15, 9, 0, tzinfo=UTC)
HREF = "https://climate.example.org/zarr/{}"


def build_store(tmpdir: str, dataset_id: str, fetch: Any, periods: list[Period]) -> tuple[Any, xr.Dataset]:
    """Create a store and ingest every period into it, one commit each.

    Args:
        tmpdir: Directory holding the instance's data.
        dataset_id: Public identifier, which also names the store on disk.
        fetch: Source callable returning a raw dataset for one period.
        periods: Periods to ingest, in chronological order.

    Returns:
        The icechunk repository and the dataset read back from its branch tip.
    """
    path = store_path(tmpdir, dataset_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    repo: Any = icechunk.Repository.create(icechunk.local_filesystem_storage(str(path)))
    ingest(repo, periods, fetch)
    return repo, xr.open_zarr(repo.readonly_session("main").store, consolidated=False)


def main() -> None:
    """Run the pipeline over precipitation and contrast it with temperature."""
    with tempfile.TemporaryDirectory() as tmpdir:
        periods = enumerate_periods(2024, MONTHS)

        # SECTION: the instance already holds one dataset
        print("The instance already holds the temperature dataset that example 0401 built.")
        _, temperature = build_store(tmpdir, TEMPERATURE_ID, lambda p: fetch_temperature(p, ny=NY, nx=NX), periods)
        print(f"  {TEMPERATURE_ID}: {dict(temperature.sizes)} in {temperature['t2m'].attrs['units']}")
        print("Now a second source arrives. Nothing in the pipeline is told it is different.")

        # SECTION: normalize -- a different unit problem, the same fix
        print("\n-- normalize: metres to millimetres --")
        raw = fetch_precipitation(periods[6], ny=NY, nx=NX)
        clean = normalize(raw)
        print(f"  source : tp{tuple(raw['tp'].dims)} in {raw['tp'].attrs['units']!r}, max {float(raw['tp'].max()):.4f}")
        print(
            f"  clean  : tp{tuple(clean['tp'].dims)} in {clean['tp'].attrs['units']!r}, "
            f"max {float(clean['tp'].max()):.4f}"
        )
        print("  Metres of rainfall is what the reanalysis publishes and nobody thinks in.")
        print("  The values are scaled by 1000 AND the units attribute is rewritten; doing")
        print("  only the first is how a store ends up labelled 'm' while holding mm.")
        dry_season = normalize(fetch_precipitation(periods[0], ny=NY, nx=NX))
        wet_zeros = float((np.asarray(clean["tp"].values) == 0.0).mean())
        dry_zeros = float((np.asarray(dry_season["tp"].values) == 0.0).mean())
        print(f"  Note the shape of the data: {dry_zeros:.0%} of January's cell-days are exactly zero,")
        print(f"  against {wet_zeros:.0%} of July's. Rainfall is zero-inflated and strongly seasonal;")
        print("  temperature is neither, and that difference is what makes the derived")
        print("  products further down diverge.")

        # SECTION: ingest -- unchanged
        print("\n-- ingest: the same call, a different store --")
        repo, ds = build_store(tmpdir, PRECIPITATION_ID, lambda p: fetch_precipitation(p, ny=NY, nx=NX), periods)
        stamps = pd.DatetimeIndex(ds["time"].values)
        history: list[Any] = list(repo.ancestry(branch="main"))
        print(f"  ingest(repo, periods, fetch_precipitation) -> {dict(ds.sizes)}")
        print(f"  time span : {stamps[0].date()} .. {stamps[-1].date()} ({len(stamps)} days)")
        print(f"  snapshots : {len(history)} (1 init + {MONTHS} ingests), one commit per month")
        print(f"  units     : {ds['tp'].attrs['units']}, mean {float(ds['tp'].mean()):.2f} mm/day")
        print("  Byte for byte this is the temperature code path. Streaming ingest, resume,")
        print("  and versioning are properties of the pipeline, not of the variable.")

        # SECTION: intensive vs extensive
        print("\n-- derive: why rainfall is summed and temperature is averaged --")
        totals = monthly_total(ds)
        daily_mean = ds["tp"].resample(time="1ME").mean()
        totals_area = np.asarray(totals.mean(dim=["y", "x"]).values, dtype="float64")
        means_area = np.asarray(daily_mean.mean(dim=["y", "x"]).values, dtype="float64")
        months = [int(m) for m in pd.DatetimeIndex(totals["time"].values).month]
        print("    month " + " ".join(f"{m:>6d}" for m in months))
        print("    total " + " ".join(f"{v:>6.0f}" for v in totals_area) + "   mm accumulated")
        print("    mean  " + " ".join(f"{v:>6.1f}" for v in means_area) + "   mm per day")
        print("  Temperature is INTENSIVE: it has a value at every instant, and averaging")
        print("  two days gives a temperature. Rainfall is EXTENSIVE: it is an amount that")
        print("  accumulates, and the honest monthly figure is the sum of the days.")
        print("  The mean is not wrong arithmetic, it is the wrong question. A reservoir")
        print(f"  fills from the {totals_area.max():.0f} mm that fell, not from {means_area.max():.1f} mm/day,")
        print("  and a mean also quietly makes a 28-day month look like a 31-day one.")

        # SECTION: wet days
        print("\n-- derive: counting days, not averaging them --")
        wet = wet_days(ds, threshold=1.0)
        wet_area = np.asarray(wet.mean(dim=["y", "x"]).values, dtype="float64")
        print(f"  wet_days(ds, threshold=1.0) -> {tuple(wet.dims)} {tuple(wet.shape)}, units {wet.attrs['units']}")
        print("    month " + " ".join(f"{m:>6d}" for m in months))
        print("    days  " + " ".join(f"{v:>6.1f}" for v in wet_area))
        print("  The 1 mm cutoff is conventional: below it a reading is dew or gauge noise.")
        print("  Two months can share a total and differ completely -- 200 mm over 20 days")
        print("  is a growing season, 200 mm over 3 days is a flood -- so the count is its")
        print("  own product, not a summary of the total. The temperature equivalent is")
        print("  hot_days: same idea, count exceedances, different threshold and meaning.")

        # SECTION: standardized anomalies
        print("\n-- derive: a standardized index needs a history to standardize against --")
        single_year = spi_like(ds)
        finite = int(np.isfinite(np.asarray(single_year.values, dtype="float64")).sum())
        print(f"  spi_like over one year -> {tuple(single_year.shape)}, finite values: {finite}")
        print("  All NaN, and correctly so: with one year, each calendar month has exactly")
        print("  one sample, its standard deviation is zero, and 'unusual compared to what?'")
        print("  has no answer. The library refuses rather than dividing by zero.")

        print("\n  Same call over a three-year series (2022-2024):")
        parts = [
            normalize(fetch_precipitation(p, ny=NY, nx=NX))
            for year in (2022, 2023, 2024)
            for p in enumerate_periods(year, MONTHS)
        ]
        history_ds = xr.concat(parts, dim="time")
        spi = spi_like(history_ds)
        spi_cell = np.asarray(spi.isel(y=0, x=0).sel(time="2024").values, dtype="float64")
        print(f"  spi_like(history) -> {tuple(spi.dims)} {tuple(spi.shape)}, units {spi.attrs['units']!r}")
        print("  2024 at the north-west corner cell, standardized against 2022-2024:")
        print("    month " + " ".join(f"{m:>6d}" for m in months))
        print("    spi   " + " ".join(f"{v:>+6.2f}" for v in spi_cell))
        print(f"  Range over the whole cube: {float(spi.min()):+.2f} .. {float(spi.max()):+.2f}.")
        print("  Negative means drier than that calendar month usually is, positive wetter,")
        print("  in units of its own standard deviation -- which is what makes July in")
        print("  Freetown comparable to January in Kabala. With only three years the index")
        print("  saturates near +/-1.41; real SPI wants thirty.")
        print("  Temperature's anomaly is a plain departure in degC, because degC differences")
        print("  are already comparable. Rainfall needs standardizing first.")

        # SECTION: publish
        print("\n-- publish: a second collection, same document shape --")
        collection = stac_collection(
            ds,
            PRECIPITATION_ID,
            title="Daily rainfall, Sierra Leone",
            description="Daily total precipitation, normalized to mm on a north-up WGS 84 grid.",
            zarr_href=HREF.format(PRECIPITATION_ID),
            now=PUBLISHED_AT,
        )
        summary = collection["summaries"]["variables"]["tp"]
        print(f"  id                       = {collection['id']}")
        print(f"  extent.spatial.bbox      = {[round(v, 4) for v in collection['extent']['spatial']['bbox'][0]]}")
        print(f"  extent.temporal.interval = {collection['extent']['temporal']['interval'][0]}")
        print(f"  summaries.variables.tp   = {summary['units']}, {summary['min']} .. {summary['max']}")
        print(f"  assets.zarr.href         = {collection['assets']['zarr']['href']}")

        temperature_collection = stac_collection(
            temperature,
            TEMPERATURE_ID,
            title="Daily 2m temperature, Sierra Leone",
            description="Daily mean 2 metre air temperature, normalized to degC on a north-up WGS 84 grid.",
            zarr_href=HREF.format(TEMPERATURE_ID),
            now=PUBLISHED_AT,
        )
        same_bbox = collection["extent"]["spatial"]["bbox"] == temperature_collection["extent"]["spatial"]["bbox"]
        print(f"  Both collections share a bbox ({same_bbox}) because both grids share a")
        print("  transform. They differ in id, units, and value range -- nothing structural.")

        # SECTION: the catalogue
        print("\n-- GET /stac/collections --")
        catalogue = [temperature_collection, collection]
        print(f"  {'id':<24} {'variable':<9} {'units':<6} {'interval':<24} range")
        for doc in catalogue:
            name, stats = next(iter(doc["summaries"]["variables"].items()))
            start, end = doc["extent"]["temporal"]["interval"][0]
            span = f"{start[:10]}..{end[:10]}"
            print(
                f"  {doc['id']:<24} {name:<9} {stats['units']:<6} {span:<24} {stats['min']:.1f} .. {stats['max']:.1f}"
            )
        print(f"  Both intervals were read off their own stores: {temporal_extent(ds)[0][:10]} onward.")
        print("  That listing is the whole catalogue of this instance: two datasets, one")
        print("  pipeline, one document shape, discovered without knowing anything inside.")

        # SECTION: summary
        print("\n=== Summary ===")
        print("- normalize, ingest, and publish were called unchanged for a second variable")
        print("- Only the units conversion differed: K -> degC for one, m -> mm for the other")
        print("- Temperature is intensive (average it); rainfall is extensive (sum it)")
        print("- Rainfall is zero-inflated, so counts of wet days say what a mean cannot")
        print("- Standardized indices need years of the same calendar month to compare with;")
        print("  one year of data yields NaN, and that is the honest answer")
        print(f"- Two STAC collections now describe this instance: {TEMPERATURE_ID}")
        print(f"  and {PRECIPITATION_ID}, sharing a footprint and differing in content")


if __name__ == "__main__":
    main()
