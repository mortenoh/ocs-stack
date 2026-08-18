import tempfile
from pathlib import Path

import icechunk
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from climate_stack_climate_pipeline import (
    Period,
    bounding_box,
    committed_periods,
    enumerate_periods,
    fetch_precipitation,
    fetch_temperature,
    geozarr_attrs,
    grid_transform,
    hot_days,
    ingest,
    monthly_anomaly,
    monthly_total,
    normalize,
    pyramid_levels,
    stac_collection,
    store_path,
    temporal_extent,
    wet_days,
)

icechunk.set_logs_filter("error")


def make_repo(tmp: str, dataset_id: str = "temperature"):
    path = store_path(tmp, dataset_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    return icechunk.Repository.create(icechunk.local_filesystem_storage(str(path)))


class TestSources:
    def test_enumerates_chronologically(self):
        periods = enumerate_periods(2024, months=12)
        assert [p.period_id for p in periods] == [f"2024-{m:02d}" for m in range(1, 13)]

    @pytest.mark.parametrize("months", [0, -1, 13, 100])
    def test_rejects_months_outside_the_year(self, months: int):
        with pytest.raises(ValueError, match="between 1 and 12"):
            enumerate_periods(2024, months=months)


class TestNormalize:
    def test_renames_and_orients(self):
        raw = fetch_temperature(enumerate_periods(2024, 1)[0], ny=6, nx=6)
        assert raw["t2m"].dims == ("time", "lat", "lon")
        assert float(raw["lat"][0]) < float(raw["lat"][-1])  # source is south-up

        ds = normalize(raw)
        assert ds["t2m"].dims == ("time", "y", "x")
        assert float(ds["y"][0]) > float(ds["y"][-1])  # normalized to north-up

    def test_converts_kelvin_to_celsius(self):
        raw = fetch_temperature(enumerate_periods(2024, 1)[0], ny=4, nx=4)
        ds = normalize(raw)
        assert ds["t2m"].attrs["units"] == "degC"
        assert 15.0 < float(ds["t2m"].mean()) < 40.0

    def test_converts_metres_to_millimetres(self):
        ds = normalize(fetch_precipitation(enumerate_periods(2024, 1)[0], ny=4, nx=4))
        assert ds["tp"].attrs["units"] == "mm"
        assert float(ds["tp"].max()) > 1.0

    def test_drops_duplicate_timestamps(self):
        base = normalize(fetch_temperature(enumerate_periods(2024, 1)[0], ny=4, nx=4))
        doubled = xr.concat([base, base], dim="time")
        assert doubled.sizes["time"] == 2 * base.sizes["time"]
        cleaned = normalize(doubled)
        assert cleaned.sizes["time"] == base.sizes["time"]
        assert cleaned.indexes["time"].is_unique

    def test_rejects_unrecognized_dims(self):
        ds = xr.DataArray(np.zeros((2, 2)), dims=("foo", "bar")).rename("v").to_dataset()
        with pytest.raises(ValueError, match="no dimension matching"):
            normalize(ds)


class TestIngest:
    def test_ingests_every_period_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = make_repo(tmp)
            periods = enumerate_periods(2024, 3)
            report = ingest(repo, periods, lambda p: fetch_temperature(p, ny=6, nx=6))
            assert report.ingested == ["2024-01", "2024-02", "2024-03"]
            assert report.failed == {}
            assert len(report.snapshots) == 3

    def test_resume_skips_committed_periods(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = make_repo(tmp)
            periods = enumerate_periods(2024, 4)
            fetch = lambda p: fetch_temperature(p, ny=6, nx=6)  # noqa: E731

            interrupted = ingest(repo, periods, fetch, stop_after=2)
            assert interrupted.ingested == ["2024-01", "2024-02"]

            resumed = ingest(repo, periods, fetch)
            assert resumed.skipped == ["2024-01", "2024-02"]
            assert resumed.ingested == ["2024-03", "2024-04"]

    def test_time_axis_is_continuous_across_appends(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = make_repo(tmp)
            ingest(repo, enumerate_periods(2024, 5), lambda p: fetch_temperature(p, ny=6, nx=6))
            ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
            # Jan 31 + Feb 29 (leap) + Mar 31 + Apr 30 + May 31
            assert ds.sizes["time"] == 152
            assert ds.indexes["time"].is_monotonic_increasing
            assert ds.indexes["time"].is_unique

    def test_committed_periods_empty_for_new_repo(self):
        with tempfile.TemporaryDirectory() as tmp:
            assert committed_periods(make_repo(tmp)) == set()

    def test_rejects_unsupported_period_type(self):
        with tempfile.TemporaryDirectory() as tmp:
            with pytest.raises(ValueError, match="unsupported period_type"):
                committed_periods(make_repo(tmp), period_type="week")

    def test_failed_period_does_not_stop_the_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = make_repo(tmp)

            def flaky(period: Period) -> xr.Dataset:
                if period.period_id == "2024-02":
                    raise RuntimeError("source unavailable")
                return fetch_temperature(period, ny=6, nx=6)

            report = ingest(repo, enumerate_periods(2024, 3), flaky)
            assert report.ingested == ["2024-01", "2024-03"]
            assert "source unavailable" in report.failed["2024-02"]
            assert report.total == 3


class TestIndices:
    @pytest.fixture
    def dataset(self):
        parts = [normalize(fetch_temperature(p, ny=6, nx=6)) for p in enumerate_periods(2024, 4)]
        return xr.concat(parts, dim="time")

    def test_hot_days_counts_per_month(self, dataset: xr.Dataset):
        counts = hot_days(dataset, threshold=27.0)
        assert counts.sizes["time"] == 4
        assert int(counts.max()) <= 31
        assert int(counts.min()) >= 0

    def test_monthly_anomaly_centres_on_zero(self, dataset: xr.Dataset):
        anomaly = monthly_anomaly(dataset)
        assert anomaly.shape == dataset["t2m"].shape
        assert float(anomaly.mean()) == pytest.approx(0.0, abs=1e-9)

    def test_wet_days_and_totals(self):
        parts = [normalize(fetch_precipitation(p, ny=6, nx=6)) for p in enumerate_periods(2024, 3)]
        ds = xr.concat(parts, dim="time")
        assert int(wet_days(ds).max()) <= 31
        assert float(monthly_total(ds).min()) >= 0.0

    def test_missing_variable_raises(self, dataset: xr.Dataset):
        with pytest.raises(KeyError, match="has no variable"):
            hot_days(dataset, variable="nope")

    def test_pyramid_halves_each_level(self, dataset: xr.Dataset):
        levels = pyramid_levels(dataset, levels=3)
        assert [int(level.sizes["y"]) for level in levels] == [6, 3, 1]

    def test_pyramid_rejects_zero_levels(self, dataset: xr.Dataset):
        with pytest.raises(ValueError, match="at least 1"):
            pyramid_levels(dataset, levels=0)


class TestPublish:
    @pytest.fixture
    def dataset(self):
        return normalize(fetch_temperature(enumerate_periods(2024, 1)[0], ny=8, nx=10))

    def test_transform_is_pixel_registered_and_north_up(self, dataset: xr.Dataset):
        step_x, rot_x, origin_x, rot_y, step_y, _origin_y = grid_transform(dataset)
        assert step_x > 0 and step_y < 0  # north-up
        assert rot_x == 0.0 and rot_y == 0.0
        # Origin sits half a cell outside the first centre.
        assert origin_x == pytest.approx(float(dataset["x"][0]) - step_x / 2)

    def test_bbox_orders_west_south_east_north(self, dataset: xr.Dataset):
        west, south, east, north = bounding_box(dataset)
        assert west < east and south < north

    def test_geozarr_attrs_use_array_order(self, dataset: xr.Dataset):
        attrs = geozarr_attrs(dataset)
        assert attrs["spatial:dimensions"] == ["y", "x"]
        assert attrs["spatial:shape"] == [8, 10]
        assert attrs["proj:code"] == "EPSG:4326"

    def test_temporal_extent_is_iso(self, dataset: xr.Dataset):
        start, end = temporal_extent(dataset)
        assert start.startswith("2024-01-01")
        assert end.endswith("Z")

    def test_temporal_extent_needs_a_time_axis(self, dataset: xr.Dataset):
        with pytest.raises(ValueError, match="no time coordinate"):
            temporal_extent(dataset.isel(time=0, drop=True))

    def test_stac_collection_shape(self, dataset: xr.Dataset):
        collection = stac_collection(
            dataset, "temperature", description="daily 2m temperature", zarr_href="http://x/zarr/temperature"
        )
        assert collection["type"] == "Collection"
        assert collection["id"] == "temperature"
        assert len(collection["extent"]["spatial"]["bbox"][0]) == 4
        assert collection["summaries"]["variables"]["t2m"]["units"] == "degC"
        assert collection["assets"]["zarr"]["href"].endswith("temperature")

    def test_transform_needs_a_real_grid(self):
        tiny = xr.DataArray(
            np.zeros((1, 1, 1)),
            dims=("time", "y", "x"),
            coords={"time": pd.date_range("2024-01-01", periods=1), "y": [0.0], "x": [0.0]},
            name="t2m",
        ).to_dataset()
        with pytest.raises(ValueError, match="at least two cells"):
            grid_transform(tiny)


class TestEndToEnd:
    def test_ingest_derive_publish(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = make_repo(tmp)
            ingest(repo, enumerate_periods(2024, 6), lambda p: fetch_temperature(p, ny=8, nx=8))

            ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
            assert ds.sizes["time"] == 182
            assert ds["t2m"].attrs["units"] == "degC"

            index = hot_days(ds, threshold=27.0)
            assert index.sizes["time"] == 6

            collection = stac_collection(ds, "temperature")
            assert collection["extent"]["temporal"]["interval"][0][0].startswith("2024-01-01")
            assert Path(store_path(tmp, "temperature")).exists()
