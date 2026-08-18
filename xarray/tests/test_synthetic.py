import numpy as np
import pytest

from ocs_stack_xarray import precipitation_dataset, temperature_dataset


class TestTemperatureDataset:
    def test_shape_and_dims(self):
        ds = temperature_dataset(days=10, ny=4, nx=6)
        assert ds["t2m"].dims == ("time", "y", "x")
        assert ds["t2m"].shape == (10, 4, 6)

    def test_coords_and_attrs(self):
        ds = temperature_dataset(days=3)
        assert str(ds["time"].values[0])[:10] == "2024-01-01"
        assert ds["y"].values[0] > ds["y"].values[-1]  # north-up: y descending
        assert ds["t2m"].attrs["units"] == "degC"

    def test_deterministic_for_seed(self):
        a = temperature_dataset(days=5, seed=42)
        b = temperature_dataset(days=5, seed=42)
        assert np.array_equal(a["t2m"].values, b["t2m"].values)

    @pytest.mark.parametrize("kwargs", [{"days": 0}, {"ny": 0}, {"nx": -1}])
    def test_rejects_non_positive_sizes(self, kwargs: dict[str, int]):
        with pytest.raises(ValueError, match="must be at least 1"):
            temperature_dataset(**kwargs)


class TestPrecipitationDataset:
    def test_shape_and_attrs(self):
        ds = precipitation_dataset(days=10, ny=4, nx=6)
        assert ds["tp"].dims == ("time", "y", "x")
        assert ds["tp"].attrs["units"] == "mm/day"

    def test_zero_inflated_and_non_negative(self):
        tp = precipitation_dataset(days=30).tp
        assert float(tp.min()) == 0.0
        dry_fraction = float((tp == 0.0).mean())
        assert 0.4 < dry_fraction < 0.8
        assert bool((tp >= 0.0).all())

    def test_rejects_non_positive_sizes(self):
        with pytest.raises(ValueError, match="days must be at least 1"):
            precipitation_dataset(days=0)
