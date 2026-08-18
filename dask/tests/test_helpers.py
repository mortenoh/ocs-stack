import numpy as np
import pytest

from ocs_stack_dask import chunk_report, random_field, task_count


class TestRandomField:
    def test_shape_and_chunks(self):
        arr = random_field(days=60, ny=100, nx=100, time_chunk=30, spatial_chunk=50)
        assert arr.shape == (60, 100, 100)
        assert arr.chunks[0] == (30, 30)
        assert arr.chunks[1] == (50, 50)

    def test_lazy_until_computed(self):
        arr = random_field(days=10, ny=8, nx=8)
        values = arr.compute()
        assert values.shape == (10, 8, 8)
        assert 0.0 <= float(np.min(values)) and float(np.max(values)) < 1.0

    def test_deterministic_for_seed(self):
        a = random_field(days=5, ny=4, nx=4, seed=7).compute()
        b = random_field(days=5, ny=4, nx=4, seed=7).compute()
        assert np.array_equal(a, b)

    @pytest.mark.parametrize("kwargs", [{"days": 0}, {"ny": 0}, {"nx": -2}])
    def test_rejects_non_positive_sizes(self, kwargs: dict[str, int]):
        with pytest.raises(ValueError, match="must be at least 1"):
            random_field(**kwargs)


class TestReporting:
    def test_chunk_report_contents(self):
        arr = random_field(days=365, ny=256, nx=256, time_chunk=30, spatial_chunk=128)
        report = chunk_report(arr)
        assert "shape=(365, 256, 256)" in report
        assert "chunks=(30, 128, 128)" in report
        assert "n_chunks=52" in report

    def test_task_count_grows_with_operations(self):
        arr = random_field(days=10, ny=8, nx=8)
        base = task_count(arr)
        derived = task_count((arr * 2).mean(axis=0))
        assert base > 0
        assert derived > base
