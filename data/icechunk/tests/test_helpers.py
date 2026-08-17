import tempfile
from pathlib import Path

import pytest

from playground_data_icechunk import (
    climate_dataset,
    describe_history,
    open_repo,
    quiet_icechunk_logs,
    read_dataset,
    write_dataset,
)

quiet_icechunk_logs()


class TestClimateDataset:
    def test_shape_and_attrs(self):
        ds = climate_dataset(days=5, ny=4, nx=6)
        assert ds["t2m"].dims == ("time", "y", "x")
        assert ds["t2m"].shape == (5, 4, 6)
        assert ds["t2m"].attrs["units"] == "degC"

    def test_offset_shifts_values(self):
        base = float(climate_dataset(days=3).t2m.mean())
        shifted = float(climate_dataset(days=3, offset=10.0).t2m.mean())
        assert shifted - base == pytest.approx(10.0, abs=1e-9)

    @pytest.mark.parametrize(("days", "ny", "nx"), [(0, 4, 4), (4, 0, 4), (4, 4, -3)])
    def test_rejects_non_positive_sizes(self, days: int, ny: int, nx: int):
        with pytest.raises(ValueError, match="must be at least 1"):
            climate_dataset(days=days, ny=ny, nx=nx)


class TestRepository:
    def test_write_read_round_trip(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = open_repo(Path(tmp) / "store.icechunk")
            write_dataset(repo, climate_dataset(days=4), "initial ingest")
            ds = read_dataset(repo)
            assert ds["t2m"].shape == (4, 32, 32)

    def test_open_repo_reopens_existing(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "store.icechunk"
            repo = open_repo(path)
            write_dataset(repo, climate_dataset(days=2), "first")
            reopened = open_repo(path)
            assert float(read_dataset(reopened).t2m.mean()) == pytest.approx(float(read_dataset(repo).t2m.mean()))

    def test_history_is_newest_first(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = open_repo(Path(tmp) / "store.icechunk")
            write_dataset(repo, climate_dataset(days=2), "first")
            write_dataset(repo, climate_dataset(days=2, offset=5.0), "second")
            history = describe_history(repo)
            messages = [entry["message"] for entry in history]
            assert messages[0] == "second"
            assert messages[1] == "first"
            assert len(history[0]["short"]) == 8

    def test_snapshot_reads_are_immutable(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = open_repo(Path(tmp) / "store.icechunk")
            first = write_dataset(repo, climate_dataset(days=2), "first")
            write_dataset(repo, climate_dataset(days=2, offset=5.0), "second")
            old = float(read_dataset(repo, snapshot_id=first).t2m.mean())
            new = float(read_dataset(repo).t2m.mean())
            assert new - old == pytest.approx(5.0, abs=1e-9)

    def test_append_extends_time(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = open_repo(Path(tmp) / "store.icechunk")
            write_dataset(repo, climate_dataset(days=3), "january")
            write_dataset(
                repo,
                climate_dataset(days=2, start="2024-01-04"),
                "february",
                append_dim="time",
            )
            assert read_dataset(repo).sizes["time"] == 5
