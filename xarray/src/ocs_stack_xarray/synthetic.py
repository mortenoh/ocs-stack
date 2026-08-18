"""Synthetic climate datasets shaped like open-climate-service stores.

Every generator returns an ``xr.Dataset`` with dims ``(time, y, x)`` — the
normalized layout OCS writes to its zarr stores — daily time steps, lat/lon-like
coordinates on a regular grid, and CF-style attrs (``units``, ``long_name``).
Deterministic for a given seed, so examples and tests are reproducible.
"""

import numpy as np
import pandas as pd
import xarray as xr

# A small Sierra Leone-ish bounding box: OCS instances are scoped to one
# country extent, so the synthetic grid mimics that shape of data.
DEFAULT_BBOX = (-13.5, 6.9, -10.3, 10.0)  # (west, south, east, north)


def _grid(ny: int, nx: int) -> tuple[np.ndarray, np.ndarray]:
    """Return (y, x) coordinate arrays over the default bbox, y descending (north-up)."""
    west, south, east, north = DEFAULT_BBOX
    y = np.linspace(north, south, ny)
    x = np.linspace(west, east, nx)
    return y, x


def _validate(days: int, ny: int, nx: int) -> None:
    """Reject non-positive dimension sizes with a clear error."""
    for name, value in (("days", days), ("ny", ny), ("nx", nx)):
        if value < 1:
            raise ValueError(f"{name} must be at least 1, got {value}")


def temperature_dataset(days: int = 30, ny: int = 20, nx: int = 30, seed: int = 0) -> xr.Dataset:
    """Return a daily 2 m temperature dataset with dims (time, y, x).

    Values are degrees Celsius: a base field with a north-south gradient, a
    seasonal-ish sine over time, and gaussian noise.

    Args:
        days: Number of daily time steps; must be at least 1.
        ny: Grid height; must be at least 1.
        nx: Grid width; must be at least 1.
        seed: Seed for the random noise component.

    Returns:
        A dataset with one data variable ``t2m`` and coords time/y/x.

    Raises:
        ValueError: If days, ny, or nx is less than 1.
    """
    _validate(days, ny, nx)
    rng = np.random.default_rng(seed)
    time = pd.date_range("2024-01-01", periods=days, freq="D")
    y, x = _grid(ny, nx)

    gradient = np.linspace(2.0, -2.0, ny).reshape(1, ny, 1)
    season = 3.0 * np.sin(2 * np.pi * np.arange(days) / 365.25).reshape(days, 1, 1)
    noise = rng.normal(0.0, 0.8, size=(days, ny, nx))
    values = 26.0 + gradient + season + noise

    da = xr.DataArray(
        values,
        dims=("time", "y", "x"),
        coords={"time": time, "y": y, "x": x},
        name="t2m",
        attrs={"units": "degC", "long_name": "2 metre temperature"},
    )
    return da.to_dataset()


def precipitation_dataset(days: int = 30, ny: int = 20, nx: int = 30, seed: int = 0) -> xr.Dataset:
    """Return a daily precipitation dataset with dims (time, y, x).

    Values are mm/day: zero on dry days (roughly 60 percent of the field),
    gamma-distributed amounts otherwise — the zero-inflated shape real rainfall
    data has, which matters for masking and skipna examples.

    Args:
        days: Number of daily time steps; must be at least 1.
        ny: Grid height; must be at least 1.
        nx: Grid width; must be at least 1.
        seed: Seed for the random components.

    Returns:
        A dataset with one data variable ``tp`` and coords time/y/x.

    Raises:
        ValueError: If days, ny, or nx is less than 1.
    """
    _validate(days, ny, nx)
    rng = np.random.default_rng(seed)
    time = pd.date_range("2024-01-01", periods=days, freq="D")
    y, x = _grid(ny, nx)

    wet = rng.random(size=(days, ny, nx)) > 0.6
    amounts = rng.gamma(shape=2.0, scale=4.0, size=(days, ny, nx))
    values = np.where(wet, amounts, 0.0)

    da = xr.DataArray(
        values,
        dims=("time", "y", "x"),
        coords={"time": time, "y": y, "x": x},
        name="tp",
        attrs={"units": "mm/day", "long_name": "total precipitation"},
    )
    return da.to_dataset()
