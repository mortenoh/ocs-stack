"""Synthetic data sources, deliberately messy.

Each source stands in for a real one and arrives in its own conventions, so
the normalization step has something to actually do. Periods are enumerated
the way open-climate-service's streaming ingest does it: the plugin lists the
periods it can supply, and the framework fetches them one at a time.
"""

import zlib
from dataclasses import dataclass

import numpy as np
import pandas as pd
import xarray as xr

# A Sierra Leone-ish extent, matching the country-scoped instances OCS deploys.
BBOX = (-13.5, 6.9, -10.3, 10.0)


@dataclass(frozen=True)
class Period:
    """One ingestable period.

    Attributes:
        period_id: Stable identifier, such as ``"2024-01"``.
        start: First day of the period, as an ISO date string.
        days: Number of daily steps in the period.
    """

    period_id: str
    start: str
    days: int


def enumerate_periods(year: int = 2024, months: int = 6) -> list[Period]:
    """List the monthly periods a source can supply.

    Args:
        year: Calendar year to enumerate.
        months: How many months from January to include; must be 1..12.

    Returns:
        One :class:`Period` per month, in chronological order.

    Raises:
        ValueError: If months is outside 1..12.
    """
    if not 1 <= months <= 12:
        raise ValueError(f"months must be between 1 and 12, got {months}")
    periods: list[Period] = []
    for month in range(1, months + 1):
        start = pd.Timestamp(year=year, month=month, day=1)
        periods.append(
            Period(
                period_id=f"{year}-{month:02d}",
                start=start.strftime("%Y-%m-%d"),
                days=int(start.days_in_month),
            )
        )
    return periods


def _period_seed(period: Period) -> int:
    """Derive a stable per-period seed from its id.

    Python randomizes string hashing per process, so ``hash(period_id)`` would
    give different data on every run. crc32 is stable across processes and
    machines, which is what reproducible examples need.

    Args:
        period: The period whose id seeds the generator.

    Returns:
        A non-negative integer under 10000.
    """
    return zlib.crc32(period.period_id.encode()) % 10_000


def _grid(ny: int, nx: int, ascending_y: bool) -> tuple[np.ndarray, np.ndarray]:
    """Build coordinate arrays over the standard bbox.

    Args:
        ny: Grid height.
        nx: Grid width.
        ascending_y: When True, y ascends (south-up), as some sources publish.

    Returns:
        The y and x coordinate arrays.
    """
    west, south, east, north = BBOX
    y = np.linspace(south, north, ny) if ascending_y else np.linspace(north, south, ny)
    x = np.linspace(west, east, nx)
    return y, x


def fetch_temperature(period: Period, ny: int = 24, nx: int = 24, seed: int = 0) -> xr.Dataset:
    """Fetch one period of temperature, in a deliberately awkward source format.

    This source publishes Kelvin on a south-up ``lat``/``lon`` grid -- exactly
    the kind of thing normalization exists to fix.

    Args:
        period: The period to fetch.
        ny: Grid height.
        nx: Grid width.
        seed: Base seed; the period id is mixed in so periods differ.

    Returns:
        A dataset with variable ``t2m`` in Kelvin, dims ``(time, lat, lon)``.
    """
    rng = np.random.default_rng(seed + _period_seed(period))
    time = pd.date_range(period.start, periods=period.days, freq="D")
    lat, lon = _grid(ny, nx, ascending_y=True)

    day_of_year = time.dayofyear.to_numpy().reshape(-1, 1, 1)
    seasonal = 3.0 * np.sin(2 * np.pi * day_of_year / 365.25)
    gradient = np.linspace(-1.5, 1.5, ny).reshape(1, ny, 1)
    kelvin = 273.15 + 27.0 + seasonal + gradient + rng.normal(0.0, 0.7, size=(period.days, ny, nx))

    return xr.DataArray(
        kelvin,
        dims=("time", "lat", "lon"),
        coords={"time": time, "lat": lat, "lon": lon},
        name="t2m",
        attrs={"units": "K", "long_name": "2 metre temperature"},
    ).to_dataset()


def fetch_precipitation(period: Period, ny: int = 24, nx: int = 24, seed: int = 1) -> xr.Dataset:
    """Fetch one period of precipitation, in metres on a south-up grid.

    Args:
        period: The period to fetch.
        ny: Grid height.
        nx: Grid width.
        seed: Base seed; the period id is mixed in so periods differ.

    Returns:
        A dataset with variable ``tp`` in metres, dims ``(time, lat, lon)``.
    """
    rng = np.random.default_rng(seed + _period_seed(period))
    time = pd.date_range(period.start, periods=period.days, freq="D")
    lat, lon = _grid(ny, nx, ascending_y=True)

    # West African rainfall peaks mid-year; zero-inflated, like the real thing.
    month = time.month.to_numpy().reshape(-1, 1, 1)
    wetness = np.clip(np.sin((month - 2) / 12 * np.pi), 0.05, None)
    wet = rng.random((period.days, ny, nx)) < wetness
    amounts_mm = rng.gamma(2.0, 5.0, size=(period.days, ny, nx))
    metres = np.where(wet, amounts_mm, 0.0) / 1000.0

    return xr.DataArray(
        metres,
        dims=("time", "lat", "lon"),
        coords={"time": time, "lat": lat, "lon": lon},
        name="tp",
        attrs={"units": "m", "long_name": "total precipitation"},
    ).to_dataset()
