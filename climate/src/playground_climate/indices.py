"""Climate indices: the derived products a service actually publishes.

Raw temperature and rainfall are inputs, not answers. What a health ministry
or planning office asks for is "how many hot days", "was this month unusually
dry", "when does the rainy season start" -- indices computed from the stored
series. These are the shape of the processes open-climate-service exposes over
openEO, implemented directly here so the arithmetic is visible.
"""

from typing import Any

import numpy as np
import xarray as xr


def _require(ds: xr.Dataset, variable: str) -> xr.DataArray:
    """Fetch a variable or raise a clear error naming what is available.

    Args:
        ds: The dataset to look in.
        variable: The variable name required.

    Returns:
        The requested data array.

    Raises:
        KeyError: If the variable is absent.
    """
    if variable not in ds.data_vars:
        raise KeyError(f"dataset has no variable {variable!r}; it has {tuple(ds.data_vars)}")
    return ds[variable]


def climatological_normal(ds: xr.Dataset, variable: str = "t2m") -> xr.DataArray:
    """Return the per-month mean over all years: the climatological normal.

    Args:
        ds: A dataset with a daily ``time`` axis.
        variable: Which variable to summarize.

    Returns:
        A ``(month, y, x)`` array of long-run monthly means.

    Raises:
        KeyError: If the variable is absent.
    """
    return _require(ds, variable).groupby("time.month").mean()


def monthly_anomaly(ds: xr.Dataset, variable: str = "t2m") -> xr.DataArray:
    """Return each timestep's departure from its month's normal.

    Anomalies, not absolute values, are what make two places or two years
    comparable -- which is why nearly every published climate product is one.

    Args:
        ds: A dataset with a daily ``time`` axis.
        variable: Which variable to anomalize.

    Returns:
        An array shaped like the input, in the same units.

    Raises:
        KeyError: If the variable is absent.
    """
    values = _require(ds, variable)
    normal = values.groupby("time.month").mean()
    anomaly = values.groupby("time.month") - normal
    anomaly.attrs = {**values.attrs, "long_name": f"{variable} anomaly"}
    return anomaly


def hot_days(ds: xr.Dataset, threshold: float = 30.0, variable: str = "t2m") -> xr.DataArray:
    """Count days per month above a temperature threshold.

    Args:
        ds: A dataset with daily temperature in degrees Celsius.
        threshold: The temperature above which a day counts as hot.
        variable: Which variable to threshold.

    Returns:
        A ``(time, y, x)`` array of monthly counts, resampled to month ends.

    Raises:
        KeyError: If the variable is absent.
    """
    values = _require(ds, variable)
    counts = (values > threshold).resample(time="1ME").sum()
    counts.attrs = {"units": "days", "long_name": f"days above {threshold} degC"}
    return counts


def wet_days(ds: xr.Dataset, threshold: float = 1.0, variable: str = "tp") -> xr.DataArray:
    """Count days per month with rainfall at or above a threshold.

    One millimetre is the conventional cutoff for a "wet day": below it, the
    reading is indistinguishable from dew or gauge noise.

    Args:
        ds: A dataset with daily precipitation in millimetres.
        threshold: Millimetres at or above which a day counts as wet.
        variable: Which variable to threshold.

    Returns:
        A ``(time, y, x)`` array of monthly counts.

    Raises:
        KeyError: If the variable is absent.
    """
    values = _require(ds, variable)
    counts = (values >= threshold).resample(time="1ME").sum()
    counts.attrs = {"units": "days", "long_name": f"days with at least {threshold} mm"}
    return counts


def monthly_total(ds: xr.Dataset, variable: str = "tp") -> xr.DataArray:
    """Sum a variable per month -- the right reduction for rainfall.

    Temperature is intensive and gets averaged; rainfall is extensive and gets
    summed. Using the wrong one is a classic and silent error.

    Args:
        ds: A dataset with a daily ``time`` axis.
        variable: Which variable to total.

    Returns:
        A ``(time, y, x)`` array of monthly totals.

    Raises:
        KeyError: If the variable is absent.
    """
    values = _require(ds, variable)
    totals = values.resample(time="1ME").sum()
    totals.attrs = {**values.attrs, "long_name": f"monthly total {variable}"}
    return totals


def spi_like(ds: xr.Dataset, variable: str = "tp") -> xr.DataArray:
    """Standardize monthly rainfall totals against their own month's history.

    A simplified standardized precipitation index: for each calendar month,
    subtract that month's long-run mean and divide by its standard deviation,
    so -2 means "far drier than this month usually is". The real SPI fits a
    gamma distribution first; the standardization idea is the same.

    Args:
        ds: A dataset with daily precipitation.
        variable: Which variable to standardize.

    Returns:
        A ``(time, y, x)`` array of dimensionless standardized anomalies.

    Raises:
        KeyError: If the variable is absent.
    """
    totals = monthly_total(ds, variable)
    grouped = totals.groupby("time.month")
    mean = grouped.mean()
    std = grouped.std()
    # Guard against a month with no variation, which would divide by zero.
    safe_std = std.where(std > 0, np.nan)
    index = (totals.groupby("time.month") - mean).groupby("time.month") / safe_std
    index.attrs = {"units": "1", "long_name": "standardized precipitation index (simplified)"}
    return index


def pyramid_levels(ds: xr.Dataset, levels: int = 3) -> list[xr.Dataset]:
    """Build coarser resolutions by repeated 2x2 mean downsampling.

    This is how open-climate-service builds the multiscale GeoZarr pyramid a
    map viewer needs: level 0 is full resolution, each subsequent level halves
    both spatial dimensions so a zoomed-out tile reads a small array instead
    of the whole grid.

    Args:
        ds: A dataset with ``y`` and ``x`` dimensions.
        levels: Total number of levels including level 0; must be at least 1.

    Returns:
        A list of datasets, coarsest last.

    Raises:
        ValueError: If levels is less than 1.
    """
    if levels < 1:
        raise ValueError(f"levels must be at least 1, got {levels}")
    out = [ds]
    current = ds
    for _ in range(levels - 1):
        if current.sizes.get("y", 1) < 2 or current.sizes.get("x", 1) < 2:
            break
        # xarray injects the reduction methods onto Coarsen at runtime, so
        # neither type checker can see .mean(); go through Any deliberately.
        coarsened: Any = current.coarsen(y=2, x=2, boundary="trim")
        current = coarsened.mean()
        out.append(current)
    return out
