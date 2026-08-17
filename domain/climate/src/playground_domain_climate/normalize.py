"""Normalizing source data into the service's own conventions.

Real sources disagree about everything: dimension names (``lat``/``lon`` vs
``latitude``/``longitude``), units (Kelvin vs Celsius, m vs mm), and axis
direction (south-up vs north-up). open-climate-service resolves all of that at
ingest so that every stored dataset looks identical to everything downstream,
which is what makes its API uniform across sources.

This module is that step: source in, canonical ``(time, y, x)`` in degrees
Celsius or millimetres out.
"""

import numpy as np
import xarray as xr

# Every spelling a source might use for the spatial axes.
Y_ALIASES = ("y", "lat", "latitude", "Latitude", "LAT")
X_ALIASES = ("x", "lon", "longitude", "Longitude", "LON")
TIME_ALIASES = ("time", "t", "valid_time", "date")

CANONICAL_UNITS = {"t2m": "degC", "tp": "mm"}


def rename_dims(ds: xr.Dataset) -> xr.Dataset:
    """Rename whatever the source calls its axes to ``(time, y, x)``.

    Args:
        ds: A dataset using any of the recognized dimension spellings.

    Returns:
        The dataset with its dimensions and coordinates renamed.

    Raises:
        ValueError: If a spatial or time axis cannot be identified.
    """
    mapping: dict[str, str] = {}
    for aliases, target in ((TIME_ALIASES, "time"), (Y_ALIASES, "y"), (X_ALIASES, "x")):
        found = next((name for name in aliases if name in ds.dims or name in ds.coords), None)
        if found is None:
            raise ValueError(f"no dimension matching {target!r} among {tuple(ds.dims)}")
        if found != target:
            mapping[found] = target
    return ds.rename(mapping) if mapping else ds


def orient_north_up(ds: xr.Dataset) -> xr.Dataset:
    """Ensure the y axis descends, so row 0 is the northernmost.

    GeoZarr places a raster with an affine whose y step is negative for a
    north-up grid. A south-up source silently renders upside down, so the
    orientation is fixed here rather than trusted.

    Args:
        ds: A dataset with a ``y`` coordinate.

    Returns:
        The dataset, reversed along y if it was ascending.
    """
    if "y" not in ds.coords or ds.sizes.get("y", 0) < 2:
        return ds
    if float(ds["y"][0]) < float(ds["y"][-1]):
        return ds.isel(y=slice(None, None, -1))
    return ds


def convert_units(ds: xr.Dataset) -> xr.Dataset:
    """Convert known variables to the service's canonical units.

    Kelvin becomes Celsius and metres of precipitation become millimetres.
    Attributes are inert in xarray, so the ``units`` attribute is rewritten by
    hand -- forgetting that is how a dataset ends up labelled ``K`` while
    holding Celsius.

    Args:
        ds: A dataset whose variables carry a ``units`` attribute.

    Returns:
        A new dataset with converted values and corrected unit attributes.
    """
    out = ds.copy()
    for name, var in ds.data_vars.items():
        units = str(var.attrs.get("units", "")).strip()
        if units in ("K", "kelvin", "Kelvin"):
            out[name] = var - 273.15
            out[name].attrs = {**var.attrs, "units": "degC"}
        elif units in ("m", "metre", "meters") and name == "tp":
            out[name] = var * 1000.0
            out[name].attrs = {**var.attrs, "units": "mm"}
        else:
            out[name].attrs = dict(var.attrs)
    return out


def sort_time(ds: xr.Dataset) -> xr.Dataset:
    """Sort along time and drop duplicate timestamps, keeping the last.

    A re-fetched period arrives with timestamps the store may already hold.
    Appending it blindly produces a store with duplicate coordinates that
    cannot be indexed sanely, so duplicates are resolved here.

    Args:
        ds: A dataset with a ``time`` dimension.

    Returns:
        The dataset sorted by time with unique timestamps.
    """
    if "time" not in ds.dims:
        return ds
    ds = ds.sortby("time")
    index = ds.indexes["time"]
    if index.has_duplicates:
        keep = ~index.duplicated(keep="last")
        ds = ds.isel(time=np.flatnonzero(keep))
    return ds


def normalize(ds: xr.Dataset) -> xr.Dataset:
    """Run the full normalization pipeline on a source dataset.

    Args:
        ds: Raw source data in whatever conventions it arrived with.

    Returns:
        A dataset with dims ``(time, y, x)``, a north-up y axis, canonical
        units, and a sorted, duplicate-free time axis.

    Raises:
        ValueError: If the dataset's axes cannot be identified.
    """
    ds = rename_dims(ds)
    ds = orient_north_up(ds)
    ds = convert_units(ds)
    ds = sort_time(ds)
    return ds.transpose("time", "y", "x", ...)
