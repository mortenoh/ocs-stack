"""Publishing: GeoZarr attributes and STAC metadata.

Storing the data is not the same as publishing it. A client that finds this
store needs to know where on Earth the grid sits, in which CRS, what time
range it covers, and what the variables mean. GeoZarr answers the first two
with root attributes; STAC answers the rest with a collection document that a
catalogue can index.
"""

from datetime import UTC, datetime
from typing import Any

import numpy as np
import pandas as pd
import xarray as xr

CRS = "EPSG:4326"


def grid_transform(ds: xr.Dataset) -> list[float]:
    """Return the affine transform placing a north-up grid on Earth.

    The six values are ``[stepX, rotX, originX, rotY, stepY, originY]`` with
    the origin on the OUTER EDGE of the first cell -- pixel registration, not
    cell centres -- and a negative y step for a north-up grid. Getting the
    half-cell offset wrong shifts every rendered tile by half a pixel.

    Args:
        ds: A dataset with ``y`` and ``x`` coordinates.

    Returns:
        The six affine coefficients.

    Raises:
        ValueError: If the grid has fewer than two cells on an axis.
    """
    if ds.sizes.get("x", 0) < 2 or ds.sizes.get("y", 0) < 2:
        raise ValueError("a transform needs at least two cells on each axis")

    x = ds["x"].values
    y = ds["y"].values
    step_x = float(x[1] - x[0])
    step_y = float(y[1] - y[0])
    origin_x = float(x[0]) - step_x / 2.0
    origin_y = float(y[0]) - step_y / 2.0
    return [step_x, 0.0, origin_x, 0.0, step_y, origin_y]


def bounding_box(ds: xr.Dataset) -> list[float]:
    """Return ``[west, south, east, north]`` covering the grid's outer edges.

    Args:
        ds: A dataset with ``y`` and ``x`` coordinates.

    Returns:
        The bounding box in the stored CRS.

    Raises:
        ValueError: If the grid has fewer than two cells on an axis.
    """
    step_x, _, origin_x, _, step_y, origin_y = grid_transform(ds)
    far_x = origin_x + step_x * ds.sizes["x"]
    far_y = origin_y + step_y * ds.sizes["y"]
    return [min(origin_x, far_x), min(origin_y, far_y), max(origin_x, far_x), max(origin_y, far_y)]


def geozarr_attrs(ds: xr.Dataset) -> dict[str, Any]:
    """Build the GeoZarr root attributes for a dataset.

    Args:
        ds: A normalized dataset with dims ``(time, y, x)``.

    Returns:
        The attribute mapping to write at the store root.

    Raises:
        ValueError: If the grid is too small to place.
    """
    return {
        "spatial:transform": grid_transform(ds),
        # Array order, y first: read positionally by clients. Naming these
        # x-first transposes every raster that reads the store.
        "spatial:dimensions": ["y", "x"],
        "spatial:shape": [int(ds.sizes["y"]), int(ds.sizes["x"])],
        "spatial:bbox": bounding_box(ds),
        "proj:code": CRS,
        "zarr_conventions": [{"name": "geozarr", "version": "0.4"}],
    }


def temporal_extent(ds: xr.Dataset) -> list[str]:
    """Return the ISO 8601 start and end of a dataset's time axis.

    Args:
        ds: A dataset with a ``time`` coordinate.

    Returns:
        A two-element list of ISO timestamps.

    Raises:
        ValueError: If the dataset has no time values.
    """
    if "time" not in ds.coords or ds.sizes.get("time", 0) == 0:
        raise ValueError("dataset has no time coordinate to describe")
    stamps = pd.DatetimeIndex(np.asarray(ds["time"].values))
    return [stamps[0].isoformat() + "Z", stamps[-1].isoformat() + "Z"]


def stac_collection(
    ds: xr.Dataset,
    dataset_id: str,
    *,
    title: str | None = None,
    description: str = "",
    zarr_href: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a STAC Collection document describing a published dataset.

    STAC is how a client discovers what an instance holds without knowing
    anything about its internals: one document per dataset, with spatial and
    temporal extent, variable summaries, and a link to the actual store.

    Args:
        ds: The published dataset.
        dataset_id: Stable public identifier, used as the collection id.
        title: Human-readable title; defaults to the id.
        description: Longer prose description.
        zarr_href: URL where the store is served, if it is served.
        now: Timestamp to record as the publication time; defaults to now.

    Returns:
        A STAC Collection as a plain dict, ready to serialize as JSON.

    Raises:
        ValueError: If the dataset lacks the extents STAC requires.
    """
    bbox = bounding_box(ds)
    interval = temporal_extent(ds)
    stamp = (now or datetime.now(UTC)).isoformat()

    variables: dict[str, Any] = {}
    for name, var in ds.data_vars.items():
        values = np.asarray(var.values, dtype="float64")
        variables[str(name)] = {
            "units": var.attrs.get("units", "unknown"),
            "long_name": var.attrs.get("long_name", str(name)),
            "min": round(float(np.nanmin(values)), 4),
            "max": round(float(np.nanmax(values)), 4),
        }

    collection: dict[str, Any] = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": dataset_id,
        "title": title or dataset_id,
        "description": description,
        "license": "proprietary",
        "extent": {
            "spatial": {"bbox": [bbox]},
            "temporal": {"interval": [interval]},
        },
        "summaries": {
            "variables": variables,
            "proj:code": [CRS],
        },
        "properties": {"published": stamp},
        "links": [],
    }
    if zarr_href:
        collection["assets"] = {
            "zarr": {
                "href": zarr_href,
                "type": "application/vnd+zarr",
                "roles": ["data"],
                "title": "Zarr store",
            }
        }
    return collection
