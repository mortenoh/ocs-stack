"""A miniature climate service: ingest, store, derive, publish."""

from playground_climate_pipeline.indices import (
    climatological_normal,
    hot_days,
    monthly_anomaly,
    monthly_total,
    pyramid_levels,
    spi_like,
    wet_days,
)
from playground_climate_pipeline.ingest import (
    IngestReport,
    committed_periods,
    ingest,
    ingest_period,
    store_path,
)
from playground_climate_pipeline.normalize import normalize
from playground_climate_pipeline.publish import (
    bounding_box,
    geozarr_attrs,
    grid_transform,
    stac_collection,
    temporal_extent,
)
from playground_climate_pipeline.sources import (
    Period,
    enumerate_periods,
    fetch_precipitation,
    fetch_temperature,
)

__all__ = [
    "IngestReport",
    "Period",
    "bounding_box",
    "climatological_normal",
    "committed_periods",
    "enumerate_periods",
    "fetch_precipitation",
    "fetch_temperature",
    "geozarr_attrs",
    "grid_transform",
    "hot_days",
    "ingest",
    "ingest_period",
    "monthly_anomaly",
    "monthly_total",
    "normalize",
    "pyramid_levels",
    "spi_like",
    "stac_collection",
    "store_path",
    "temporal_extent",
    "wet_days",
]
