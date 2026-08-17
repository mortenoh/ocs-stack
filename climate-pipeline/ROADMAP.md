# climate roadmap

The capstone: a miniature climate service that reassembles everything the
`data/` projects taught into one working pipeline, modelled on
[open-climate-service](https://github.com/dhis2/open-climate-service).

Source data arrives messy (Kelvin, `lat`/`lon`, south-up). It gets normalized,
appended to a versioned icechunk store one period at a time, derived into
climate indices, and published with GeoZarr attributes and a STAC collection.
Each phase is one stage of that pipeline.

The library lives in `src/climate_stack_climate_pipeline/`:

| Module | Role |
|---|---|
| `sources.py` | Synthetic sources, deliberately in awkward conventions; period enumeration |
| `normalize.py` | Source conventions to canonical `(time, y, x)`, degC/mm, north-up |
| `ingest.py` | Streaming per-period append into icechunk, with resume |
| `indices.py` | Climatology, anomalies, hot/wet days, an SPI-like index, pyramid levels |
| `publish.py` | GeoZarr root attributes and the STAC collection document |

## Phase 1 — Ingest

- [x] `0101_normalize` — what a raw source looks like and what normalization fixes
- [x] `0102_streaming_ingest` — one period, one commit; the store grows a month at a time
- [x] `0103_resume` — interrupt an ingest and resume it from what the store actually holds

## Phase 2 — Derive

- [x] `0201_climatology` — monthly normals and anomalies over the ingested series
- [x] `0202_indices` — hot days, wet days, monthly totals, and a standardized rainfall index
- [x] `0203_pyramid` — multiscale levels by 2x2 coarsening, and why a map viewer needs them

## Phase 3 — Publish

- [x] `0301_geozarr` — the root attributes that place a grid on Earth, and the axis-order trap
- [x] `0302_stac` — the collection document a client discovers the dataset through

## Phase 4 — The whole thing

- [x] `0401_full_pipeline` — source to published product in one run, with every stage reported
- [x] `0402_second_dataset` — the same pipeline over precipitation, showing the shape generalizes
