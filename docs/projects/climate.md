# climate

**The capstone.** A miniature climate service that reassembles the other projects into one pipeline: a deliberately messy source normalized, ingested into a versioned store one period per commit, derived into indices, and published with GeoZarr attributes and a STAC collection.

```bash
cd climate
make install
make run-all
```

10 examples, in phases:

### Phase 1 — Ingest

- `0101_normalize` — what a raw source looks like and what normalization fixes
- `0102_streaming_ingest` — one period, one commit; the store grows a month at a time
- `0103_resume` — interrupt an ingest and resume it from what the store actually holds

### Phase 2 — Derive

- `0201_climatology` — monthly normals and anomalies over the ingested series
- `0202_indices` — hot days, wet days, monthly totals, and a standardized rainfall index
- `0203_pyramid` — multiscale levels by 2x2 coarsening, and why a map viewer needs them

### Phase 3 — Publish

- `0301_geozarr` — the root attributes that place a grid on Earth, and the axis-order trap
- `0302_stac` — the collection document a client discovers the dataset through

### Phase 4 — The whole thing

- `0401_full_pipeline` — source to published product in one run, with every stage reported
- `0402_second_dataset` — the same pipeline over precipitation, showing the shape generalizes
