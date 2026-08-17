# playground-domain-climate

The capstone: a miniature climate service that reassembles what the `data/`
projects taught into one working pipeline, modelled on
[open-climate-service](https://github.com/dhis2/open-climate-service).

Source data arrives messy — Kelvin, `lat`/`lon`, south-up — and comes out the
other end as a versioned store with derived indices, GeoZarr placement
attributes, and a STAC collection a client can discover.

```text
source  ->  normalize  ->  icechunk store  ->  indices  ->  publish
(K, lat/lon,   (degC,        (one commit      (hot days,   (GeoZarr attrs,
 south-up)      time/y/x)     per period)      SPI, ...)    STAC collection)
```

## Quick start

```bash
make install
make run EXAMPLE=0401_full_pipeline   # the whole thing, start to finish
make run-all
```

## The library

| Module | Role |
|---|---|
| `sources` | Synthetic sources in awkward conventions; period enumeration |
| `normalize` | Source conventions to canonical `(time, y, x)`, degC/mm, north-up |
| `ingest` | Per-period append into icechunk, with resume |
| `indices` | Climatology, anomalies, hot/wet days, SPI-like index, pyramid levels |
| `publish` | GeoZarr root attributes and the STAC collection document |

See the [API Reference](api-reference.md), and `ROADMAP.md` for the syllabus.
