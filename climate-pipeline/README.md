# climate

The capstone project: a miniature climate service, modelled on
[open-climate-service](https://github.com/dhis2/open-climate-service), that
puts the `data/` projects to work in one pipeline.

```text
source  ->  normalize  ->  icechunk store  ->  indices  ->  publish
(K, lat/lon,   (degC,        (one commit      (hot days,   (GeoZarr attrs,
 south-up)      time/y/x)     per period)      SPI, ...)    STAC collection)
```

Every stage is a lesson from an earlier project made concrete: xarray's data
model and `coarsen` for pyramids, dask's chunk alignment on append, and
icechunk's per-period commits with resume.

## Usage

```bash
make install                          # uv sync
make run EXAMPLE=0401_full_pipeline   # the whole pipeline in one run
make run-all                          # every example
make lint test                        # ruff + mypy + pyright, pytest
```

See `ROADMAP.md` for the syllabus.

Full documentation: [`docs/projects/climate-pipeline.md`](../docs/projects/climate-pipeline.md)
(`make docs-serve` at the repository root renders the whole site).
