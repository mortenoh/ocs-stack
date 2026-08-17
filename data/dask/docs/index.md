# playground-data-dask

Learning dask from first principles to the patterns
[open-climate-service](https://github.com/dhis2/open-climate-service) depends
on: delayed and task graphs, dask.array, schedulers and distributed,
dask.dataframe, dask-backed xarray, and production chunking patterns.

The syllabus lives in `ROADMAP.md`: six phases, each example a self-contained
runnable lesson.

## Quick start

```bash
make install
make run EXAMPLE=0101_delayed_basics
make run-all
```

## Shared helpers

Examples draw on chunked synthetic fields shaped like a climate zarr store
and small graph-inspection utilities. See the [API Reference](api-reference.md).
