# playground-data-xarray

Learning xarray end to end — data model, selection, computation, combining,
netCDF/zarr I/O, and dask-backed lazy evaluation — aimed at understanding how
[open-climate-service](https://github.com/dhis2/open-climate-service) uses it.

The syllabus lives in `ROADMAP.md`: seven phases from DataArray anatomy to
`map_blocks`, each example a self-contained runnable lesson.

## Quick start

```bash
make install
make run EXAMPLE=0101_dataarray_anatomy
make run-all
```

## Shared helpers

Examples draw on synthetic climate datasets shaped like OCS stores — dims
`(time, y, x)`, CF-style attrs, deterministic per seed. See the
[API Reference](api-reference.md).
