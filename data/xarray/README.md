# xarray

Learning xarray end to end — data model, selection, computation, combining,
netCDF/zarr I/O, and dask-backed lazy evaluation — aimed at understanding how
open-climate-service uses it. See `ROADMAP.md` for the syllabus.

## Usage

```bash
make install                       # uv sync
make run EXAMPLE=0101_dataarray_anatomy
make run-all                       # run every example
make lint test                     # ruff + mypy + pyright, pytest
make docs-serve                    # mkdocs at http://127.0.0.1:8000
```
