# dask

Learning dask from first principles — delayed and task graphs, dask.array,
schedulers and distributed, dask.dataframe, dask-backed xarray, and
production chunking patterns — aimed at understanding how open-climate-service
executes on it. See `ROADMAP.md` for the syllabus.

## Usage

```bash
make install                       # uv sync
make run EXAMPLE=0101_delayed_basics
make run-all                       # run every example
make lint test                     # ruff + mypy + pyright, pytest
make docs-serve                    # mkdocs at http://127.0.0.1:8000
```
