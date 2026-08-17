# Relationship to open-climate-service

[OCS](https://github.com/dhis2/open-climate-service) is a climate data
platform: each instance is scoped to one country, ingests from sources like
CHIRPS and ERA5, stores results as GeoZarr in icechunk, and exposes them
through STAC, Zarr over HTTP, and openEO. Every project here was chosen
because OCS depends on it.

| OCS does this | Learn it here |
|---|---|
| Normalizes every source to `(time, y, x)`, Kelvin to Celsius | `climate-pipeline/examples/0101_normalize.py` |
| Ingests one period at a time, committing each | `icechunk/examples/0401_append_periods.py` |
| Resumes an interrupted ingest from committed time steps | `icechunk/examples/0402_resume.py` |
| Builds multiscale pyramids by mean downsampling | `xarray/examples/0305_rolling_coarsen.py` |
| Rechunks to Zarr-legal uniform chunks before writing | `dask/examples/0601_zarr_legal_chunks.py` |
| Writes GeoZarr root attributes so clients can place the grid | `climate-pipeline/examples/0301_geozarr.py` |
| Publishes a STAC collection per dataset | `climate-pipeline/examples/0302_stac.py` |
| Runs openEO process graphs on dask | `dask/` phases 1–3 |

Two things here are deliberate re-implementations of OCS code, kept close to
the original: the `_uniform_chunks` fix in
`dask/examples/0601_zarr_legal_chunks.py`, and the open-or-create plus
commit-and-append pattern in `icechunk/src/climate_stack_icechunk/helpers.py`.

The [climate-pipeline](projects/climate-pipeline.md) project is the whole shape in miniature — a
messy source normalized, ingested as one commit per period, derived into
climatologies and indices, and published with GeoZarr attributes and a STAC
collection whose extents are read back off the store rather than declared. Run
`make run EXAMPLE=0401_full_pipeline` there to see all six stages in one pass.

## Groundwork for the planned work

Two extensions are planned for OCS. The projects that prepare for them:

### Icechunk on S3

OCS currently calls `icechunk.local_filesystem_storage` only. The
[Storage](storage.md) page works through when that stops being adequate, and
the short version is: a commit is compare-and-swap on a branch pointer, so one
committer at a time is correct on a local filesystem, and object storage
becomes necessary the moment compute spans machines or a second writer appears.

Three findings from the `icechunk` project bear directly on the move:

- Local filesystem storage warns on every open that it **is not safe for
  concurrent commits** and recommends an object store. That warning is the
  argument for migrating.
- Commits need a conditional write. Object stores provide it; POSIX has no
  portable equivalent. `icechunk/examples/0303_conflicts.py` shows both
  outcomes — a rebase that succeeds and one that cannot.
- icechunk shares chunks *by reference*, not by content hash. Appends cost one
  period each, but rewriting a store with byte-identical values costs a full
  copy. `0501_storage_growth.py` measures it; on metered object storage the
  distinction is a bill.

### Distributed dask

The [dask-distributed](projects/dask-distributed.md) project is a working
cluster in Docker Compose, and its lessons are the deployment questions:

- **Client and workers must run the same library versions.** The base image was
  one patch behind on numpy and warned on every connect; the Dockerfile pins to
  `uv.lock`. An API image and a worker image need the same discipline.
- **Worker filesystems are not the client's.**
  `0302_shared_storage.py` shows a file the client just wrote reporting `False`
  on all three workers.
- **A dask graph carries one path string, used by client and workers alike.**
  This is the sharpest argument for object storage, and it ties the two planned
  extensions together: an `s3://` URL that resolves identically on both sides
  dissolves a problem that no amount of volume-mounting solves cleanly.

Worth knowing before starting: icechunk's distributed write model is
**fork/merge** — the coordinator forks a session per worker, workers write
chunks in parallel, and the coordinator merges and commits once. Many writers,
one committer. The concurrency hazard is therefore not the cluster itself but
two independent jobs committing to the same branch.

## One trap worth knowing before extending OCS

Appending variable-length months to a store chunked at 30 days along time fails
outright once the final chunk is partial:

> `ValueError: Specified Zarr chunks encoding['chunks']=(30, 32, 32) for
> variable named 't2m' would overlap multiple Dask chunks`

It is not a corner case — it appeared independently in the `climate-pipeline` and
`icechunk` projects, on the same period, the fifth month in both. The fix is
`align_chunks=True` on the append, and both projects demonstrate it rather than
working around it (`icechunk/examples/0401_append_periods.py` probes each
period and reports honestly which ones would have succeeded unaligned). It is
the same family as the `_uniform_chunks` problem: dask and zarr disagree about
what a legal chunk layout is, and the disagreement surfaces at write time.
