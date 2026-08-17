# xarray roadmap

Learning xarray end to end, aimed at understanding how
[open-climate-service](https://github.com/dhis2/open-climate-service) (OCS)
uses it: every phase notes what it unlocks in that codebase. The destination
is dask-backed lazy xarray over zarr stores — the follow-up projects
`data/dask` and `data/icechunk` build on this one.

Shared helpers live in `src/playground_data_xarray/`: synthetic climate
datasets shaped like OCS stores — dims `(time, y, x)`, CF-style attrs.

## Phase 1 — Anatomy and creation

The DataArray/Dataset data model: dims, coords, attrs, and how labeled
dimensions replace positional axis bookkeeping. OCS relevance: everything in
OCS is an `xr.Dataset` with dims normalized to `(time, y, x)`.

- [x] `0101_dataarray_anatomy` — build a DataArray from numpy; dims, coords, attrs, values
- [x] `0102_dataset_construction` — Dataset as a dict of variables sharing dims; inspection
- [x] `0103_from_pandas` — Series/DataFrame round-trips; when tabular beats N-dimensional

## Phase 2 — Selection and indexing

- [x] `0201_isel_sel` — positional vs label indexing; slices on time
- [x] `0202_nearest_and_interp` — `method="nearest"`, tolerance, `interp`
- [x] `0203_masking` — boolean masks, `where`, `isin`, dropping vs keeping NaN

## Phase 3 — Computation

OCS relevance: `coarsen` is literally how pyramid levels are built (mean
downsampling); groupby/resample power the climatology processes.

- [x] `0301_arithmetic_broadcasting` — broadcasting by dim name, not position
- [x] `0302_reductions` — mean/sum/std over named dims; skipna
- [x] `0303_groupby_climatology` — `groupby("time.month")`, climatological normals
- [x] `0304_resample` — daily → monthly resampling; ISO period thinking
- [x] `0305_rolling_coarsen` — rolling windows; `coarsen` as pyramid downsampling

## Phase 4 — Alignment and combining

OCS relevance: streaming ingestion appends one period at a time — `concat`
along time and index alignment are the core mechanics.

- [x] `0401_alignment` — automatic index alignment, `align`, joins
- [x] `0402_concat_append` — `concat` along time; the append-a-period pattern
- [x] `0403_merge_combine` — `merge`, `combine_by_coords`

## Phase 5 — I/O: netCDF and zarr

OCS relevance: sources arrive as netCDF/GRIB, stores are written as Zarr v3
with explicit chunk encoding; `append_dim` and `region` are how periods land.

- [x] `0501_netcdf` — `to_netcdf`/`open_dataset`; engines
- [x] `0502_zarr_basics` — `to_zarr`/`open_zarr`; what a store looks like on disk
- [x] `0503_zarr_chunks_encoding` — chunk sizing, compressors, encoding pitfalls
- [x] `0504_zarr_append_region` — `append_dim` and `region` writes

## Phase 6 — Dask-backed xarray

The payoff phase: the same API, lazy. OCS relevance: stores are opened
chunked; computation is deferred until write/compute.

- [x] `0601_chunked_open` — `chunks=` at open time; the dask repr; nothing computes
- [x] `0602_lazy_graphs` — operations build graphs; `.compute()` vs `.persist()`
- [x] `0603_rechunking` — `chunk()`, zarr-legal uniform chunks, when rechunking bites
- [x] `0604_map_blocks_apply_ufunc` — escaping to numpy per block

## Phase 7 — Conventions and interop

- [x] `0701_cf_attrs_units` — CF attribute conventions; units metadata
- [x] `0702_time_handling` — datetime64 coords, calendars, `dt` accessor
- [x] `0703_missing_data` — NaN vs `_FillValue`, `fillna`, `interpolate_na`
