# xarray

**Labeled arrays.** Labeled N-dimensional arrays, from the data model through to dask-backed lazy evaluation. The foundation every other project builds on: dimensions get names, positions get labels, and operations address them by name.

```bash
cd xarray
make install
make run-all
```

25 examples, in phases:

### Phase 1 — Anatomy and creation

- `0101_dataarray_anatomy` — build a DataArray from numpy; dims, coords, attrs, values
- `0102_dataset_construction` — Dataset as a dict of variables sharing dims; inspection
- `0103_from_pandas` — Series/DataFrame round-trips; when tabular beats N-dimensional

### Phase 2 — Selection and indexing

- `0201_isel_sel` — positional vs label indexing; slices on time
- `0202_nearest_and_interp` — `method="nearest"`, tolerance, `interp`
- `0203_masking` — boolean masks, `where`, `isin`, dropping vs keeping NaN

### Phase 3 — Computation

- `0301_arithmetic_broadcasting` — broadcasting by dim name, not position
- `0302_reductions` — mean/sum/std over named dims; skipna
- `0303_groupby_climatology` — `groupby("time.month")`, climatological normals
- `0304_resample` — daily → monthly resampling; ISO period thinking
- `0305_rolling_coarsen` — rolling windows; `coarsen` as pyramid downsampling

### Phase 4 — Alignment and combining

- `0401_alignment` — automatic index alignment, `align`, joins
- `0402_concat_append` — `concat` along time; the append-a-period pattern
- `0403_merge_combine` — `merge`, `combine_by_coords`

### Phase 5 — I/O: netCDF and zarr

- `0501_netcdf` — `to_netcdf`/`open_dataset`; engines
- `0502_zarr_basics` — `to_zarr`/`open_zarr`; what a store looks like on disk
- `0503_zarr_chunks_encoding` — chunk sizing, compressors, encoding pitfalls
- `0504_zarr_append_region` — `append_dim` and `region` writes

### Phase 6 — Dask-backed xarray

- `0601_chunked_open` — `chunks=` at open time; the dask repr; nothing computes
- `0602_lazy_graphs` — operations build graphs; `.compute()` vs `.persist()`
- `0603_rechunking` — `chunk()`, zarr-legal uniform chunks, when rechunking bites
- `0604_map_blocks_apply_ufunc` — escaping to numpy per block

### Phase 7 — Conventions and interop

- `0701_cf_attrs_units` — CF attribute conventions; units metadata
- `0702_time_handling` — datetime64 coords, calendars, `dt` accessor
- `0703_missing_data` — NaN vs `_FillValue`, `fillna`, `interpolate_na`
