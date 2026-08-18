"""Pandas round-trips: to_dataframe, to_xarray, and when tabular beats N-dimensional.

What: converts a (time, y, x) Dataset to a pandas DataFrame and back, builds a
DataArray from a MultiIndex Series, and shows why sparse point data belongs in
a table while dense grids belong in xarray.

Why: open-climate-service sits between both worlds — its zarr stores are dense
(time, y, x) cubes, but request/response payloads and station-style inputs are
tabular. to_dataframe/to_xarray are the bridge, and knowing when the dense cube
representation explodes with NaN tells you which side of the bridge to live on.

Run: make run EXAMPLE=0103_from_pandas
"""

import numpy as np
import pandas as pd

from ocs_stack_xarray import temperature_dataset


def main() -> None:
    """Round-trip between xarray and pandas and compare the two data models."""
    # SECTION: dataset -> dataframe
    print("A small (time, y, x) temperature cube, flattened to a table:")
    ds = temperature_dataset(days=4, ny=3, nx=4)
    print(f"  cube sizes: {dict(ds.sizes)}  ({ds.t2m.size} values)")
    df = ds.to_dataframe()
    print(f"  ds.to_dataframe() -> DataFrame shape {df.shape}, MultiIndex levels {list(df.index.names)}")
    print("  every (time, y, x) cell becomes one row:")
    print("    " + "\n    ".join(df.head(3).to_string().splitlines()))

    # SECTION: dataframe -> dataset
    print("\nRound-trip back with to_xarray() -- the MultiIndex is unstacked into dims:")
    back = df.to_xarray()
    print(f"  df.to_xarray() -> sizes {dict(back.sizes)}, data_vars {list(back.data_vars)}")
    print(f"  values and coords survive:  ds.equals(back)    = {ds.equals(back)}")
    print(f"  attrs do NOT survive:       ds.identical(back) = {ds.identical(back)}")
    print(f"  (back.t2m.attrs = {back['t2m'].attrs} -- units/long_name were dropped by pandas)")

    # SECTION: series with a MultiIndex
    print("\nA MultiIndex Series converts too -- index levels become dims:")
    idx = pd.MultiIndex.from_product([["s1", "s2"], [0, 6, 12, 18]], names=["station", "hour"])
    series = pd.Series(np.round(np.linspace(24.0, 31.0, 8), 1), index=idx, name="t2m")
    da = series.to_xarray()
    print(f"  Series of {series.size} rows -> DataArray dims {da.dims}, sizes {dict(da.sizes)}")
    print(f"  da.sel(station='s2', hour=12) = {float(da.sel(station='s2', hour=12)):.1f}")

    # SECTION: when tabular beats N-dimensional
    print("\nWhen tabular wins: sparse station observations, most (station, time) pairs empty:")
    obs = pd.DataFrame(
        {
            "station": ["s1", "s1", "s2", "s3"],
            "time": pd.to_datetime(["2024-01-01", "2024-01-03", "2024-01-02", "2024-01-05"]),
            "t2m": [25.0, 26.0, 24.5, 27.0],
        }
    ).set_index(["station", "time"])
    dense = obs.to_xarray()
    n_nan = int(dense["t2m"].isnull().sum())
    print(f"  table: {len(obs)} rows, zero waste")
    print(f"  cube:  sizes {dict(dense.sizes)} = {dense['t2m'].size} cells, {n_nan} of them NaN padding")
    print("  the cube materializes every station x time combination -- sparse data pays for cells it never had")
    print("  dense grids are the opposite: every cell exists, so (time, y, x) cubes waste nothing")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- to_dataframe() flattens dims into a MultiIndex; one row per cell")
    print("- to_xarray() unstacks a MultiIndex back into dims; values round-trip, attrs do not")
    print("- a MultiIndex Series becomes a DataArray with one dim per index level")
    print("- sparse point/event data: keep it tabular (cube = NaN padding)")
    print("- dense grids like OCS (time, y, x) stores: keep them N-dimensional")


if __name__ == "__main__":
    main()
