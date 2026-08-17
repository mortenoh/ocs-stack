"""DataArray anatomy: dims, coords, attrs, and the labeled data model.

What: builds an xr.DataArray from a raw numpy array and takes it apart —
values, dims, coords, attrs — showing what each layer adds over plain numpy.

Why: everything in xarray (and in open-climate-service) is built on this one
data structure. The core idea: axes get names, indices get labels, so code
says "mean over time" instead of "mean over axis 0".

Run: make run EXAMPLE=0101_dataarray_anatomy
"""

import numpy as np
import pandas as pd
import xarray as xr


def main() -> None:
    """Build a DataArray from numpy and inspect each layer of the data model."""
    # SECTION: the raw numpy starting point
    print("Plain numpy: a (3, 2, 4) array of temperatures. Which axis is time?")
    values = 20.0 + np.arange(24, dtype=np.float64).reshape(3, 2, 4)
    print(f"  shape={values.shape}, dtype={values.dtype} -- axes are anonymous positions")

    # SECTION: naming the axes
    print("\nStep 1 -- dims give every axis a name:")
    da = xr.DataArray(values, dims=("time", "y", "x"))
    print(f"  dims={da.dims}, sizes={dict(da.sizes)}")

    # SECTION: labeling the indices
    print("\nStep 2 -- coords label positions along each dim:")
    da = xr.DataArray(
        values,
        dims=("time", "y", "x"),
        coords={
            "time": pd.date_range("2024-01-01", periods=3, freq="D"),
            "y": [9.0, 8.0],  # descending latitude: north-up
            "x": [-13.0, -12.0, -11.0, -10.0],
        },
    )
    print(f"  time labels: {[str(t)[:10] for t in da.time.values]}")
    print(f"  y labels:    {da.y.values.tolist()}  (descending = north-up)")

    # SECTION: attaching metadata
    print("\nStep 3 -- attrs carry metadata; name identifies the variable:")
    da.name = "t2m"
    da.attrs = {"units": "degC", "long_name": "2 metre temperature"}
    print(f"  name={da.name!r}, attrs={da.attrs}")

    # SECTION: what the labels buy
    print("\nPayoff -- operations address dims by name, not position:")
    print(f"  da.mean()                     = {float(da.mean()):.2f}  (grand mean)")
    print(f"  da.mean(dim='time').shape     = {da.mean(dim='time').shape}  (spatial map)")
    print(f"  da.mean(dim=['y', 'x']).shape = {da.mean(dim=['y', 'x']).shape}  (time series)")
    first_day = da.sel(time="2024-01-01")
    print(f"  da.sel(time='2024-01-01')     -> shape {first_day.shape}, mean {float(first_day.mean()):.2f}")

    # SECTION: the underlying numpy is still there
    print("\nEscape hatch -- .values returns the numpy array underneath:")
    print(f"  type(da.values) = {type(da.values).__name__}, da.values[0, 0, 0] = {da.values[0, 0, 0]}")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- DataArray = numpy values + named dims + labeled coords + attrs")
    print("- dims replace positional axis numbers with names")
    print("- coords let you select by label (dates, lat/lon), not index math")
    print("- attrs carry units and descriptions; xarray never interprets them")
    print("- .values drops back to the raw numpy array")


if __name__ == "__main__":
    main()
