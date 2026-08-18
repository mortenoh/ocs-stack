# xarray

**Labeled N-dimensional arrays.** This project is a 25-example teaching track
through xarray, from the raw data model up to dask-backed lazy evaluation over
zarr stores. It exists to make one library thoroughly understood before the
projects that sit on top of it — `dask`, `dask-distributed`, `icechunk`, and the
`climate-pipeline` capstone — because every one of them manipulates the same
objects: a `Dataset` whose dimensions are named `(time, y, x)`, whose
coordinates are real dates and real degrees, and whose values may or may not
have been read from disk yet. The examples are deliberately concrete: they use
synthetic climate cubes shaped exactly like the stores
[open-climate-service](https://github.com/dhis2/open-climate-service) (OCS)
writes, so that a lesson about `coarsen` is simultaneously a lesson about how
GeoZarr pyramid levels get built.

---

## Introduction: what xarray is for

If you have never used xarray, this section is the one that matters. Everything
after it is mechanics. This one is the mental model, and without it the API
looks like a large pile of methods with confusing names.

### The problem, stated precisely

Take a perfectly ordinary piece of climate data: daily 2 metre air temperature
over a country, for a year, on a grid. In numpy that is a single array:

```python
import numpy as np

values = np.zeros((365, 20, 30), dtype="float64")
```

Three numbers, `365`, `20`, `30`, and nothing else. The array knows its shape.
It does not know:

- that axis 0 is time, and that step 0 is 2024-01-01,
- that axis 1 is latitude, running north to south, from 10.0 down to 6.9,
- that axis 2 is longitude, running west to east, from -13.5 to -10.3,
- that the values are degrees Celsius rather than Kelvin,
- that missing observations are encoded as `NaN` rather than `-9999`.

Every one of those facts is real, load-bearing, and required to do anything
correct with the array. In pure numpy they live in three places, all of them
bad: in variable names (`temp_daily_c`), in comments, and in your head. So the
code that uses the array reads like this:

```python
# axis 0 is time, axis 1 is lat (north first!), axis 2 is lon
spatial_mean = values.mean(axis=0)          # a map
time_series = values.mean(axis=(1, 2))      # a series
february = values[31:60]                     # 2024 is a leap year, so 31:60
anomaly = values - values.mean(axis=0)       # broadcasting works by luck of shape
```

Look at that `february` line. It is a hardcoded offset that silently becomes
wrong if the data starts on a different day, if a day is missing, or if
somebody hands you 2023 instead of 2024. Look at the `anomaly` line: it works
because `(20, 30)` happens to right-align against `(365, 20, 30)`. If the
array had been `(20, 30, 365)` — a perfectly common layout, "band-last" — the
same expression would still run, still produce an array, and be completely
wrong.

That last point is the whole argument. Positional array code does not fail
loudly when the axes are wrong. It produces numbers.

### The idea

xarray's proposal is small and it is the entire library: **attach the labels to
the array**.

A `DataArray` is a numpy array plus three things:

| Layer | What it adds | Example |
|---|---|---|
| `dims` | a **name** for every axis | `("time", "y", "x")` |
| `coords` | **labels** for every position along a dim | `time = 2024-01-01 .. 2024-12-31` |
| `attrs` | arbitrary **metadata** riding along | `{"units": "degC"}` |

Once those exist, the four operations above stop being positional:

```python
spatial_mean = da.mean(dim="time")
time_series = da.mean(dim=["y", "x"])
february = da.sel(time="2024-02")
anomaly = da - da.mean(dim="time")
```

None of these depend on axis order. `da.mean(dim="time")` is correct whether
time is the first axis or the last. `da.sel(time="2024-02")` is correct whether
2024 is a leap year or not, whether the series starts in January or in
mid-March, whether days are missing. And the anomaly line broadcasts by
matching the dimension *names* `y` and `x`, not by right-aligning shapes — so
there is no arrangement of the same data that makes it silently wrong.

That is the payoff, and it is worth being blunt about the size of it: the
value of xarray is not that it saves keystrokes. It is that a large class of
plausible-looking mistakes becomes impossible to express.

### A `Dataset` is a bag of `DataArray`s on a shared grid

Real data is rarely one variable. A climate store has temperature *and*
precipitation *and* maybe humidity, all on the same `(time, y, x)` grid,
sharing the same time axis and the same coordinates.

`Dataset` is the container for that: a dict-like collection of `DataArray`s
that share dimensions and coordinates.

```python
import xarray as xr

from ocs_stack_xarray import precipitation_dataset, temperature_dataset

ds = xr.merge([temperature_dataset(days=31), precipitation_dataset(days=31)])
print(list(ds.data_vars))       # ["t2m", "tp"]
print(dict(ds.sizes))           # {"time": 31, "y": 20, "x": 30}

monthly = ds.mean(dim=["y", "x"])   # both variables, one call
```

The coordinates are stored **once** and shared, not duplicated per variable.
Selection, reduction, alignment, and I/O all operate on the whole `Dataset`.
This is what opening a climate zarr store gives you, so it is the object almost
all real code manipulates.

If you want a one-line summary of the two types, it is this:

- **`DataArray`** = one labeled array. The unit of computation.
- **`Dataset`** = several `DataArray`s on one shared coordinate system. The unit
  of storage and of I/O.

### How this relates to numpy

xarray does not replace numpy. It **wraps** it. Under every `DataArray` there
is a real array, reachable at any time:

```python
da.values      # the numpy ndarray
da.data        # the backing array: numpy, dask, cupy, ... whatever it is
```

The relationship in practice:

| numpy | xarray |
|---|---|
| `arr.shape` | `da.shape`, or better `da.sizes` (a name to size mapping) |
| `arr.mean(axis=0)` | `da.mean(dim="time")` |
| `arr[3]` | `da.isel(time=3)` (positional) |
| — | `da.sel(time="2024-02-04")` (by label; no numpy equivalent) |
| `arr[arr > 0]` | `da.where(da > 0)` — keeps shape, inserts NaN |
| broadcasting by trailing shape | broadcasting by dimension **name** |
| `np.concatenate` | `xr.concat` — but aligns coordinates too |
| `arr.T`, `np.moveaxis` | `da.transpose("x", "y", "time")` by name |

Two differences deserve emphasis because they change how you write code.

**Broadcasting is by name, and it is an outer product over the union of dims.**
In numpy, shapes are right-aligned and a dimension of size 1 is stretched. In
xarray, operands are matched dim-name by dim-name, and any dim present in only
one operand appears in the result:

```python
series = t2m.mean(dim=["y", "x"])   # dims ("time",)
spatial = t2m.mean(dim="time")      # dims ("y", "x")
outer = series * spatial            # dims ("time", "y", "x")
```

There is no `np.newaxis`, no `reshape(-1, 1, 1)`, and no way to get the pairing
wrong. Dim order in the operands is irrelevant.

**Indexing is by label, and label lookup is a first-class operation.** numpy
has no notion of "the row whose label is 2024-02-04". xarray does, because each
dimension coordinate is backed by a real pandas index. That index is what makes
`sel`, slicing by date string, `method="nearest"`, `tolerance=`, alignment,
`reindex`, and `groupby` possible at all.

### How this relates to pandas

The honest one-sentence version: **xarray is pandas generalised past two
dimensions**, and much of the label machinery is literally pandas underneath.

The lineage is visible everywhere. Dimension coordinates are `pandas.Index`
objects. `sel` is `loc`. `groupby` is `groupby`. `resample` is `resample`, with
the same frequency strings (`"D"`, `"MS"`, `"1ME"`, `"W-MON"`). `rolling` is
`rolling`. `fillna`, `interpolate_na`, `isnull`, `notnull`, `count` — all
familiar. If you know pandas, you already know a third of the xarray API and
you will guess correctly most of the rest.

Where they diverge:

| | pandas | xarray |
|---|---|---|
| Natural shape | 2-D table (rows x columns) | N-D cube |
| Higher dims | `MultiIndex`, `stack`/`unstack` | native `dims` |
| Best for | heterogeneous columns, sparse records | homogeneous numeric grids |
| Missing cells | rows simply absent | must be materialised, as NaN |
| Storage | CSV, Parquet | netCDF, Zarr |

The decision rule is about **density**, and example
[`0103_from_pandas.py`](../../xarray/examples/0103_from_pandas.py) measures it
directly. A dense grid — every `(time, y, x)` cell has a value — costs nothing
to store as a cube and everything to store as a table (one row per cell, with
the coordinates repeated on every row). Sparse point observations are the exact
opposite: four station readings become a table of 4 rows, or a cube of 12
cells, 8 of which are NaN padding that exists only because the cube must be
rectangular.

Both directions are one call — `to_dataframe()` and `to_xarray()` — so you are
never locked in. Just do not push sparse data into a cube because "xarray is
the array library". The NaN padding grows as the product of the index
cardinalities.

### Why labeled dimensions matter, concretely

Five things become straightforward that are awkward or unsafe without labels.

**1. Reductions read as intent.** `ds.mean(dim="time")` is "the mean map".
`ds.mean(dim=["y", "x"])` is "the area-mean series". Neither requires knowing,
or preserving, axis order. Six transformations later, when a `transpose` or a
`stack` has moved things around, both are still correct.

**2. Selection stops being index arithmetic.** `ds.sel(time="2024-02")` is
February — all 29 days of it in 2024, all 28 in 2023, and it does not care that
`2024-02` is a partial date string rather than a full timestamp. There is no
offset to compute and therefore no offset to get wrong.

**3. Alignment happens automatically, and consistently.** When two objects meet
in an expression, xarray reconciles their indexes first. Add a 21-day dataset
to a 17-day one that overlaps by 7 days and you get 7 days out — the
intersection. This is a two-edged property (see the pitfalls section) but it
means you cannot accidentally add day 5 of one array to day 12 of another.

**4. Broadcasting cannot pair the wrong axes.** Covered above; it is worth
repeating because it is the single most common silent bug in positional array
code.

**5. Metadata survives the pipeline.** `attrs` ride along through operations, so
the `units` and `long_name` you attached at ingest are still on the variable ten
steps later when it gets written to a store. With one enormous caveat, which is
the first thing to internalise about xarray:

!!! note "attrs are inert"
    xarray **never interprets** `attrs`. It does not read `units`. It will add a
    Celsius array to a Kelvin array and give you a number, silently. `attrs` are
    a contract between the producer and the consumer of the data, enforced by
    neither. See
    [`0701_cf_attrs_units.py`](../../xarray/examples/0701_cf_attrs_units.py).

### The other half: laziness

Everything above is about labels. There is a second, independent reason xarray
is the standard tool for climate data, and it only shows up once the data stops
fitting in memory.

A `DataArray` does not have to be backed by numpy. It can be backed by a
**dask** array — a grid of chunks plus a task graph describing how to produce
them. When it is, the entire API above still works, but nothing computes:

```python
ds = xr.open_zarr("store.zarr")            # reads metadata only, milliseconds
anomaly = ds.t2m - ds.t2m.mean("time")     # builds a graph, computes nothing
monthly = anomaly.resample(time="MS").mean()  # more graph, still nothing
monthly.to_zarr("out.zarr")                # NOW it runs, chunk by chunk
```

The consequences are large enough to be worth stating explicitly:

- Opening a store is **free**, regardless of its size, because opening reads
  only the small JSON metadata files.
- Building a pipeline is **free**. Chaining a dozen operations on a 100 GB
  dataset takes milliseconds.
- The cost arrives all at once, at the point where you force materialisation:
  a write, a `.compute()`, a plot, a `float()`.
- Memory use is governed by chunk size and parallelism, not by dataset size.

Example [`0602_lazy_graphs.py`](../../xarray/examples/0602_lazy_graphs.py)
measures this: the graph grows from 52 tasks to 180 to 460 while cumulative
build time stays around 10 ms, and then 460 tasks execute in about 0.05 s.
Phase 6 of this project is entirely about that mode.

### When you would NOT use xarray

xarray is not a universal array library, and reaching for it in the wrong place
produces slower, more awkward code than the obvious alternative.

**Do not use it for sparse or event-shaped data.** Station observations,
irregular time series, log records, anything where "most combinations do not
exist". The cube representation materialises every combination of every index,
so the memory cost is the product of the cardinalities, most of it NaN. Use
pandas, or Parquet, or a database. `0103_from_pandas.py` shows 4 real
observations becoming 12 cells, 8 of them padding — and that ratio only gets
worse as you add dimensions.

**Do not use it for small, hot numerics.** Every xarray operation carries
bookkeeping: dimension matching, coordinate alignment, attrs propagation. On a
`(3, 4)` array in a tight loop that overhead dominates completely. Drop to
`.values` and work in numpy, or use numba, and come back to xarray at the
boundaries.

**Do not use it when there are no meaningful labels.** An image batch of
`(n, height, width, channels)` for a neural network has dimension names but no
coordinate labels worth carrying; there is nothing to `sel` by. The array
libraries built for that job (torch, jax) are the right answer.

**Do not use it for heterogeneous columns.** A `Dataset` variable is one dtype
over one grid. A table with a string column, a categorical, a timestamp, and
three floats is a DataFrame.

**Do not use it as a database.** No transactions, no concurrent-write safety, no
query planner. (Which is exactly what the `icechunk` project in this repository
is about: adding transactions underneath.)

**Be careful with very many small variables.** A `Dataset` with thousands of
data variables works, but repr, alignment, and merge all become noticeably slow.
That is usually a sign the data wants an extra dimension instead of an extra
hundred variables.

The shape that fits xarray perfectly: **dense, homogeneous, numeric, on a
regular N-dimensional grid, where every axis means something you can name.**
Climate data is the archetype, which is why the library grew up there.

### Official documentation

This page teaches the subset of xarray that this project's 25 examples cover,
with real output from real runs. It is not a replacement for upstream docs.

- **User guide and everything else:** <https://docs.xarray.dev/>
- **Tutorial (notebook-based, run it yourself):** <https://tutorial.xarray.dev/>
- **API reference (every method, every argument):**
  <https://docs.xarray.dev/en/stable/api.html>

When the two disagree, upstream is right and this page is out of date.

---

## Setup

The project is self-contained: its own `pyproject.toml`, its own `.venv`, its
own `uv.lock`. There is no root package and no uv workspace, so nothing needs
to be installed at the repository level.

```bash
cd xarray
make install                                    # uv sync
make run EXAMPLE=0101_dataarray_anatomy         # run one example
make run-all                                    # run all 25, in order
make test                                       # pytest
make lint                                       # ruff format + check, mypy, pyright
make ci                                         # lint + test
```

`make run` requires the `EXAMPLE` variable and fails with a usage message
without it. The name is the example's filename without the `.py`:

```bash
make run EXAMPLE=0504_zarr_append_region
```

Under the hood every target is `uv run`, so you can bypass the Makefile
entirely when you want to poke at something:

```bash
cd xarray
uv run python examples/0602_lazy_graphs.py
uv run python -c "import xarray as xr; print(xr.__version__)"
```

### What is installed

The dependency set is deliberately small and pinned by `uv.lock`:

| Package | Why |
|---|---|
| `xarray` | the subject |
| `dask[array,diagnostics]` | the lazy backend used in phase 6 |
| `zarr` | Zarr v3 stores, phases 5 and 6 |
| `netcdf4` | the netCDF engine, phase 5 |
| `scipy` | required by `interp()`, phase 2 |

Versions on the machine these outputs were captured on — worth recording,
because a few of the printed reprs are version-sensitive:

```text
python 3.13.14
xarray 2026.7.0   numpy 2.5.2   pandas 3.0.5   dask 2026.7.1   zarr 3.3.0
```

### The shared helpers

Every example imports from `src/ocs_stack_xarray/synthetic.py`
([source](../../xarray/src/ocs_stack_xarray/synthetic.py)) rather than
building data inline. Two generators, both returning an `xr.Dataset` with dims
`(time, y, x)`, daily time steps, CF-style attrs, and deterministic values for a
given seed:

```python
from ocs_stack_xarray import precipitation_dataset, temperature_dataset

ds = temperature_dataset(days=30, ny=20, nx=30, seed=0)     # variable "t2m", degC
ds = precipitation_dataset(days=30, ny=20, nx=30, seed=0)   # variable "tp", mm/day
```

The design of those generators is not incidental — each choice exists to make a
later lesson land:

- **Dims `(time, y, x)`.** The normalised layout OCS writes. Every example
  therefore works on the same shape as the real thing.
- **A Sierra Leone-ish bounding box**, `(-13.5, 6.9, -10.3, 10.0)`. OCS
  instances are scoped to one country extent, so the grid mimics that.
- **`y` descending** (north at index 0). Real geospatial rasters are north-up,
  and the descending axis is what makes the axis-reversal trap in
  [`0603_rechunking.py`](../../xarray/examples/0603_rechunking.py) realistic
  rather than contrived.
- **Temperature is smooth**: a north-south gradient, a seasonal sine over
  `365.25` days, plus gaussian noise. Good for anomalies, climatologies, and
  rolling means.
- **Precipitation is zero-inflated**: roughly 60 percent of cells are exactly
  `0.0`, the rest gamma-distributed. This is the shape real rainfall has, and it
  is what makes the masking and `skipna` lessons in
  [`0203_masking.py`](../../xarray/examples/0203_masking.py) concrete rather
  than abstract.
- **Deterministic per seed**, so every number quoted on this page is
  reproducible by re-running the example.
- **Validated inputs**: `days`, `ny`, `nx` below 1 raise `ValueError` with a
  clear message.

```python
def temperature_dataset(days: int = 30, ny: int = 20, nx: int = 30, seed: int = 0) -> xr.Dataset:
    """Return a daily 2 m temperature dataset with dims (time, y, x).

    Values are degrees Celsius: a base field with a north-south gradient, a
    seasonal-ish sine over time, and gaussian noise.

    Args:
        days: Number of daily time steps; must be at least 1.
        ny: Grid height; must be at least 1.
        nx: Grid width; must be at least 1.
        seed: Seed for the random noise component.

    Returns:
        A dataset with one data variable ``t2m`` and coords time/y/x.

    Raises:
        ValueError: If days, ny, or nx is less than 1.
    """
```

### About the output on this page

Every code block on this page was run. Every quoted output is real, captured
from `uv run python examples/<name>.py` on the versions listed above. Long
output is trimmed, and trimming is always marked.

!!! note "Timings vary"
    Wall-clock numbers — "computed in 0.048 s", "opened in 5.7 ms", "6x faster"
    — are **machine-dependent** and move between runs, sometimes by a factor of
    two or more. They are quoted for the *ratios* and the *orders of magnitude*,
    which are stable. Do not treat them as benchmarks.
---

## Core concepts

Seven ideas carry the whole library: `DataArray`, `Dataset`, `dims`, `coords`,
`attrs`, `indexes`, and `encoding`. Everything else is composition. This section
takes each one in turn — what it is, why it exists, a runnable block, the real
output, and the traps.

### `DataArray` — one labeled array

**What it is.** A numpy (or dask) array, plus a name per axis, plus labels along
each axis, plus a metadata dict, plus an optional variable name.

**Why it exists.** Because a bare array cannot answer "which axis is time?" and
every question you actually want to ask starts there.

Build one from nothing:

```python
import numpy as np
import pandas as pd
import xarray as xr

values = 20.0 + np.arange(24, dtype="float64").reshape(3, 2, 4)

da = xr.DataArray(values, dims=("time", "y", "x"))
print(repr(da))
```

```text
<xarray.DataArray (time: 3, y: 2, x: 4)> Size: 192B
array([[[20., 21., 22., 23.],
        [24., 25., 26., 27.]],
       ...
       [[36., 37., 38., 39.],
        [40., 41., 42., 43.]]])
Dimensions without coordinates: time, y, x
```

(Array body trimmed; the real output prints all three time steps.)

That last line is the important one. The axes have names but no labels — this
`DataArray` supports `da.mean(dim="time")` but not `da.sel(time=...)`, because
there is nothing to select *by*. Naming and labeling are separate steps, and
"dimensions without coordinates" is a perfectly legal, frequently useful state.

Add the labels, a name, and metadata:

```python
da = xr.DataArray(
    values,
    dims=("time", "y", "x"),
    coords={
        "time": pd.date_range("2024-01-01", periods=3, freq="D"),
        "y": [9.0, 8.0],                          # descending: north-up
        "x": [-13.0, -12.0, -11.0, -10.0],
    },
    name="t2m",
    attrs={"units": "degC", "long_name": "2 metre temperature"},
)
print(repr(da))
```

```text
<xarray.DataArray 't2m' (time: 3, y: 2, x: 4)> Size: 192B
array([[[20., 21., 22., 23.],
        [24., 25., 26., 27.]],
       ...
       [[36., 37., 38., 39.],
        [40., 41., 42., 43.]]])
Coordinates:
  * time     (time) datetime64[us] 24B 2024-01-01 2024-01-02 2024-01-03
  * y        (y) float64 16B 9.0 8.0
  * x        (x) float64 32B -13.0 -12.0 -11.0 -10.0
Attributes:
    units:      degC
    long_name:  2 metre temperature
```

Learn to read that repr; it is the primary debugging tool in xarray, and almost
every question you will have about "what happened to my data" is answered by
comparing two of them.

- Line 1: type, variable name, and `(dim: size)` for every dimension, plus the
  in-memory size.
- The array block: the values, elided in the middle when large.
- `Coordinates:`: one line per coordinate. The leading `*` marks an **index
  coordinate** — one whose name matches a dimension and which is therefore
  backed by a real index. No `*` means the coordinate exists but does not
  support label lookup by itself.
- `Attributes:`: the inert metadata.

The attribute surface worth knowing:

```python
print("da.dims  =", da.dims)
print("da.sizes =", dict(da.sizes))
print("da.shape =", da.shape)
print("da.name  =", da.name)
print("da.attrs =", da.attrs)
print("da.encoding =", da.encoding)
print("type(da.values) =", type(da.values).__name__)
```

```text
da.dims  = ('time', 'y', 'x')
da.sizes = {'time': 3, 'y': 2, 'x': 4}
da.shape = (3, 2, 4)
da.name  = t2m
da.attrs = {'units': 'degC', 'long_name': '2 metre temperature'}
da.encoding = {}
type(da.values) = ndarray
```

Note `sizes` versus `shape`. `shape` is the numpy tuple; `sizes` is the mapping
from name to length, and it is what you want in almost all code, because it
does not depend on axis order.

**Traps.**

- **`dims` must match the array's rank.** Passing three names for a 2-D array
  raises. Passing them in the wrong order does not raise — it silently mislabels
  your axes, which is the one class of error xarray cannot protect you from,
  because it happens at the boundary where labels are first attached. Getting
  this right at ingest is the whole job.
- **`.values` is an escape hatch, not a normal accessor.** On a dask-backed
  array it forces a full computation into memory. Reaching for `.values` inside
  a lazy pipeline is how a 100 GB job turns into an OOM kill. Use `.data` when
  you want the backing array without materialising it.
- **`name` is optional and often `None`.** A `DataArray` produced by arithmetic
  usually keeps the name; one produced by some reductions may not. `to_dataset()`
  requires a name.
- **Setting `da.attrs = {...}` replaces the dict wholesale.** Use
  `da.attrs.update(...)` or `da.assign_attrs(...)` to add without clobbering.

### `Dataset` — several arrays on one grid

**What it is.** A dict-like container of `DataArray`s (the `data_vars`) that
share a coordinate system (the `coords`), plus its own `attrs`.

**Why it exists.** Because variables measured on the same grid should share one
copy of the coordinates, be selected together, be aligned together, and be
written to one store together.

```python
import xarray as xr

from ocs_stack_xarray import precipitation_dataset, temperature_dataset

ds = xr.merge([temperature_dataset(days=5, ny=4, nx=6),
               precipitation_dataset(days=5, ny=4, nx=6)])
ds.attrs["title"] = "synthetic climate cube"
print(repr(ds))
```

```text
<xarray.Dataset> Size: 2kB
Dimensions:  (time: 5, y: 4, x: 6)
Coordinates:
  * time     (time) datetime64[us] 40B 2024-01-01 2024-01-02 ... 2024-01-05
  * y        (y) float64 32B 10.0 8.967 7.933 6.9
  * x        (x) float64 48B -13.5 -12.86 -12.22 -11.58 -10.94 -10.3
Data variables:
    t2m      (time, y, x) float64 960B 28.1 27.89 28.51 ... 24.79 24.88 25.14
    tp       (time, y, x) float64 960B 12.04 0.0 0.0 0.0 ... 7.861 0.0 23.23 0.0
Attributes:
    title:    synthetic climate cube
```

The four sections of the repr map exactly onto the four parts of the object:
dimensions, coordinates, data variables, attributes.

```python
print("list(ds.data_vars) =", list(ds.data_vars))
print("list(ds.coords)    =", list(ds.coords))
print("dict(ds.sizes)     =", dict(ds.sizes))
print("ds.attrs           =", ds.attrs)
print("type(ds['t2m'])    =", type(ds["t2m"]).__name__)
print("ds.nbytes          =", ds.nbytes)
```

```text
list(ds.data_vars) = ['t2m', 'tp']
list(ds.coords)    = ['time', 'y', 'x']
dict(ds.sizes)     = {'time': 5, 'y': 4, 'x': 6}
ds.attrs           = {'title': 'synthetic climate cube'}
type(ds['t2m'])    = DataArray
ds.nbytes          = 2040
```

Access is dict-style (`ds["t2m"]`) or attribute-style (`ds.t2m`); both return a
`DataArray` carrying the shared coordinates. Prefer `ds["t2m"]` in library code,
because attribute access breaks on any variable named like an existing method
(`ds.mean` is the method, not a variable called `mean`).

Operations broadcast across variables:

```python
daily = ds.mean(dim=["y", "x"])       # both t2m and tp reduced
week = ds.sel(time=slice("2024-01-02", "2024-01-04"))   # both sliced
```

**Traps.**

- **`ds.dims` is not `ds.sizes`.** On a `Dataset`, `dims` is a mapping whose
  value access is deprecated:

  ```python
  print(type(ds.dims).__name__)
  ```

  ```text
  FrozenMappingWarningOnValuesAccess
  ```

  Iterating it gives dimension names; calling `.values()` on it emits a
  deprecation warning. Use `ds.sizes` when you want name-to-length, and
  `ds.dims` only when you want the set of names. On a `DataArray`, `dims` is a
  plain tuple of names — different type, same word. This asymmetry catches
  everyone once.
- **Merging requires compatible grids.** `xr.merge` aligns with an outer join by
  default, so two datasets on subtly different grids do not error — they produce
  a union grid full of NaN. If your merged dataset suddenly has twice the y
  size, that is why.
- **A `Dataset` cannot hold two variables with the same name.** Obvious, but it
  is the source of the `MergeError` in
  [`0403_merge_combine.py`](../../xarray/examples/0403_merge_combine.py).
- **`ds.attrs` and `ds["t2m"].attrs` are different dicts.** Dataset-level attrs
  describe the collection (title, institution, history); variable-level attrs
  describe the variable (units, long_name). Writers put them in different places
  in the file, and readers look in different places. Do not conflate them.

### `dims` — names for axes

**What it is.** An ordered tuple of names, one per axis.

**Why it exists.** It is the substrate everything else stands on. Reductions,
broadcasting, `concat`, `stack`, `transpose`, `groupby` all address dims by
name.

```python
from ocs_stack_xarray import temperature_dataset

t2m = temperature_dataset(days=31)["t2m"]
print(t2m.dims, t2m.shape)
print(t2m.mean(dim="time").dims)
print(t2m.mean(dim=["y", "x"]).dims)
print(t2m.transpose("x", "y", "time").dims)
```

```text
('time', 'y', 'x') (31, 20, 30)
('y', 'x')
('time',)
('x', 'y', 'time')
```

The essential property: **a reduction removes the named dim from the result and
leaves the others in place**. `mean(dim="time")` on `(time, y, x)` gives
`(y, x)` — a map. `mean(dim=["y", "x"])` gives `(time,)` — a series. No `dim`
argument at all reduces everything to a scalar.

**Traps.**

- **Dimension order still exists**, it just stops mattering for most operations.
  It matters when you hand `.values` to numpy code, and it matters for storage
  layout: the on-disk chunk grid is expressed in the dimension order the array
  had at write time. `transpose` is free in xarray and expensive on disk.
- **Two arrays with the same dim name are assumed to be on the same axis.** That
  assumption is what makes broadcasting work, and it is why reusing a generic
  name like `"n"` or `"index"` for unrelated things produces bizarre alignment
  behaviour. Name dimensions after what they *are*.
- **A dim with no coordinate cannot be `sel`ed.** It is still a real dimension —
  `isel`, reductions, and concat all work.

### `coords` — labels along the axes

**What it is.** Named arrays attached to the object, each associated with zero
or more dims. Two flavours:

- **Dimension coordinates** (also called index coordinates): the coordinate's
  name equals a dimension name, it is 1-D along that dim, and xarray builds a
  pandas index for it. These are the ones marked `*` in the repr.
- **Non-dimension coordinates**: everything else. Auxiliary labels, scalar
  markers, 2-D lat/lon arrays for curvilinear grids, CRS holders.

**Why it exists.** Dimension coordinates are what make label-based selection,
alignment, `reindex`, `groupby`, and `resample` possible. Non-dimension
coordinates are how you carry extra labeling without claiming it is an axis.

```python
from ocs_stack_xarray import temperature_dataset

ds = temperature_dataset(days=5, ny=4, nx=6)

# a scalar non-dimension coordinate, the usual home for a CRS marker
ds2 = ds.assign_coords(spatial_ref=0)
print("coords: ", list(ds2.coords))
print("indexes:", list(ds2.indexes))

# a 1-D non-dimension coordinate along an existing dim
ds3 = ds.assign_coords(dayname=("time", [str(t)[:10] for t in ds.time.values]))
print(repr(ds3.coords))
print("indexes:", list(ds3.indexes))
```

```text
coords:  ['time', 'y', 'x', 'spatial_ref']
indexes: ['time', 'y', 'x']
Coordinates:
  * time     (time) datetime64[us] 40B 2024-01-01 2024-01-02 ... 2024-01-05
    dayname  (time) <U10 200B '2024-01-01' '2024-01-02' ... '2024-01-05'
  * y        (y) float64 32B 10.0 8.967 7.933 6.9
  * x        (x) float64 48B -13.5 -12.86 -12.22 -11.58 -10.94 -10.3
indexes: ['time', 'y', 'x']
```

`spatial_ref` and `dayname` are coordinates but not indexes: no `*`, absent from
`.indexes`. To promote one to an index you either swap it in for the dim
coordinate or set it as the index:

```python
sw = ds3.swap_dims({"time": "dayname"})
print("after swap_dims:", list(sw.indexes), dict(sw.sizes))
print("sel works:", dict(sw.sel(dayname="2024-01-03").sizes))
```

```text
after swap_dims: ['y', 'x', 'dayname'] {'dayname': 5, 'y': 4, 'x': 6}
sel works: {'y': 4, 'x': 6}
```

**Traps.**

- **A coordinate is not automatically an index.** In this xarray version, `sel`
  on a non-index 1-D coordinate does work — it falls back to scanning — and even
  slices work. But `method="nearest"` does not:

  ```python
  ds3.sel(dayname="2024-01-02", method="nearest")
  ```

  ```text
  TypeError: unsupported operand type(s) for -: 'str' and 'str'
  ```

  and, more importantly, none of the *alignment* machinery uses it. Two datasets
  are aligned on their indexes, never on their auxiliary coordinates. If a label
  is what identifies a position, make it an index.
- **Coordinate dtype matters for selection.** Floating-point coordinates almost
  never contain the exact value a caller asks for, which is the entire subject of
  [`0202_nearest_and_interp.py`](../../xarray/examples/0202_nearest_and_interp.py).
- **Scalar selection leaves a scalar coordinate behind.** `ds.sel(time="...")`
  drops the `time` dim but keeps `time` as a 0-D coordinate — usually helpful,
  occasionally in the way. `drop=True` removes it.
- **Coordinates participate in `identical()` but with attrs.** `equals()`
  compares values and coordinates; `identical()` additionally compares attrs, on
  the object *and* on every coordinate. A round-trip that loses attrs will pass
  `equals` and fail `identical` — exactly what
  [`0103_from_pandas.py`](../../xarray/examples/0103_from_pandas.py) shows.

### `attrs` — metadata that rides along and does nothing

**What it is.** A plain Python dict on every `Dataset`, `DataArray`, and
coordinate. Conventionally holds CF metadata: `units`, `long_name`,
`standard_name`, `axis`.

**Why it exists.** Self-describing data. A netCDF or zarr store that says
`units: "degC"` can be interpreted by someone who did not write it. That is the
whole point of the CF conventions, and it is why climate data is unusually
portable compared to most scientific formats.

**And now the crucial part.** xarray does not read them. Ever.

```python
import xarray as xr

from ocs_stack_xarray import temperature_dataset

celsius = temperature_dataset(days=10, ny=3, nx=4)["t2m"]
kelvin = (celsius + 273.15).assign_attrs(units="K")
mixed = celsius + kelvin        # degC + K

print(f"celsius mean          = {float(celsius.mean()):7.2f} [{celsius.attrs['units']}]")
print(f"kelvin mean           = {float(kelvin.mean()):7.2f} [{kelvin.attrs['units']}]")
print(f"celsius + kelvin mean = {float(mixed.mean()):7.2f}")
```

```text
celsius mean          =   26.30 [degC]
kelvin mean           =  299.45 [K]
celsius + kelvin mean =  325.74
```

No warning, no error, a number. This is not a bug — xarray has no unit system
and deliberately does not guess. It is why unit normalisation has to be a
deliberate ingest-time step in any pipeline that reads from more than one
source.

Propagation is controlled by an option, and it is a blind copy:

```python
with xr.set_options(keep_attrs=True):
    print("mean().attrs['units'] =", celsius.mean().attrs["units"])
    print("var().attrs['units']  =", celsius.var().attrs["units"])

with xr.set_options(keep_attrs=False):
    print("keep_attrs=False:", celsius.mean().attrs)
```

```text
mean().attrs['units'] = degC
var().attrs['units']  = degC
keep_attrs=False: {}
```

The mean of a Celsius field is in Celsius; the variance is in Celsius squared.
xarray copies the string either way, because it cannot know which operations
invalidate which metadata.

**Traps.**

- **Inert. Always.** No unit checking, no unit conversion, no dimensional
  analysis. `pint-xarray` adds that layer if you want it.
- **Propagation defaults have changed across versions.** In this version,
  reductions and arithmetic keep attrs. In older releases reductions dropped
  them. If your code depends on the answer, set `xr.set_options(keep_attrs=...)`
  explicitly rather than relying on the default.
- **Binary operations keep only what both operands agree on.** `celsius +
  kelvin` produced `{'long_name': ..., 'standard_name': ...}` — the conflicting
  `units` was silently dropped. Silently losing `units` is arguably worse than
  keeping the wrong one.
- **`attrs` values must be serialisable** to the target format. netCDF and zarr
  accept strings, numbers, and arrays of those. A nested dict or a Python object
  raises at write time, not at assignment time — so the failure surfaces far from
  the cause. Encode structured metadata as a JSON string.
- **Attrs are not a place for anything you need to be correct.** They are a hint
  to humans and to downstream tools that opt in. Behaviour must come from code.

### `indexes` — what makes label lookup possible

**What it is.** The mapping from dimension name to the pandas index built from
its dimension coordinate.

**Why it exists.** `sel`, slicing, `reindex`, `align`, `groupby`, `resample`, and
every binary operation between two objects go through it. It is the machinery of
"by label" rather than "by position".

```python
print("list(da.indexes) =", list(da.indexes))
print("da.indexes['time'] =", repr(da.indexes["time"]))
print(repr(da.xindexes))
```

```text
list(da.indexes) = ['time', 'y', 'x']
da.indexes['time'] = DatetimeIndex(['2024-01-01', '2024-01-02', '2024-01-03'], dtype='datetime64[us]', name='time', freq='D')
Indexes:
    time     PandasIndex
    y        PandasIndex
    x        PandasIndex
```

`.indexes` gives the underlying pandas objects; `.xindexes` gives xarray's own
wrapper types (the extension point that lets non-pandas index implementations
exist). For everyday work `.indexes` is what you want, and the pandas index it
hands back is the source of the health checks every ingestion loop needs:

```python
idx = store.indexes["time"]
assert idx.is_monotonic_increasing
assert idx.is_unique
```

That two-line guard is the single most valuable thing in this section. `concat`
and zarr's `append_dim` both trust the caller completely; nothing between the
feed and the store checks that a period was not ingested twice.
[`0402_concat_append.py`](../../xarray/examples/0402_concat_append.py) is built
around exactly this.

`reindex` is the manual form of alignment — put the data on an index you
specify, padding with NaN:

```python
import pandas as pd

target = pd.date_range("2024-01-03", periods=5, freq="D")
ri = ds.reindex(time=target)
print("sizes:", dict(ri.sizes), "NaN:", int(ri.t2m.isnull().sum()))
```

```text
sizes: {'time': 5, 'y': 4, 'x': 6} NaN: 48
```

The source ran 2024-01-01 to 2024-01-05; the target runs 2024-01-03 to
2024-01-07, so two of five days have no data and are filled with NaN across
all 24 cells.

**Traps.**

- **Reindexing an integer array to a longer index upcasts it to float**, because
  NaN is a float:

  ```python
  c = xr.DataArray([1, 2, 3], dims="n", coords={"n": [0, 1, 2]})
  print(c.reindex(n=[0, 1, 2, 3]).dtype, c.reindex(n=[0, 1, 2, 3]).values)
  ```

  ```text
  float64 [ 1.  2.  3. nan]
  ```

  Any operation that can introduce NaN into an int array does this: `where`,
  `reindex`, an outer `align`, a masking `sel`. It is silent.
- **Unsorted indexes break slicing.** `sel(time=slice(a, b))` on a
  non-monotonic index raises or returns nonsense. `sortby("time")` first.
- **Duplicate labels break everything downstream.** `sel` on a duplicated label
  returns multiple entries where the caller expected one, which turns a scalar
  into an array several function calls away from the cause.
- **Index dtype must match for alignment to find anything.** A `datetime64`
  index and a `cftime` index look the same in a repr and share zero labels. See
  the calendar trap in the pitfalls section.

### `encoding` — how the values are stored, not what they are

**What it is.** A dict, separate from `attrs`, describing the on-disk
representation: chunk shape, compressor, dtype, `_FillValue`, `scale_factor`,
CF time units.

**Why it exists.** So that "the data" and "how the data was serialised" stay
distinct. A temperature field is degrees Celsius in memory regardless of whether
the file stored it as `int16` with a scale factor, or as `float32` with `-9999`
for missing.

An in-memory dataset that has never touched storage has empty encoding. After a
round-trip it is populated:

```python
import tempfile, os
import xarray as xr

from ocs_stack_xarray import temperature_dataset

ds = temperature_dataset(days=5, ny=4, nx=6)
print("before any write:", ds.t2m.encoding)

with tempfile.TemporaryDirectory() as tmp:
    path = os.path.join(tmp, "s.zarr")
    ds.to_zarr(path, consolidated=False)
    back = xr.open_zarr(path, consolidated=False)
    print("t2m.encoding keys:", sorted(back.t2m.encoding))
    print("time.encoding:", {k: v for k, v in back.time.encoding.items()
                             if k in ("units", "calendar", "dtype", "chunks")})
```

```text
before any write: {}
t2m.encoding keys: ['_FillValue', 'chunks', 'compressors', 'dtype', 'fill_value', 'filters', 'preferred_chunks', 'serializer', 'shards']
time.encoding: {'chunks': (5,), 'units': 'days since 2024-01-01 00:00:00', 'calendar': 'proleptic_gregorian', 'dtype': dtype('<i8')}
```

Two things to notice. First, the time coordinate is stored as **integers with a
reference epoch** — `days since 2024-01-01` — which is the CF convention, and
the decoding of it into `datetime64` on read is why `time.attrs` has no `units`
key while `time.encoding` does. Second, `_FillValue` is encoding, not data:
xarray reads the sentinel from the file and converts it to NaN in memory, and
records what the sentinel was so a later write can use the same one.

**Traps.**

- **Encoding is sticky and can bite on rewrite.** A dataset opened from a store
  carries that store's chunk encoding. Write it somewhere else, or after
  rechunking, and the stale `chunks` entry can conflict with the new dask
  layout. The fix is to clear it: `ds.t2m.encoding.pop("chunks", None)`, or pass
  a fresh `encoding=` to the writer, or `ds.to_zarr(..., encoding={...})` with
  everything you want stated explicitly.
- **`encoding` is not `attrs` and writers treat them completely differently.**
  Putting `units` for a variable in `encoding` does nothing useful; putting
  `_FillValue` in `attrs` puts a literal attribute in the file that decoders may
  or may not honour.
- **Encoding only exists after a write or a read.** Do not assert on it for
  in-memory data.
---

## Phase 1 — Anatomy and creation

Three examples establishing the data model: what a `DataArray` is made of, what
a `Dataset` adds, and where the boundary with pandas lies. Everything later
assumes this phase.

**OCS relevance:** everything in open-climate-service is an `xr.Dataset` with
dims normalised to `(time, y, x)`. This phase is the shape of that object.

### `0101_dataarray_anatomy` — building a DataArray layer by layer

Source: [`xarray/examples/0101_dataarray_anatomy.py`](../../xarray/examples/0101_dataarray_anatomy.py)

**What it teaches.** The example starts from a raw numpy array and adds one
layer at a time — dims, then coords, then attrs and a name — printing what each
layer buys. It is deliberately structured as an argument rather than a demo: at
each step you can see what became possible that was not possible before.

The starting point is a `(3, 2, 4)` array of temperatures with anonymous axes:

```python
values = 20.0 + np.arange(24, dtype=np.float64).reshape(3, 2, 4)
print(f"  shape={values.shape}, dtype={values.dtype} -- axes are anonymous positions")
```

```text
Plain numpy: a (3, 2, 4) array of temperatures. Which axis is time?
  shape=(3, 2, 4), dtype=float64 -- axes are anonymous positions
```

Step 1 names the axes:

```python
da = xr.DataArray(values, dims=("time", "y", "x"))
print(f"  dims={da.dims}, sizes={dict(da.sizes)}")
```

```text
Step 1 -- dims give every axis a name:
  dims=('time', 'y', 'x'), sizes={'time': 3, 'y': 2, 'x': 4}
```

Step 2 adds `coords` — the full call is in the core-concepts section above.
Note the descending `y`: north-up, as real rasters are. Step 3 attaches
`name` and `attrs`:

```python
da.name = "t2m"
da.attrs = {"units": "degC", "long_name": "2 metre temperature"}
```

```text
Step 2 -- coords label positions along each dim:
  time labels: ['2024-01-01', '2024-01-02', '2024-01-03']
  y labels:    [9.0, 8.0]  (descending = north-up)

Step 3 -- attrs carry metadata; name identifies the variable:
  name='t2m', attrs={'units': 'degC', 'long_name': '2 metre temperature'}
```

And then the payoff section, which is the point of the whole file:

```python
print(f"  da.mean()                     = {float(da.mean()):.2f}  (grand mean)")
print(f"  da.mean(dim='time').shape     = {da.mean(dim='time').shape}  (spatial map)")
print(f"  da.mean(dim=['y', 'x']).shape = {da.mean(dim=['y', 'x']).shape}  (time series)")
first_day = da.sel(time="2024-01-01")
```

```text
Payoff -- operations address dims by name, not position:
  da.mean()                     = 31.50  (grand mean)
  da.mean(dim='time').shape     = (2, 4)  (spatial map)
  da.mean(dim=['y', 'x']).shape = (3,)  (time series)
  da.sel(time='2024-01-01')     -> shape (2, 4), mean 23.50

Escape hatch -- .values returns the numpy array underneath:
  type(da.values) = ndarray, da.values[0, 0, 0] = 20.0
```

**Why it matters.** Three lines that say what they mean, and a `sel` on a date
string that has no numpy equivalent at all. The mean of `20..43` is `31.5`; the
mean of the first day's `20..27` is `23.5` — the arithmetic is trivial, which is
the point. Nothing about the computation changed; only the addressing did.

The escape hatch matters just as much. `.values` is always there. Adopting
xarray does not mean abandoning numpy, scipy, or any library that takes arrays;
it means the numpy call happens at a boundary you chose rather than everywhere.

**Traps.**

- **`dims` order must match the array's axis order.** `xr.DataArray(values,
  dims=("x", "y", "time"))` on this array raises only because the sizes happen
  to differ. On a cube where two dims are the same length, mislabeling is
  silent and permanent. This is the one place where getting it wrong poisons
  everything downstream, so it deserves a test.
- **Assigning `da.attrs = {...}` replaces the dict.** Here that is intentional
  (the array had none). In code that already carries metadata, use
  `assign_attrs` or `attrs.update`.
- **`da.name` is settable after construction**, and `to_dataset()` needs it.
- **`.values` on a dask-backed array triggers a full compute.** Harmless on a
  `(3, 2, 4)` toy, catastrophic on a real store. Phase 6 revisits this.

### `0102_dataset_construction` — variables sharing a grid

Source: [`xarray/examples/0102_dataset_construction.py`](../../xarray/examples/0102_dataset_construction.py)

**What it teaches.** How a `Dataset` is assembled from single-variable pieces,
what its repr shows, and how operations fan out over every variable at once.
This is the first example that uses the shared synthetic helpers, and it
produces the object shape everything after it works on.

```python
ds = xr.merge([temperature_dataset(days=31), precipitation_dataset(days=31)])
print(f"  data_vars: {list(ds.data_vars)}")
print(f"  coords:    {list(ds.coords)}")
print(f"  sizes:     {dict(ds.sizes)}")
```

```text
Two single-variable datasets on the same (time, y, x) grid, merged:
  data_vars: ['t2m', 'tp']
  coords:    ['time', 'y', 'x']
  sizes:     {'time': 31, 'y': 20, 'x': 30}
```

Then it prints the repr, which is the real lesson:

```text
The Dataset repr summarizes everything (variables, dtypes, coords):
  <xarray.Dataset> Size: 298kB
  Dimensions:  (time: 31, y: 20, x: 30)
  Coordinates:
    * time     (time) datetime64[us] 248B 2024-01-01 2024-01-02 ... 2024-01-31
    * y        (y) float64 160B 10.0 9.837 9.674 9.511 ... 7.389 7.226 7.063 6.9
    * x        (x) float64 240B -13.5 -13.39 -13.28 -13.17 ... -10.52 -10.41 -10.3
  Data variables:
      t2m      (time, y, x) float64 149kB 28.1 27.89 28.51 ... 26.68 25.48 25.67
      tp       (time, y, x) float64 149kB 12.63 0.0 0.0 0.0 ... 0.0 0.0 0.0 14.89
```

Two variables at 149 kB each, coordinates at a few hundred bytes, stored once
and shared. That ratio is the argument for `Dataset` over a dict of
`DataArray`s: coordinates are not duplicated, and they cannot drift apart.

Per-variable attrs survive the merge:

```python
for name, var in ds.data_vars.items():
    print(f"  {name}: {var.attrs['long_name']} [{var.attrs['units']}]")
```

```text
Each data_var is a DataArray with its own attrs:
  t2m: 2 metre temperature [degC]
  tp: total precipitation [mm/day]
```

And operations apply to everything at once:

```python
daily = ds.mean(dim=["y", "x"])
week = ds.sel(time=slice("2024-01-08", "2024-01-14"))
```

```text
Operations on the Dataset apply to every variable at once:
  ds.mean(dim=['y', 'x']) -> sizes {'time': 31}
  first-day spatial means: t2m=25.98 degC, tp=3.47 mm/day

Selection is shared across variables (one call, all variables):
  ds.sel(time=slice('2024-01-08', '2024-01-14')) -> 7 days
```

**Why it matters.** This is the object an OCS zarr store opens as. Every later
example — selection, groupby, zarr writes, chunked opens — operates on this
shape. Getting comfortable reading the repr now saves an enormous amount of time
later, because in xarray the answer to "what did that operation do?" is almost
always "print the repr and compare".

**Traps.**

- **`merge` aligns before it combines**, with an outer join. Two datasets whose
  grids differ by a rounding error in the coordinate values will merge into a
  union grid twice as large, full of NaN, without an error. If sizes surprise
  you after a merge, compare the coordinate arrays exactly.
- **Attribute-style access is a convenience, not an interface.** `ds.tp` works
  until a variable is called `count` or `mean`. Library code should use
  `ds["tp"]`.
- **`ds.sizes`, not `ds.dims`.** See the core-concepts trap above.
- **The repr elides.** `28.1 27.89 28.51 ... 26.68 25.48 25.67` is the first
  three and last three values, not the data. Use `.values` or `.isel(...)` when
  you need to see actual numbers.

### `0103_from_pandas` — the tabular boundary

Source: [`xarray/examples/0103_from_pandas.py`](../../xarray/examples/0103_from_pandas.py)

**What it teaches.** Both directions of the pandas bridge, what survives each
crossing, and — most usefully — a measurement of when the cube representation
is the wrong choice.

Flattening a cube produces one row per cell, with a `MultiIndex`:

```python
ds = temperature_dataset(days=4, ny=3, nx=4)
df = ds.to_dataframe()
```

```text
A small (time, y, x) temperature cube, flattened to a table:
  cube sizes: {'time': 4, 'y': 3, 'x': 4}  (48 values)
  ds.to_dataframe() -> DataFrame shape (48, 1), MultiIndex levels ['time', 'y', 'x']
  every (time, y, x) cell becomes one row:
                                      t2m
    time       y    x
    2024-01-01 10.0 -13.500000  28.100584
                    -12.433333  27.894316
                    -11.366667  28.512338
```

Coming back unstacks the `MultiIndex` into dims — and drops the metadata:

```python
back = df.to_xarray()
print(f"  values and coords survive:  ds.equals(back)    = {ds.equals(back)}")
print(f"  attrs do NOT survive:       ds.identical(back) = {ds.identical(back)}")
```

```text
Round-trip back with to_xarray() -- the MultiIndex is unstacked into dims:
  df.to_xarray() -> sizes {'time': 4, 'y': 3, 'x': 4}, data_vars ['t2m']
  values and coords survive:  ds.equals(back)    = True
  attrs do NOT survive:       ds.identical(back) = False
  (back.t2m.attrs = {} -- units/long_name were dropped by pandas)
```

That is the cleanest possible demonstration of the `equals` versus `identical`
distinction: values and coordinates round-trip perfectly, attrs do not, because
pandas has nowhere to put them.

A `MultiIndex` Series converts too, one dim per index level:

```python
idx = pd.MultiIndex.from_product([["s1", "s2"], [0, 6, 12, 18]], names=["station", "hour"])
series = pd.Series(np.round(np.linspace(24.0, 31.0, 8), 1), index=idx, name="t2m")
da = series.to_xarray()
```

```text
A MultiIndex Series converts too -- index levels become dims:
  Series of 8 rows -> DataArray dims ('station', 'hour'), sizes {'station': 2, 'hour': 4}
  da.sel(station='s2', hour=12) = 30.0
```

Then the measurement that decides which side of the bridge to live on:

```python
obs = pd.DataFrame(
    {
        "station": ["s1", "s1", "s2", "s3"],
        "time": pd.to_datetime(["2024-01-01", "2024-01-03", "2024-01-02", "2024-01-05"]),
        "t2m": [25.0, 26.0, 24.5, 27.0],
    }
).set_index(["station", "time"])
dense = obs.to_xarray()
```

```text
When tabular wins: sparse station observations, most (station, time) pairs empty:
  table: 4 rows, zero waste
  cube:  sizes {'station': 3, 'time': 4} = 12 cells, 8 of them NaN padding
```

**Why it matters.** Four observations become twelve cells. Three stations and
four dates is a 3x waste factor; a thousand stations over five years of daily
data would be a cube of 1.8 million cells holding, say, twenty thousand real
values. The cube is rectangular by construction and cannot be otherwise.

The inverse is equally true and equally important: a dense `(time, y, x)` grid
wastes nothing as a cube and is enormously wasteful as a table, because every
row repeats its three coordinates. This is why climate data is stored as cubes
and station data is stored as tables, and why a service that handles both — as
OCS does, with dense zarr stores and tabular request/response payloads — needs
the bridge in both directions.

**Traps.**

- **`to_dataframe()` materialises every cell.** On a dask-backed dataset it
  computes everything first. On a real store this is an out-of-memory error with
  a confusing traceback.
- **Attrs do not survive either direction.** Re-attach them explicitly after
  `to_xarray()`.
- **`to_xarray()` sorts and completes the index.** The output dims are the
  sorted unique values of each level, and every combination is materialised. The
  ordering may not match your input order.
- **Do not use `to_dataframe()` for output serialisation of a dense cube.** Use
  the store formats. The table form is for interoperability at the edges, not
  for bulk data.

---

## Phase 2 — Selection and indexing

Three examples on getting subsets out: by position, by label, by
nearest-neighbour, by interpolation, and by boolean mask.

**OCS relevance:** OCS slices stores constantly. "This period" is a label slice
on time; "this pyramid tile" is a positional slice on y/x; "the value at this
point" is a nearest lookup with a tolerance. Reading `sel`/`isel` fluently is
reading that codebase fluently.

### `0201_isel_sel` — position versus label

Source: [`xarray/examples/0201_isel_sel.py`](../../xarray/examples/0201_isel_sel.py)

**What it teaches.** The two indexing methods, their differing slice semantics,
and the scalar-versus-range distinction that decides whether a dimension
survives into the next operation.

`isel` is numpy indexing with names:

```python
ds = temperature_dataset(days=31, ny=4, nx=5)
ds.isel(time=0)               # first day; time dim gone
ds.isel(time=-1)              # last day
ds.isel(time=slice(0, 7))     # seven days; end EXCLUSIVE
```

```text
isel indexes by POSITION, exactly like numpy axis indexing:
  ds.isel(time=0)             -> sizes {'y': 4, 'x': 5}  (first day, time dim gone)
  ds.isel(time=-1)            -> last day, 2024-01-31
  ds.isel(time=slice(0, 7))   -> 7 days  (python slice: end EXCLUSIVE)
```

`sel` uses the coordinate labels:

```python
day = ds.sel(time="2024-01-15")
row = ds.sel(y=float(ds.y.values[1]))
```

```text
sel indexes by LABEL, using the coordinate values:
  ds.sel(time='2024-01-15')   -> sizes {'y': 4, 'x': 5}, spatial mean 26.42 degC
  ds.sel(y=8.966667) -> sizes {'time': 31, 'x': 5}  (floats must match the label exactly)
```

Note the awkwardness of the `y` line: `8.966667` is a printed approximation of a
float that came out of `np.linspace`, and the call only works because the code
pulled the exact value back out of the coordinate. That awkwardness is the whole
motivation for the next example.

The slice semantics differ, and this is the detail worth memorising:

```python
ds.sel(time=slice("2024-01-08", "2024-01-14"))   # 7 days, 14th INCLUDED
ds.sel(time=slice("2024-01-20", None))           # open-ended
```

```text
Time slices take plain strings -- and label slices are INCLUSIVE on both ends:
  ds.sel(time=slice('2024-01-08', '2024-01-14')) -> 7 days (14th included)
  ds.sel(time=slice('2024-01-20', None))         -> 12 days (open end)
  contrast: isel slice(0, 7) excluded index 7; sel slices never make you do the -1 dance
```

`isel` slices follow Python: end exclusive. `sel` slices follow pandas `.loc`:
both ends inclusive. Both are defensible, they are opposite, and mixing them up
produces off-by-one errors that survive review because the code looks right.

Then the dimension-survival rule:

```python
scalar = ds.isel(time=0)      # time dim dropped
kept = ds.isel(time=[0])      # time dim survives at length 1
```

```text
Scalar selection DROPS the dim; list/slice selection KEEPS it:
  ds.isel(time=0)   -> dims {'y': 4, 'x': 5}  (no time dim)
  ds.isel(time=[0]) -> dims {'time': 1, 'y': 4, 'x': 5}  (time survives at length 1)
  the length-1 form matters when downstream code expects a time axis (concat, resample, zarr append)
```

And what a dropped dim leaves behind:

```python
point = ds.sel(time="2024-01-15").isel(y=0, x=0)
clean = ds.sel(time="2024-01-15", drop=True)
```

```text
A dropped dim leaves a scalar coord behind (handy label, sometimes unwanted):
  point dims: {}  -- 0-d, but coords remember where it came from:
  point coords: time=2024-01-15, y=10.00, x=-13.50
  ds.sel(time='2024-01-15', drop=True) -> coords ['y', 'x']  (scalar time coord removed)
```

**Why it matters.** The length-1 form is not pedantry. `xr.concat`,
`resample`, and `to_zarr(append_dim="time")` all require the dimension to exist.
A pipeline that processes one period at a time and uses `isel(time=0)` somewhere
in the middle will fail at the write with a confusing error about missing
dimensions, and the cause will be several functions away.

The scalar coordinate left behind by a dropped dim is usually a feature — it
records where the value came from — but it will also be written into a store as
a scalar variable, and it participates in `identical()` comparisons. `drop=True`
when you do not want it.

**Traps.**

- **`isel` exclusive, `sel` inclusive.** The single most common indexing bug in
  xarray code.
- **Exact float `sel` is a trap.** Never write `ds.sel(y=8.97)`. Either use
  `method="nearest"` or select positionally.
- **`sel` with a list preserves order and allows duplicates.**
  `ds.sel(time=["2024-01-05", "2024-01-01"])` returns those two days in that
  order, which breaks index monotonicity.
- **Negative indices work in `isel`, not in `sel`.** `sel(time=-1)` looks for a
  label equal to `-1`.
- **Chained `sel` calls each re-align.** `ds.sel(...).sel(...)` is correct but
  does the work twice; pass multiple dims in one call.

### `0202_nearest_and_interp` — reconciling grids that almost line up

Source: [`xarray/examples/0202_nearest_and_interp.py`](../../xarray/examples/0202_nearest_and_interp.py)

**What it teaches.** What to do when the label you want is not in the index:
snap to the closest one, bound how far the snap may go, and — a different
operation entirely — compute a new value between the stored ones.

The grid's labels are `np.linspace` output, so they are ugly:

```python
ds = temperature_dataset(days=5)
x_step = float(ds.x.values[1] - ds.x.values[0])
```

```text
Temperature on the default grid -- coordinate labels are ugly floats:
  sizes: {'time': 5, 'y': 20, 'x': 30}
  x: -13.5000 .. -10.3000, step ~0.1103 deg
```

Asking for a round number fails:

```python
try:
    ds.sel(x=-12.0)
except KeyError as err:
    print(f"  KeyError: {str(err)[:70]}...")
```

```text
Exact label selection at x=-12.0 (not a stored label) fails:
  KeyError: "not all values found in index 'x'. Try setting the `method` keyword a...
  float grids almost never contain the exact value a caller asks for
```

`method="nearest"` snaps:

```python
col = ds.sel(x=-12.0, method="nearest")
point = ds.sel(y=8.0, x=-12.0, method="nearest")
```

```text
sel(method="nearest") snaps to the closest stored label:
  ds.sel(x=-12.0, method='nearest') -> chose label x=-11.9552 (off by 0.0448)
  works per-dim: y=8.0, x=-12.0 snapped to (y=8.0421, x=-11.9552)
```

`tolerance=` turns "snap anywhere" into "snap within this distance or fail":

```python
ok = ds.sel(x=-12.0, method="nearest", tolerance=0.2)
try:
    ds.sel(x=-12.0, method="nearest", tolerance=0.01)
except KeyError as err:
    print(f"  tolerance=0.01: KeyError: {str(err)[:60]}")
```

```text
tolerance= caps how far nearest may snap -- beyond it, KeyError:
  tolerance=0.2 (> half a cell): ok, x=-11.9552
  tolerance=0.01 (< distance to any label): KeyError: "not all values found in index 'x'"
```

An array of targets regrids by copying nearest cells:

```python
new_x = np.round(np.linspace(-13.5, -10.3, 9), 2)
coarse = ds.sel(x=new_x, method="nearest")
```

```text
Passing an ARRAY of targets regrids by nearest neighbour (values are copied, not blended):
  ds.sel(x=<9 targets>, method='nearest') -> x size 30 -> 9
  requested: [-13.5, -13.1, -12.7, -12.3] ...
  snapped:   [-13.5, -13.06, -12.73, -12.29] ... (nearest stored labels)
```

`interp` computes new values instead:

```python
fine_x = ds.x.values[:2].mean()          # midpoint of the first two columns
mid = ds.interp(x=[fine_x], method="linear")
```

```text
interp() computes NEW values on the target grid instead of copying the nearest cell:
  ds.interp(x=[-13.4448]) -> t2m at the midpoint = 27.997
  neighbours at x[0], x[1]: 28.101, 27.894
  linear value at their midpoint x=-13.4448: 27.997  (exactly the average)
  nearest would have returned one of the neighbours unchanged; interp blends by distance
```

The example checks its own arithmetic against `np.interp` and gets the same
number to three decimals — the midpoint of `28.101` and `27.894` is `27.997`.

**Why it matters.** These are three genuinely different operations that get
conflated. `nearest` **copies** a stored value and tells you which one, via the
returned coordinate. `interp` **invents** a value that was never measured.
`tolerance` is the policy knob that decides whether a request outside the
store's footprint gets a plausible-looking wrong answer or an error.

For a service, the last one is the important one. A request for a point 400 km
outside the country extent, without a tolerance, snaps silently to the nearest
edge cell and returns a number. With a tolerance, it raises, and the API can
return a 404 instead of fiction.

**Traps.**

- **`interp` needs scipy.** It is an explicit dependency of this project for
  exactly that reason; the example carries a fallback path in case it is
  missing.
- **`interp` is not conservative.** Linear interpolation does not preserve
  totals, which matters for accumulated variables like precipitation. For
  area-weighted, conservative regridding you want `xesmf` or similar, not
  `interp`.
- **`nearest` on a descending coordinate works**, because the index knows its
  own direction — but a *non-monotonic* coordinate breaks it.
- **`tolerance` is in coordinate units.** On a lat/lon grid that is degrees, and
  a degree of longitude is not a fixed distance. Do not treat a degree tolerance
  as a metre tolerance.
- **`interp` on a dask-backed array along a chunked dimension** requires the
  interpolated dim to be in one chunk, same rule as `apply_ufunc` in phase 6.

### `0203_masking` — booleans, `where`, and event counting

Source: [`xarray/examples/0203_masking.py`](../../xarray/examples/0203_masking.py)

**What it teaches.** How boolean masks work as first-class labeled arrays, the
two modes of `where`, membership tests with `isin`, and the "count events over
time" reduction that is the shape of most climate indicators.

The data is zero-inflated on purpose:

```python
ds = precipitation_dataset(days=30)
tp = ds.tp
dry_frac = float((tp == 0.0).mean())
```

```text
A month of daily precipitation -- most cells are exactly zero:
  sizes: {'time': 30, 'y': 20, 'x': 30}  (18000 cells)
  60% of cells are 0.0 mm/day; max is 53.9 mm/day
```

A comparison gives a boolean `DataArray` with the same dims, which composes:

```python
wet = tp > 0.0
heavy = wet & (tp > 10.0)
```

```text
A comparison produces a boolean DataArray with the same dims:
  (tp > 0.0) -> dtype bool, sizes {'time': 30, 'y': 20, 'x': 30}
  mask.mean() is the wet fraction: 0.402
  masks combine with & | ~ :  heavy = wet & (tp > 10) -> 2061 cells
```

`mask.mean()` being the fraction is a small delight: `True` is 1, so the mean of
a boolean array is the proportion satisfying the condition, and no separate
counting API is needed.

`where(mask)` keeps the grid and inserts NaN:

```python
wet_only = tp.where(wet)
```

```text
where(mask) KEEPS the grid and writes NaN where the mask is False:
  tp.where(tp > 0) -> sizes unchanged {'time': 30, 'y': 20, 'x': 30}, valid cells: 7232
  mean over ALL days      (zeros included): 3.19 mm/day
  mean over WET cells only (NaN skipped):   7.94 mm/day  -- rain intensity
```

Those two means are different physical quantities. `3.19` mm/day is the rainfall
rate averaged over all days including dry ones. `7.94` mm/day is rain intensity:
how hard it rains when it rains. Both are legitimate; confusing them is a real
error in real climate products, and `where` plus the `skipna` default is how you
get from one to the other.

`where(mask, drop=True)` shrinks instead:

```python
ts = tp.mean(dim=["y", "x"])
rainy = ts.where(ts > 3.5, drop=True)
```

```text
where(mask, drop=True) SHRINKS the array to labels where the mask holds:
  areal-mean series: 30 days; days above 3.5 mm/day: 3
  surviving labels:  ['2024-01-12', '2024-01-20', '2024-01-21']
  drop=True only pays off on 1-d selections; in N-d it keeps the bounding box, NaN-padded
```

`isin` for calendar-style membership:

```python
weekend_mask = ds.time.dt.dayofweek.isin([5, 6])
weekends = ds.sel(time=weekend_mask)
```

```text
isin() tests membership -- ideal for calendar-style selection:
  ds.time.dt.dayofweek.isin([5, 6]) -> 8 of 30 days are weekend
  ds.sel(time=<mask>) -> sizes {'time': 8, 'y': 20, 'x': 30}
  weekend mean rain 3.26 vs overall 3.19 mm/day
```

And the indicator recipe:

```python
wet_days = (tp > 1.0).sum(dim="time")
```

```text
Counting events = sum a boolean mask over time (True counts as 1):
  (tp > 1.0).sum(dim='time') -> a (y, x) map, sizes {'y': 20, 'x': 30}
  wet days (> 1 mm) per pixel: min 4, max 20, mean 11.7 of 30
  this map IS the OCS wet-days indicator: one reduction over a mask, per pixel
```

**Why it matters.** That last block is a complete climate indicator in one
expression. Wet-day counts, dry-spell lengths, days-above-threshold, heatwave
frequency — the whole family is "build a boolean, reduce it over time, keep the
`(y, x)` map". Understanding that shape means understanding most of what a
climate service actually computes.

The keep-versus-drop distinction has a storage consequence. `where(mask)`
preserves the grid, so the result slots straight back into a `(time, y, x)`
store alongside its siblings. `drop=True` destroys the grid, which is what you
want for a 1-D series and almost never what you want for a field.

**Traps.**

- **`drop=True` on N-dimensional data keeps the bounding box.** It drops labels
  only where *every* value along the other dims is masked. Applied to a 3-D
  field with scattered mask holes it usually drops nothing and just NaN-pads —
  slower than `where(mask)` and no smaller.
- **`where` upcasts integers to float.** NaN is a float; there is no integer NaN.
  A masked count array silently becomes `float64`.
- **`mask.sum()` counts `True`; `mask.mean()` gives the fraction; `count()` is
  something else** — `count()` counts non-NaN values, not `True` values. On a
  boolean array they mean different things.
- **`where` has a second argument.** `tp.where(cond, 0.0)` substitutes `0.0`
  instead of NaN, which is often what you want and avoids the dtype upcast.
- **`ds.sel(time=boolean_mask)` works** because a boolean array along a dim is a
  valid indexer — but the mask's dim must match, and passing a mask with the
  wrong dim name gives a confusing error rather than a helpful one.
---

## Phase 3 — Computation

Five examples covering the arithmetic and aggregation surface: broadcasting,
reductions, groupby, resample, rolling and coarsen. This is where xarray stops
being a nicer container and starts being a nicer *language*.

**OCS relevance:** `coarsen` is literally how GeoZarr pyramid levels are built.
`groupby` and `resample` power the climatology and monthly-product processes.
Anomaly computation is one line of broadcasting.

### `0301_arithmetic_broadcasting` — matching by name, not position

Source: [`xarray/examples/0301_arithmetic_broadcasting.py`](../../xarray/examples/0301_arithmetic_broadcasting.py)

**What it teaches.** Four kinds of arithmetic on a `(time, y, x)` field, chosen
to isolate the broadcasting rules: scalar, field-minus-time-mean, an outer
product of two disjoint-dim operands, and a unit conversion that goes
metadata-wrong on purpose.

Scalar arithmetic is unremarkable, except for what happens to attrs:

```python
t2m = temperature_dataset(days=31)["t2m"]
corrected = t2m + 0.5
```

```text
Scalar ops apply elementwise, shape unchanged (e.g. a +0.5 degC bias correction):
  (t2m + 0.5): shape=(31, 20, 30), mean=27.263 degC
  attrs ride along unchanged: corrected.attrs['units']='degC'
```

The anomaly is the canonical example:

```python
time_mean = t2m.mean(dim="time")     # dims (y, x)
anomaly = t2m - time_mean            # dims (time, y, x)
```

```text
Anomaly = field minus its time mean. The time mean has dims (y, x);
xarray lines it up with the (time, y, x) field by NAME and broadcasts over time:
  time_mean: dims=('y', 'x'), shape=(20, 30)
  anomaly:   dims=('time', 'y', 'x'), shape=(31, 20, 30)
  anomaly.mean(dim='time') ~ 0 everywhere: max abs = 1.64e-14
  in numpy this needs values - values.mean(axis=0) -- get the axis wrong and it still 'works'
```

`1.64e-14` is float round-off — the anomaly's time mean is zero to machine
precision, which is the correctness check. And the last line is the argument:
`values - values.mean(axis=0)` is correct only if time is axis 0, and produces
a plausible array either way.

Disjoint dims broadcast to their union:

```python
series = t2m.mean(dim=["y", "x"])    # dims ("time",)
spatial = t2m.mean(dim="time")       # dims ("y", "x")
outer = series * spatial             # dims ("time", "y", "x")
```

```text
Operands with DISJOINT dims broadcast into their union (an outer product by name):
  (time,) * (y, x) -> dims=('time', 'y', 'x'), shape=(31, 20, 30)
  dim order in the operands is irrelevant; only the names matter
```

In numpy this is `series[:, None, None] * spatial[None, :, :]`, and every one of
those `None`s is an opportunity to be wrong.

Then the metadata trap, staged deliberately:

```python
t2m_f = t2m * 9.0 / 5.0 + 32.0
print(f"  danger: attrs are copied verbatim, so units are now STALE: {t2m_f.attrs['units']!r}")
t2m_f.attrs["units"] = "degF"
```

```text
Unit conversions are plain arithmetic (xarray never interprets units attrs):
  degC: mean=26.763, min=21.723, max=32.142
  degF: mean=80.173, min=71.101, max=89.855
  danger: attrs are copied verbatim, so units are now STALE: 'degC' on degF values
  fix them yourself after converting: t2m_f.attrs['units'] = 'degF'
```

**Why it matters.** Broadcasting-by-name is the property that makes xarray
expressions safe to read. The anomaly line means what it says, no matter what
shape the array happens to have or how many transposes preceded it.

The unit staleness is the mirror image. Metadata propagates but is not
maintained. Any operation that changes what the numbers *mean* — unit
conversion, `var()`, a ratio, a count — leaves you with a correct array and a
lying `units` string. There is no automatic fix; the discipline is to update
attrs at every point where semantics change, and to normalise units once at
ingest so downstream code never has to ask.

**Traps.**

- **Alignment happens before arithmetic.** Two operands with different time
  ranges silently inner-join. That is phase 4's subject and is the single most
  surprising behaviour in the library.
- **Attrs are copied but not validated.** Covered above; see also
  [`0701_cf_attrs_units.py`](../../xarray/examples/0701_cf_attrs_units.py).
- **Outer-product broadcasting can explode memory.** `(time,) * (y, x)` is
  harmless here, but two large 1-D arrays with different dim names produce their
  full Cartesian product. If a result has more dims than you expected, a dim
  name is wrong somewhere.
- **In-place operators (`+=`) do work** but mutate shared coordinate objects in
  ways that surprise. Prefer rebinding.
- **`float()` on a non-scalar raises**, and on a dask array it computes. The
  examples use `float(x.mean())` freely because the data is small.

### `0302_reductions` — the reduction family and `skipna`

Source: [`xarray/examples/0302_reductions.py`](../../xarray/examples/0302_reductions.py)

**What it teaches.** That every reduction has the same signature, that `dim=`
takes one name or a list, and — the substantial half — how `skipna` governs
what happens when the data has holes.

The family, all reducing time to leave a map:

```python
for name, reduced in (
    ("mean", t2m.mean(dim="time")),
    ("sum", t2m.sum(dim="time")),
    ("std", t2m.std(dim="time")),
    ("min", t2m.min(dim="time")),
    ("max", t2m.max(dim="time")),
):
    print(f"  t2m.{name}(dim='time'): shape={reduced.shape}, grand {name} of map = {float(reduced.mean()):.3f}")
```

```text
Every reduction takes dim= by NAME; reducing 'time' leaves a (y, x) map:
  t2m.mean(dim='time'): shape=(20, 30), grand mean of map = 26.763
  t2m.sum(dim='time'): shape=(20, 30), grand sum of map = 829.642
  t2m.std(dim='time'): shape=(20, 30), grand std of map = 0.898
  t2m.min(dim='time'): shape=(20, 30), grand min of map = 24.872
  t2m.max(dim='time'): shape=(20, 30), grand max of map = 28.620
```

Multi-dim and no-dim forms:

```text
Passing a list of dims reduces several at once:
  t2m.mean(dim=['y', 'x']): dims=('time',), shape=(31,)  (area-mean time series)
  t2m.mean(dim=['time', 'y', 'x']) = 26.763  (scalar)
  t2m.mean() reduces ALL dims:      26.763  (same scalar)
```

Then holes are punched with `where`, mimicking ocean pixels outside a border:

```python
holey = t2m.where(t2m.x < -11.5)
```

```text
where(cond) keeps values where cond is True and inserts NaN elsewhere.
Mask out the eastern third of the grid, like ocean pixels outside a country border:
  NaN cells: 6820 of 18600 (36.7 percent)
```

And the `skipna` contrast:

```text
Reductions skip NaN by default (skipna=True), so stats cover only real data:
  full field mean:            26.763
  holey.mean()  [skipna=True] 26.762  (mean of the surviving western cells)

skipna=False propagates NaN: one hole poisons every reduction that touches it:
  holey.mean(skipna=False)                = nan
  holey.mean(dim='time', skipna=False):     220 of 600 map
    cells are NaN -- exactly the masked columns, since their whole time axis is NaN
```

**Why it matters.** The default is `skipna=True`, and it is the right default
for climate data — a store with a few missing days should still produce a
monthly mean. But it is a default that hides problems. A pixel with 29 of 30
days missing gets a "monthly mean" computed from a single observation, reported
with the same confidence as a fully observed pixel.

The strict mode is the other extreme: `skipna=False` on real data mostly returns
NaN, because real data always has at least one hole somewhere.

The useful middle ground is neither, and it is worth writing down because
xarray does not provide it directly:

```python
monthly = daily.resample(time="1ME").mean()                    # skipna default
coverage = daily.notnull().resample(time="1ME").sum()          # observations per bucket
expected = daily.notnull().resample(time="1ME").count()        # bucket length
monthly = monthly.where(coverage / expected >= 0.8)            # require 80% coverage
```

Compute the aggregate with `skipna=True`, compute the coverage alongside it, and
mask the aggregate where coverage is too thin. That is what a real product does,
and both halves come from this example's vocabulary.

The `220 of 600` figure is a good sanity check on the mechanics: with the mask
applied along `x` only, whole columns are NaN for their entire time axis, so a
strict reduction over time gives NaN for exactly those columns —
`11 columns x 20 rows = 220`.

**Traps.**

- **`skipna=True` hides coverage.** See above. Compute coverage explicitly.
- **`sum` of all-NaN with `skipna=True` is `0.0`, not NaN.** That is numpy's
  convention and it is a real source of wrong answers: an entirely missing month
  reports zero rainfall rather than "unknown". `min_count=` fixes it —
  `tp.sum(dim="time", min_count=1)` gives NaN when nothing was observed.
- **`std` defaults to `ddof=0`** (population, not sample). pandas defaults to
  `ddof=1`. Same word, different number.
- **`skipna` costs performance.** NaN-aware reductions use slower code paths. On
  data known to be complete, `skipna=False` is faster as well as stricter.
- **Integer arrays have no NaN**, so `skipna` is meaningless on them until
  something upcasts to float.

### `0303_groupby_climatology` — the climatology recipe

Source: [`xarray/examples/0303_groupby_climatology.py`](../../xarray/examples/0303_groupby_climatology.py)

**What it teaches.** `groupby` on a datetime component, what a climatological
normal is, and the one-line anomaly that comes from subtracting a groupby mean.

Three years of dailies:

```python
t2m = temperature_dataset(days=1096)["t2m"]   # 2024 (366) + 2025 + 2026
grouped = t2m.groupby("time.month")
```

```text
Three years of daily 2 m temperature:
  dims=('time', 'y', 'x'), shape=(1096, 20, 30), time 2024-01-01 .. 2026-12-31

groupby('time.month') buckets every timestamp by calendar month across ALL years:
  number of groups: 12 (three Januaries land in group 1, etc.)
  group 1 holds 93 time steps (31 January days x 3 years)
```

The `"time.month"` string is the key piece of syntax: it means "the `month`
attribute of the `time` coordinate", equivalent to `t2m.time.dt.month`, and it
groups across years. Ninety-three January days from three separate years land in
one bucket.

The normal is one reduction:

```python
clim = grouped.mean()
```

```text
.mean() over the groups = climatological normal, one field per month:
  clim: dims=('month', 'y', 'x'), shape=(12, 20, 30)  (time is replaced by month=1..12)
  area-mean normal by month (the seasonal cycle, degC):
    month  1: 26.784
    month  4: 28.881
    month  7: 25.285
    month 10: 23.129
```

The `time` dimension is gone, replaced by a `month` dimension of length 12. That
is a structural change: the output no longer has a time axis and cannot be
appended to a time-indexed store. It is a different kind of object — a
climatology — and it usually lives in its own store.

The seasonal cycle is visible in the numbers: warmest around April, coolest in
October, which is what the synthetic sine over `365.25` days produces.

The anomaly is where `groupby` earns its keep:

```python
anom = t2m.groupby("time.month") - clim
```

```text
Subtracting a groupby mean broadcasts each month's normal back onto its own days:
  anom: dims=('time', 'y', 'x'), shape=(1096, 20, 30)  (same shape as the dailies)
  every July day got the July normal subtracted, not one global mean
  anomaly grand mean:            -0.00000  (~0 by construction)
  mean anomaly over January days: -0.00000
  mean anomaly over July days:    -0.00000
  most-above-normal day: 2025-07-01
```

Read that expression carefully, because it is doing something non-obvious.
`t2m.groupby("time.month")` produces a grouped object; subtracting a
`(month, y, x)` array from it aligns **on the group label**. Each of the 1096
days looks up its own month's normal and subtracts that one. The result has the
original daily shape.

The verification is that every subset's mean is zero: the grand mean, the
January-only mean, the July-only mean. That can only be true if each day was
measured against its own month's normal.

**Why it matters.** This is the climatology and anomaly pipeline, complete, in
two lines. It is one of the most common operations in the whole field, and it
is genuinely awkward without groupby: you would need to build a month index,
loop over twelve masks, and reassemble.

The calendar bookkeeping is the part that is easy to underestimate. Ninety-three
January days across three years, twenty-nine February days in 2024 and
twenty-eight in the others — `groupby` handles all of it because it works on the
actual timestamps rather than on positional arithmetic.

**Traps.**

- **`groupby` fragments dask chunks catastrophically.** This is the big one and
  it is invisible on eager data. On a chunked array, `ds.groupby("time.month") -
  clim` produces **one chunk per time step**. Measured on a 365-day, 13-chunk
  array (see the pitfalls section for the full reproduction): 13 chunks in, 365
  chunks out. Always rechunk after a groupby before writing.
- **`groupby` is slow on many groups.** `groupby("time.dayofyear")` gives 366
  groups; `groupby` over a spatial dimension gives thousands. `flox` accelerates
  this substantially and xarray uses it automatically when installed.
- **`"time.month"` is not `"time.season"` is not `"time.dayofyear"`.** All valid,
  wildly different group counts, and the resulting dimension is named after the
  component.
- **The output dim is `month`, not `time`.** Downstream code expecting a time
  axis breaks. Renaming does not fix it — the labels are 1..12, not dates.
- **Anomalies against a short baseline are noise.** Three years is fine for a
  demo and meaningless as a real normal, where 30 years is the WMO convention.
- **`groupby(...) - clim` requires the group dim name to match.** It works here
  because `clim` came from the same groupby and carries a `month` dim.

### `0304_resample` — daily to monthly, and the sum-versus-mean decision

Source: [`xarray/examples/0304_resample.py`](../../xarray/examples/0304_resample.py)

**What it teaches.** Calendar-aware temporal aggregation, and — the substantial
part — that choosing the reduction is a semantic decision the code must make
correctly, because xarray will happily compute the wrong one.

```python
days = 366   # all of 2024
t2m = temperature_dataset(days=days)["t2m"]
tp = precipitation_dataset(days=days)["tp"]
t2m_monthly = t2m.resample(time="1ME").mean()
```

```text
resample(time='1ME') buckets by calendar month ('ME' = month-end frequency),
then a reduction collapses each bucket -- 366 days -> 12 months:
  t2m.resample(time='1ME').mean(): shape=(12, 20, 30)
  new time labels are bucket ends: ['2024-01-31', '2024-02-29', '2024-03-31'] ...
  calendar-aware: February 2024 contributes 29 days, January 31 -- no fixed window math
```

Note `2024-02-29`. The bucket boundaries come from the calendar, not from a
fixed window of 30 days, and the labels are the bucket ends.

The contrast with `groupby` is worth stating: `groupby("time.month")` collapses
*all* Januaries into one, giving a 12-step climatology with no year.
`resample(time="1ME")` keeps years distinct, giving one value per month per
year — a shorter time series, still a time series. They look similar and answer
different questions.

Temperature is intensive, so the mean is the meaningful aggregate:

```text
Temperature is intensive: 'the mean January temperature' is meaningful, a sum is not.
  area-mean monthly temperature (degC):
    2024-01:   26.763
    2024-04:   28.877
    2024-07:   25.279
  .sum() on temperature 'works' but is nonsense: January -> 829.6
```

`829.6` is degree-days, if you squint. As a monthly temperature it is garbage,
and nothing in xarray objects.

Precipitation is an accumulation, so both reductions are meaningful and mean
different things:

```python
tp_total = tp.resample(time="1ME").sum()     # mm per month
tp_rate = tp.resample(time="1ME").mean()     # mm/day
```

```text
Precipitation is an accumulation: monthly TOTAL (mm) is .sum(); .mean() is intensity (mm/day):
  month     sum [mm]   mean [mm/day]   mean * days_in_month
  2024-01      98.66           3.183      98.66  (31 days)
  2024-02      93.21           3.214      93.21  (29 days)
  sum = mean * days-in-month, so the two only agree if you track month length -- resample does
```

The arithmetic closes exactly: `3.183 x 31 = 98.66`, `3.214 x 29 = 93.21`.
February has a *higher* daily rate and a *lower* monthly total, purely because
it is shorter. Reporting one as the other reverses the ranking of the two
months.

The example closes with the ISO period framing:

```text
Climate stores think in ISO 8601 periods: a monthly value is the period 2024-01 (duration P1M),
not the instant '2024-01-31'. resample's bucket labels map 1:1 onto those period keys, which is
how OCS addresses monthly slices when appending to or querying a store.
```

**Why it matters.** Two things.

First, the sum-versus-mean choice is per variable, it is semantic, and there is
no way for the library to help. A pipeline that resamples a whole `Dataset` with
one reduction is wrong for at least one of its variables as soon as it holds
both a temperature and a rainfall. The correct shape is a per-variable mapping
of variable name to reduction, applied explicitly.

Second, the ISO period framing is how services address data. A monthly value is
not an instant, it is a period with a duration: `2024-01` / `P1M`. A resampled
axis labeled with bucket ends maps one-to-one onto those keys, which is what
makes "append the 2024-02 period" a well-defined operation.

**Traps.**

- **Frequency aliases changed in recent pandas.** `"M"` became `"ME"` (month
  end), `"Y"` became `"YE"`, `"H"` became `"h"`. Old code raises or warns. Use
  `"1ME"`, `"MS"`, `"YE"`, `"D"`, `"6h"`.
- **`"ME"` labels buckets at month end, `"MS"` at month start.** Both are month
  buckets; the labels differ, and downstream code keyed on the label sees
  different strings.
- **Resample fills gaps.** If the source is missing whole days, the buckets
  still exist and the reduction runs over what is there — so `skipna` semantics
  from `0302` apply directly.
- **`.sum()` of an empty bucket is `0.0`.** Same `min_count` caveat as before.
- **Resample fragments dask chunks** the same way `groupby` does. Same fix.
- **`resample` needs a datetime-like index.** An integer time coordinate cannot
  be resampled.

### `0305_rolling_coarsen` — smoothing in time, downsampling in space

Source: [`xarray/examples/0305_rolling_coarsen.py`](../../xarray/examples/0305_rolling_coarsen.py)

**What it teaches.** Two windowing operations that look similar and are
opposites: `rolling` uses overlapping windows and preserves length, `coarsen`
uses non-overlapping blocks and reduces it.

`rolling` on a single noisy grid cell:

```python
series = t2m.isel(y=10, x=15)          # 60 days at one cell
smooth = series.rolling(time=7).mean()
```

```text
Daily temperature at one grid cell (60 days, noisy):
  shape=(60,), std=1.036 degC

rolling(time=7).mean() slides an overlapping 7-day window over the series:
  output shape=(60,) (unchanged -- windows overlap, one output per input step)
  first 6 values are NaN (incomplete windows): NaN count = 6
  smoothed std = 0.654 degC -- day-to-day noise averaged away (7-day mean)
  check: mean(days 1..7) = 26.5936 vs rolling value at day 7 = 26.5936
  center=True + min_periods=1 -> label at window center, no NaN: count = 0
```

Three details in that output. The length is unchanged, because windows overlap.
The first six values are NaN because there is no complete 7-day window yet. And
the standard deviation drops from `1.036` to `0.654`, which is the smoothing
doing its job — very close to the `1/sqrt(7) = 0.378` factor you would get for
independent noise, the difference being the real signal that survives.

The self-check is worth copying as a habit: `mean(days 1..7)` computed manually
equals the rolling value at day 7, to four decimals. When you are unsure what a
windowed operation aligned to, compute one window by hand.

`center=True` labels each window at its middle rather than its right edge, and
`min_periods=1` accepts partial windows, which removes the NaNs at the cost of
edge values being computed from fewer observations.

`coarsen` on a spatial grid:

```python
field = temperature_dataset(days=10, ny=16, nx=32)["t2m"]
level1 = field.coarsen(y=2, x=2).mean()
level2 = level1.coarsen(y=2, x=2).mean()
```

```text
coarsen(y=2, x=2).mean() averages NON-overlapping 2x2 blocks: the pyramid downsampler.
  level 0 (native): shape=(10, 16, 32)

Each level halves y and x (time untouched) -- OCS GeoZarr multiscale levels:
  level 1: shape=(10, 8, 16)  (16x32 -> 8x16, 4x fewer pixels)
  level 2: shape=(10, 4, 8)  (8x16 -> 4x8, 16x fewer than native)
```

Verified cell by cell:

```text
Each coarse cell is the mean of its 2x2 source block (identical up to float round-off):
  source block values: [28.101, 27.894, 27.606, 28.166]
  block mean = 27.941716, level-1 cell [0, 0] = 27.941716, abs diff = 3.6e-15
  coords are coarsened too -- new cell centers are block means of the old ones:
  y[0:2] = [10.0, 9.793] -> level-1 y[0] = 9.897
  means preserved across levels: 26.2296 -> 26.2296 -> 26.2296
```

Three separate facts confirmed. The coarse value is exactly the block mean. The
coarse **coordinate** is the block mean of the old coordinates — `(10.0 +
9.793) / 2 = 9.897` — so the new cell centres are geometrically correct, which
matters enormously for a tile pyramid. And the grand mean is invariant across
levels, `26.2296` at all three, which is the property a mean-downsampled pyramid
must have.

**Why it matters.** `coarsen(y=2, x=2).mean()` applied repeatedly *is* the
multiscale pyramid. Level 0 is native, each level halves both spatial dims, and
a viewer requesting a zoomed-out map fetches a level whose chunks are 4x, 16x,
64x smaller. That is the whole algorithm; there is no separate pyramid library
involved.

The coordinate coarsening is what makes it correct rather than merely plausible.
If coordinates were subsampled instead of averaged, every level would be offset
by half a cell from the one below, and overlaid layers would drift.

**Traps.**

- **`coarsen` requires the dim size to divide evenly** by default:

  ```python
  f = temperature_dataset(days=2, ny=5, nx=5)["t2m"]
  f.coarsen(y=2, x=2).mean()
  ```

  ```text
  ValueError: Could not coarsen a dimension of size 5 with window 2 and
  boundary='exact'. Try a different 'boundary' option.
  ```

  `boundary="trim"` drops the remainder (`ny=5 -> 2`); `boundary="pad"` pads it
  (`ny=5 -> 3`). Both change the extent, so a pyramid built with `pad` covers
  slightly more ground than its parent. Prefer grid sizes that are powers of two
  times something, and be explicit about the boundary policy.
- **`rolling` reductions are not free.** They construct overlapping views; a
  large window on a large array is expensive, and on dask it needs overlapping
  chunk exchange.
- **`rolling` labels at the window's right edge by default.** A 7-day mean
  labeled at day 7 covers days 1-7 — a trailing mean. `center=True` for a
  centred mean. Getting this wrong shifts a whole series by three days.
- **`min_periods` trades NaN for reduced confidence.** A `min_periods=1` value at
  day 1 is a 1-day mean wearing a 7-day label.
- **The coarsen reduction methods are injected at runtime**, so type checkers do
  not see `.mean()` on the coarsen object. The example carries
  `# type: ignore[attr-defined]` for exactly that reason — worth knowing before
  you fight your own type checker over it.
- **`coarsen` on a chunked array wants the window to divide the chunks.** A 2x2
  coarsen on 65-row chunks forces a rechunk.
---

## Phase 4 — Alignment and combining

Three examples on what happens when two objects meet: automatic index
alignment, appending along a dimension, and assembling a dataset from variables
or from tiles.

**OCS relevance:** streaming ingestion appends one period at a time, so `concat`
along time and index alignment are the core mechanics. Sources also arrive
tiled, per region and per period, and have to be reassembled.

### `0401_alignment` — the silent inner join

Source: [`xarray/examples/0401_alignment.py`](../../xarray/examples/0401_alignment.py)

**What it teaches.** That every binary operation aligns its operands first, that
the default join is an intersection, and how to take control of it.

Two overlapping slices of one month:

```python
full = temperature_dataset(days=31)
early = full.isel(time=slice(0, 21))   # Jan 01 .. Jan 21
late = full.isel(time=slice(14, 31))   # Jan 15 .. Jan 31
```

```text
Two temperature datasets sliced from one month, overlapping in the middle:
  early: 21 days, 2024-01-01 .. 2024-01-21
  late: 17 days, 2024-01-15 .. 2024-01-31
  overlap: 2024-01-15 .. 2024-01-21 (7 days)
```

Subtract them and watch the time axis collapse:

```python
diff = late.t2m - early.t2m
```

```text
Arithmetic aligns on labels first -- and the default join is INNER:
  (late.t2m - early.t2m).sizes = {'time': 7, 'y': 20, 'x': 30}
  21 days minus 17 days -> 7 days: only labels present in BOTH survive
  No error, no warning. This surprises people: a missing period in one
  input quietly shrinks the result instead of failing loudly.
```

Twenty-one days minus seventeen days gives seven. Not an error, not a warning —
the intersection.

`xr.align` makes the choice explicit:

```python
for join in ("inner", "outer", "left"):
    a, b = xr.align(early, late, join=join)
```

```text
xr.align returns both objects re-indexed onto a common index, join= chosen by you:
  join='inner'  -> early:  7 days, late:  7 days
  join='outer'  -> early: 31 days, late: 31 days
  join='left'   -> early: 21 days, late: 21 days
  inner = intersection, outer = union, left = index of the first argument
```

An outer join cannot invent data:

```python
early_out, late_out = xr.align(early, late, join="outer")
```

```text
An outer join cannot invent data -- missing labels are filled with NaN:
  outer index: 31 days (the full month)
  early padded with 6000 NaNs = 10 missing days x 600 cells
  late  padded with 8400 NaNs = 14 missing days x 600 cells

Arithmetic on the outer-aligned pair keeps the full axis but NaNs the gaps:
  (late - early) after outer align: 31 days, 7 fully valid
  .fillna(0.0) replaces the padding when a neutral value is correct: 0 NaNs left
  (whether 0.0 is correct depends on the variable -- fine for counts, wrong for temperature)
```

The sixth join option is the one the summary flags and the example does not
demonstrate: `join="exact"`, which refuses to align at all.

```python
xr.align(a, b, join="exact")
```

```text
AlignmentError: cannot align objects with join='exact' where index/labels/sizes
are not equal along these coordinates (dimensions): 'time' ('time',)
```

**Why it matters.** This is the behaviour that produces the single most common
"why is my output empty?" bug in xarray. A pipeline computes observations minus
climatology, one source is late by a day, and the result is a correctly-shaped
array with a time axis one day shorter than it should be — or, if the sources
have drifted entirely, an array with zero time steps. Nothing raises.

The diagnosis is always the same and it takes ten seconds once you know it:
print `.sizes` on both operands and on the result, and compare the index
endpoints.

The defence is `join="exact"` inside a `xr.set_options(arithmetic_join="exact")`
block, or an explicit assertion on the time index before the operation. In a
pipeline that ingests from multiple feeds, one of those belongs at every point
where two sources meet.

**Traps.**

- **The default is inner and it is silent.** Everything above.
- **Alignment is on indexes only.** Non-index coordinates are not aligned; if
  they conflict they are dropped from the result, quietly.
- **Float coordinates rarely align.** Two grids computed by different code paths
  can differ in the last bit and share zero labels, giving an empty inner join.
  Round coordinates at ingest, or reindex one onto the other.
- **`fillna(0.0)` is a semantic decision.** Zero is right for counts and wrong
  for temperature. The padding NaN means "not observed"; replacing it with a
  number claims an observation.
- **`align` returns re-indexed copies**, it does not modify in place.
- **Alignment on a dask-backed array is still lazy**, but an outer join
  introduces new NaN blocks and can change the chunk structure.

### `0402_concat_append` — the append-a-period pattern, and its guard rails

Source: [`xarray/examples/0402_concat_append.py`](../../xarray/examples/0402_concat_append.py)

**What it teaches.** The ingestion loop in miniature, and what happens when a
feed re-sends a period that is already in the store.

Slice a quarter into monthly periods and append them one at a time:

```python
full = temperature_dataset(days=91)
periods = [full.sel(time=month) for month in ("2024-01", "2024-02", "2024-03")]

store = periods[0]
for period in periods[1:]:
    store = xr.concat([store, period], dim="time")
```

```text
A 91-day dataset (Jan-Mar 2024) sliced into the monthly periods a feed would deliver:
  period 2024-01: 31 days
  period 2024-02: 29 days
  period 2024-03: 31 days

The append-a-period pattern: start with the first period, concat each arrival onto the store:
  ingest 2024-01 -> store has 31 days
  ingest 2024-02 -> store has 60 days
  ingest 2024-03 -> store has 91 days
```

The health check after each append:

```python
idx = store.indexes["time"]
print(f"  monotonic increasing: {idx.is_monotonic_increasing}")
print(f"  unique timestamps:    {idx.is_unique}")
print(f"  identical to the unsliced original: {store.identical(full)}")
```

```text
A healthy time axis after appending is monotonic, unique, and matches the source:
  monotonic increasing: True
  unique timestamps:    True
  identical to the unsliced original: True
```

`identical` — not just `equals` — meaning values, coordinates, *and* attrs all
round-tripped through three concats.

Then the failure mode, staged deliberately:

```python
resend = full.sel(time=slice("2024-03-25", "2024-03-31"))
bad = xr.concat([store, resend], dim="time")
```

```text
Now the failure mode: the feed re-sends a period that overlaps the store (Mar 25-31):
  concat happily produces 98 days -- no error, no warning
  monotonic increasing: False
  unique timestamps:    False
  duplicated labels:    7
  selecting one duplicated day returns 2 entries -- downstream code breaks here
```

That last line is where the damage surfaces. `bad.sel(time="2024-03-28")`
returns a length-2 time dimension where every caller expects a scalar, and the
resulting error appears in whatever function consumed it — arbitrarily far from
the append that caused it.

Detection and two repairs:

```python
# guard, after every append
assert idx.is_unique and idx.is_monotonic_increasing

# repair after the fact
fixed = bad.drop_duplicates(dim="time", keep="first").sortby("time")

# better: trim the incoming period before appending
trimmed = resend.sel(time=resend.time > store.time.values[-1])
```

```text
Detection is an index check; repair is drop_duplicates (or trim before appending):
  guard:  assert idx.is_unique and idx.is_monotonic_increasing  # after every append
  bad.drop_duplicates(dim='time', keep='first') -> 91 days
  monotonic: True, unique: True
  identical to the pre-overlap store: True
  better: trim the incoming period to time > store max -> 0 genuinely new days
  (the resend was entirely old data, so nothing survives the trim -- the append becomes a no-op)
```

**Why it matters.** `concat` trusts the caller absolutely. It does not check for
overlap, does not check monotonicity, does not sort. Neither does
`to_zarr(append_dim="time")` — same contract, and there the damage is written to
disk.

The trim approach is better than the repair approach for a service, because it
makes a duplicate delivery idempotent: filter the incoming period to timestamps
strictly greater than the store's maximum, and a re-sent period becomes a no-op
of zero new days rather than a corrupted axis needing repair.

**Traps.**

- **`concat` never validates.** The two-line assertion is not optional.
- **`concat` aligns non-concat dimensions with an outer join.** Two periods on
  slightly different spatial grids concat into a union grid full of NaN rather
  than failing. Pass `join="exact"` to make that an error.
- **`concat` in a Python loop is O(n^2).** Each iteration copies everything.
  For many pieces, collect them and call `xr.concat(parts, dim="time")` once.
- **`drop_duplicates(keep="first")` keeps the *older* value.** If a resend is a
  correction, `keep="last"` is what you want. Neither is right by default.
- **`sortby("time")` is cheap on an in-memory array and a full shuffle on a
  chunked one.**
- **Attrs come from the first object by default.** `combine_attrs=` controls it;
  `"drop_conflicts"` and `"identical"` are the two useful settings.

### `0403_merge_combine` — variables, conflicts, and tiles

Source: [`xarray/examples/0403_merge_combine.py`](../../xarray/examples/0403_merge_combine.py)

**What it teaches.** The other two combining functions and, critically, when to
use which of the three.

`merge` puts different variables on one grid:

```python
ds = xr.merge([temperature_dataset(days=10), precipitation_dataset(days=10)])
```

```text
xr.merge combines datasets holding DIFFERENT variables on the same grid:
  merge([t2m dataset, tp dataset]) -> data_vars=['t2m', 'tp'], sizes={'time': 10, 'y': 20, 'x': 30}
  merge also aligns indexes (outer join by default), so grids must agree or NaNs appear
```

The same variable twice, with different values, is refused:

```python
temp_alt = temperature_dataset(days=10, seed=1)   # same name, different noise
xr.merge([temp, temp_alt])
```

```text
When the SAME variable appears twice with different values, merge refuses:
  xr.MergeError: conflicting values for variable 't2m' on objects to be combined. You can skip this check by specifying compat='override'.
  default compat='no_conflicts': values may only disagree where one side is NaN
```

`compat=` is the policy knob:

```text
compat= chooses the conflict policy explicitly:
  compat='override' -> keeps the first dataset's values (equals first: True)
  compat='equals'   -> demands identical values; self-merge passes (round-trips: True)
  compat='identical' additionally compares attrs -- the strictest check
```

`combine_by_coords` reassembles tiles using nothing but their coordinates:

```python
original = temperature_dataset(days=6, ny=8, nx=10)
tiles = [
    original.isel(time=slice(0, 3), y=slice(0, 4)),
    original.isel(time=slice(0, 3), y=slice(4, 8)),
    original.isel(time=slice(3, 6), y=slice(0, 4)),
    original.isel(time=slice(3, 6), y=slice(4, 8)),
]
shuffled = [tiles[3], tiles[0], tiles[2], tiles[1]]
combined = xr.combine_by_coords(shuffled)
```

```text
combine_by_coords reassembles a grid of tiles by READING their coordinates:
  4 tiles, each {'time': 3, 'y': 4, 'x': 10} -- a 2x2 grid over (time, y)
  combine_by_coords(shuffled tiles) -> sizes={'time': 6, 'y': 8, 'x': 10}
  identical to the original dataset: True
  tile order did not matter: coordinate values, not list position, decide placement
```

Shuffled input, `identical` output. That is the whole value proposition: nobody
has to track which file is which tile.

The example ends with the decision table, which is the part worth memorising:

```text
Which tool when:
  concat            -- one dimension, you state it, you control the order (append a period)
  merge             -- different variables onto one shared grid
  combine_by_coords -- many tiles of the same variables; coords decide the layout
```

**Why it matters.** These three cover the three real assembly problems, and
choosing wrongly produces confusing failures. Trying to `concat` a pile of
unordered tiles gives a scrambled axis. Trying to `merge` two periods of the
same variable hits a conflict, because merge is not for that. Trying to
`combine_by_coords` a stream of arriving periods works but re-reads and re-sorts
everything on each call.

The rule of thumb: **`concat` when you know the order and the dimension,
`merge` when the variables differ, `combine_by_coords` when you have a pile and
want the coordinates to sort it out.**

**Traps.**

- **`merge`'s default `compat="no_conflicts"` permits disagreement where one
  side is NaN.** That is more permissive than it sounds — two sources with
  complementary gaps merge into a filled field with no indication that the
  values came from different places.
- **`compat="override"` silently discards the second dataset's values.** It is
  the fastest option and the easiest one to regret. Use it only when you know
  the duplicates are genuinely identical.
- **`combine_by_coords` needs the tiles to form a complete hypercube.** Missing
  tiles produce NaN-filled holes or an error, depending on the gap.
- **`combine_by_coords` reads every tile's coordinates.** With thousands of
  files that is a lot of small reads; `open_mfdataset` with an explicit
  `combine=` and `concat_dim=` is faster when you already know the layout.
- **`open_mfdataset` is the lazy multi-file version** of this pattern and is what
  real ingestion uses — worth knowing it exists even though this project's
  examples do not use it.
---

## Phase 5 — I/O: netCDF and zarr

Four examples on getting data onto disk and back: the traditional single-file
format, the cloud-native chunked format, how chunk shape is chosen, and the two
write modes that make a store grow and heal.

**OCS relevance:** sources arrive as netCDF and GRIB; stores are written as
Zarr v3 with explicit chunk encoding; `append_dim` and `region` are how periods
land.

### `0501_netcdf` — the round-trip and what survives it

Source: [`xarray/examples/0501_netcdf.py`](../../xarray/examples/0501_netcdf.py)

**What it teaches.** That netCDF preserves the whole data model, which engines
exist, and what compression costs and buys.

```python
ds = xr.merge([temperature_dataset(days=31, ny=16, nx=16),
               precipitation_dataset(days=31, ny=16, nx=16)])
ds.attrs["title"] = "synthetic climate cube"
ds.to_netcdf(path)
```

```text
to_netcdf() serializes the whole Dataset -- variables, coords, attrs -- into one file:
  wrote climate.nc: 133.9 KiB
  variables: ['t2m', 'tp'], sizes: {'time': 31, 'y': 16, 'x': 16}

open_dataset() reads it back; attrs and coords survive intact:
  dataset attrs:    {'title': 'synthetic climate cube'}
  t2m attrs:        {'units': 'degC', 'long_name': '2 metre temperature'}
  coords preserved: ['time', 'y', 'x']
  time coord identical: True, t2m values identical: True
  time axis: 31 days, 2024-01-01 .. 2024-01-31
```

Both attr levels survive — dataset-level `title`, variable-level `units` and
`long_name` — as do the coordinates and the datetime dtype. This is the contrast
with the pandas round-trip in `0103`, where attrs were dropped: netCDF was
designed for self-describing scientific data, and metadata is a first-class part
of the format rather than an afterthought.

Engines are pluggable backends:

```python
from xarray.backends import list_engines

engines = [name for name in list_engines() if name != "store"]
```

```text
Engines: xarray delegates file I/O to a backend; installed backends here:
  available: ['netcdf4', 'scipy', 'zarr']
  'netcdf4' (the netCDF-C/HDF5 library) is the default for .nc when installed;
  alternatives are 'h5netcdf' (pure-HDF5) and 'scipy' (netCDF3 only).
  open_dataset(engine='netcdf4') -> 2 vars, sizes {'time': 31, 'y': 16, 'x': 16}
```

Compression is opt-in, per variable, through `encoding`:

```python
comp = {"zlib": True, "complevel": 4}
ds.to_netcdf(packed, encoding={name: comp for name in ds.data_vars})
```

```text
Default netCDF4 output is uncompressed; per-variable zlib encoding shrinks it:
  in-memory data:   124.0 KiB
  uncompressed .nc: 133.9 KiB
  zlib level 4 .nc: 101.5 KiB  (76% of uncompressed)
  compressed round-trip lossless: True
```

Note the uncompressed file is *larger* than the in-memory data — 133.9 against
124.0 KiB — because the file also carries metadata and coordinates. And note
that zlib only reaches 76 percent: the data is gaussian noise on a smooth field,
which is close to incompressible. Real fields with spatial structure compress
much better, and lossy tricks (`scale_factor` with `int16`) do far better again
at the cost of precision.

**Why it matters.** netCDF is the entry point. ERA5, CHIRPS, CMIP output, and
essentially every public climate archive ships netCDF or GRIB. An ingestion
pipeline's first act is `open_dataset`, and its second is normalising what comes
out — dimension names, units, dtypes, time encoding — before rewriting as zarr.

The one-file-per-dataset structure is exactly what makes netCDF a poor serving
format and a fine archive format. There is no way to fetch one month of one
variable without either a server that understands the file (OPeNDAP) or reading
across the whole thing. That limitation is what zarr exists to remove, which is
the next example.

**Traps.**

- **`open_dataset` holds the file open.** The example calls `.load()` inside the
  `with` block for exactly that reason. Reading inside a temporary directory and
  then using the dataset after cleanup fails, sometimes only on Windows. Use
  `load_dataset` for small data, or a context manager plus explicit `.load()`.
- **`h5netcdf` is listed as an alternative but is not installed here.** The
  engine list is what is actually available in the environment; do not assume a
  named engine exists.
- **netCDF3 has no compression, no groups, no 64-bit offsets by default.** If
  `engine="scipy"` is in play, that is what you have.
- **Compression settings live in `encoding`, not `attrs`**, and are per variable.
  Passing one dict for the whole dataset does not work; the example builds a
  dict comprehension over `data_vars` for that reason.
- **`to_netcdf` on a dask-backed dataset computes everything**, in chunks, and
  writes serially. It works; it is not parallel the way a zarr write is.

### `0502_zarr_basics` — what a store actually is

Source: [`xarray/examples/0502_zarr_basics.py`](../../xarray/examples/0502_zarr_basics.py)

**What it teaches.** That a zarr store is a directory of ordinary files at
predictable paths, and why that single fact is the reason cloud-native climate
serving works.

```python
ds = temperature_dataset(days=10, ny=8, nx=8)
ds.to_zarr(store, consolidated=False, encoding={"t2m": {"chunks": (5, 8, 8)}})
back = xr.open_zarr(store, consolidated=False)
```

```text
open_zarr() reads it back -- attrs, coords, and values all survive:
  data_vars: ['t2m'], coords: ['time', 'y', 'x']
  t2m attrs: {'units': 'degC', 'long_name': '2 metre temperature'}
  values identical: True, time axis length: 10
```

Then the store gets walked, which is the point of the example:

```text
The store is a plain directory tree -- every piece is an ordinary file:
  climate.zarr/
        zarr.json  (66 B)
        t2m/
            zarr.json  (810 B)
            c/
                0/
                    0/
                        0  (2398 B)
                1/
                    0/
                        0  (2397 B)
        time/
            zarr.json  (742 B)
            c/
                0  (42 B)
        x/
            zarr.json  (691 B)
            c/
                0  (73 B)
        y/
            zarr.json  (691 B)
            c/
                0  (73 B)
  10 files total
```

Ten files. One root `zarr.json` describing the group, one per array describing
its shape and chunk grid, and one file per chunk under `c/` with a path
component per dimension.

The metadata is small and readable:

```python
with open(os.path.join(store, "t2m", "zarr.json")) as f:
    arr_meta = json.load(f)
chunk_shape = arr_meta["chunk_grid"]["configuration"]["chunk_shape"]
```

```text
zarr.json documents describe the hierarchy; the root one is the group:
  root zarr.json:     node_type='group', zarr_format=3
  t2m/zarr.json:      shape=[10, 8, 8], chunk_shape=[5, 8, 8], dtype='float64'
  a client needs only these small JSON files to know exactly which chunk paths exist
```

And the chunk paths are computable, not discovered:

```text
Chunks live under c/ with one path component per dimension (time/y/x):
  t2m/c/0/0/0  exists=True
  t2m/c/1/0/0  exists=True
  t2m/c/2/0/0  exists=False
  10 days / 5-day chunks = 2 time chunks -> c/0/0/0 and c/1/0/0; there is no chunk 2
  fetching days 0-4 touches only t2m/c/0/0/0 -- one HTTP GET against a static file server
```

**Why it matters.** This is the whole cloud-native story, and it is much simpler
than it sounds. A client that wants days 0 to 4 does two things: fetch
`t2m/zarr.json` (810 bytes) to learn the shape and chunk grid, then compute that
the data it wants is in chunk index `(0, 0, 0)` and fetch `t2m/c/0/0/0`. Two
HTTP GETs against a static file server. No server-side compute, no query
protocol, no range requests, no database.

That is why OCS can serve zarr stores over plain HTTP and why the same store
works from S3, from a CDN, or from a laptop. The format is a naming convention
plus JSON.

The `consolidated=False` argument appears in every zarr call in this project.
Consolidated metadata is a v2-era optimisation that packs all the `zarr.json`
documents into one file to save round trips; it is not part of the v3 spec, and
passing `False` keeps the stores standard and the output warning-free. On a real
store over high-latency object storage the round-trip saving is genuinely
valuable, so this is a deliberate trade for clarity, not a recommendation.

**Traps.**

- **`to_zarr` refuses to overwrite by default.** `mode="w"` overwrites,
  `mode="a"` appends into an existing store. The default `mode="w-"` errors if
  the store exists.
- **`open_zarr` is lazy; `open_dataset(..., engine="zarr")` is the more general
  entry point** and gives finer control over decoding.
- **Chunk files have no extension and no header you can read by eye.** They are
  compressed blocks. Do not expect to inspect one with a text editor.
- **A missing chunk file is not an error** — zarr treats it as the fill value.
  That is a feature for sparse writes and a silent corruption vector if a file
  is lost.
- **The `c/` prefix is v3.** Zarr v2 stores use dot-separated names like
  `t2m/0.0.0` at the array root. If you are reading a store written years ago,
  expect the other layout.

### `0503_zarr_chunks_encoding` — chunk shape is the performance decision

Source: [`xarray/examples/0503_zarr_chunks_encoding.py`](../../xarray/examples/0503_zarr_chunks_encoding.py)

**What it teaches.** That the same data written four ways produces wildly
different file layouts, and what the trade-off actually is.

One dataset, `(time=120, y=64, x=64)` float64, written with four chunk shapes:

```python
def write_and_report(ds, store, chunks, label):
    encoding = {} if chunks is None else {"t2m": {"chunks": chunks}}
    ds.to_zarr(store, consolidated=False, encoding=encoding)
    back = xr.open_zarr(store, consolidated=False)
    written = back.t2m.encoding["chunks"]
    n_files = count_chunk_files(store, "t2m")
    print(f"  chunks={str(written):>14}  -> {n_files:3d} chunk files   ({label})")
```

```text
Same data, four chunk choices -- encoding={'t2m': {'chunks': ...}} at write time:
  number of files = ceil(120/t) * ceil(64/y) * ceil(64/x):
  chunks=  (60, 32, 32)  ->   8 chunk files   (no encoding: zarr picks automatically)
  chunks=   (1, 64, 64)  -> 120 chunk files   (one file per day: append-friendly, tiny files)
  chunks= (120, 16, 16)  ->  16 chunk files   (spatial tiles: full history per tile)
  chunks=  (30, 64, 64)  ->   4 chunk files   (OCS-style: ~30 time steps, spatial capped)
```

Same 3.75 MiB of data, between 4 and 120 files depending only on the chunk
shape. The formula is exact and worth internalising: the file count is the
product over dimensions of `ceil(size / chunk)`.

The OCS-style choice is explained and measured:

```text
The OCS-style choice, time chunk ~30 and spatial capped (here 64 fits in one tile):
  - a monthly ingestion period lands as whole chunks (see 0504)
  - 'map for one month' reads 1 file; 'series at one point' reads 4 files
  - chunks stay ~1 MiB: big enough to compress well, small enough to fetch fast
  one OCS-style chunk file on disk: 900951 B (compressed 30x64x64 block)
```

Reopening reveals the full write-time encoding:

```text
Reopening reveals the full write-time encoding, compressor included:
  chunks:      (30, 64, 64)
  compressors: (ZstdCodec(level=0, checksum=False),)
  serializer:  BytesCodec(endian='little')
  zarr v3 defaults to zstd; each chunk file holds one compressed block

The in-memory dataset never changed -- chunking is purely a storage decision:
  ds.t2m.encoding before any write: {}
```

**Why it matters.** Chunk shape determines which reads are cheap. It is chosen
once, at write time, and changing it later means rewriting the store.

The two extremes in the table are both defensible and both wrong for a general
climate service. `(1, 64, 64)` — one file per day — makes daily appends trivially
cheap and makes "the time series at one point" fetch 120 files. `(120, 16, 16)`
— full history per spatial tile — makes the point query fetch one file and makes
appending a day rewrite every one of the 16 files, because every chunk spans the
whole time axis.

The OCS-style middle, roughly one ingestion period per time chunk with spatial
dims capped, is a compromise tuned to two specific access patterns: "the map for
one month" (one file) and "the series at one place" (one file per time chunk).
And critically, because the time chunk equals the ingestion period, an append
adds whole new files rather than rewriting existing ones — which is what the
next example demonstrates.

The general guidance from the dask side of the stack is roughly 100 MB per chunk
for compute-heavy workloads. For serving, smaller is better because a client
pays for the whole chunk to get any of it. Around 1 to 10 MiB is a reasonable
serving target; this example's `900951` bytes is right in that band.

**Traps.**

- **Encoding is per variable and per write.** A dataset written twice with
  different encoding produces differently chunked stores from identical data.
- **Stale encoding on rewrite.** A dataset opened from a store carries that
  store's `chunks` in `encoding`. Write it after rechunking and the two disagree.
  Clear it or override it explicitly.
- **Zarr's automatic choice is not tuned for your access pattern.** It picked
  `(60, 32, 32)` here, which is fine and arbitrary.
- **Too many small files is a real cost on object storage**, where each is a
  separate request with its own latency and, in a cloud, its own price.
- **Compression happens per chunk.** Tiny chunks compress poorly because there is
  less redundancy to exploit within each block.
- **`shards` (v3) can decouple the storage unit from the chunk unit** — many
  chunks packed into one file — which is the modern answer to the small-file
  problem. It appears in the encoding keys above; this project does not use it.

### `0504_zarr_append_region` — growing and healing a store

Source: [`xarray/examples/0504_zarr_append_region.py`](../../xarray/examples/0504_zarr_append_region.py)

**What it teaches.** The two write modes that make a store a living thing rather
than an artifact: `append_dim` to extend, `region` to correct in place.

The initial write fixes the chunk grid:

```python
period_1.to_zarr(store, consolidated=False, encoding={"t2m": {"chunks": (30, 16, 16)}})
```

```text
Step 1 -- initial period, written with time chunks of 30 (one period per chunk):
  time axis:   30 days: 2024-01-01 .. 2024-01-30
  chunk files: 1 (30 days / 30-day chunks = 1)
```

The append extends it:

```python
period_2.to_zarr(store, append_dim="time", consolidated=False)
```

```text
Step 2 -- next period arrives; to_zarr(append_dim='time') extends the store:
  time axis:   60 days: 2024-01-01 .. 2024-02-29
  chunk files: 2 (append added exactly one new chunk file)
  the period length matches the time chunk, so old chunk files were never touched

Reopening sees one continuous dataset -- readers never know it arrived in parts:
  values match the 60-day source exactly: True
```

One new file, zero existing files touched. That is the payoff for aligning the
period length with the time chunk size, and it is why `0503` chose a 30-step
time chunk. If the period were 45 days against 30-day chunks, the append would
have to read, modify, and rewrite the partially-filled boundary chunk.

The region write corrects a slice:

```python
corrected = (full.isel(time=slice(10, 20)) + 5.0).drop_vars(["y", "x"])
corrected.to_zarr(store, region={"time": slice(10, 20)}, consolidated=False)
```

```text
Step 3 -- days 10..19 turn out to be wrong; rewrite just that slice with region=:
  (drop_vars(['y', 'x']): a region write may only carry variables that overlap the region,
   so coords without a 'time' dim must be dropped)
  mean shift vs original -- day 9: +0.00, day 15: +5.00, day 25: +0.00
  time axis unchanged: 60 days: 2024-01-01 .. 2024-02-29
  only the targeted slice changed; the store's shape and chunk grid did not move
```

The verification is precise: day 9 unchanged, day 15 shifted by exactly the
`+5.0` that was written, day 25 unchanged. The write hit exactly the ten days it
targeted.

**Why it matters.** Together these two modes are the entire storage lifecycle of
a streaming climate store. New periods append. Corrected or reprocessed periods
are rewritten in place. The store grows monotonically and heals selectively, and
is never rewritten whole.

The `drop_vars(["y", "x"])` detail is the practical gotcha that costs everyone an
hour the first time. A region write may only carry variables that span the
region dimension. `y` and `x` are coordinates with no `time` dimension, so they
do not overlap `region={"time": ...}` and must be dropped from the payload. They
are already in the store and unchanged; the write is not trying to modify them.

**Traps.**

- **Neither mode validates the time axis.** `append_dim` appends whatever you
  give it, including a period already present. The guards from `0402` apply
  directly and are the caller's responsibility.
- **`region` requires the region to already exist.** You cannot region-write past
  the end of the array; that is what `append_dim` is for.
- **`region` slices are positional, not label-based.** `slice(10, 20)` is index
  10 to 19, not dates. Compute the offsets from the store's time index, and get
  them wrong at your peril — a misaligned region write corrupts silently.
- **A region write that does not align with chunk boundaries triggers
  read-modify-write** of the straddled chunks, which is slow and, on a plain zarr
  store with a concurrent reader, briefly inconsistent.
- **There is no transaction.** A crashed append or region write leaves the store
  partially updated with no record of what landed. That gap is precisely what
  the `icechunk` project in this repository exists to fill.
- **`append_dim` with mismatched non-append dimensions fails late**, after some
  chunks have already been written.
---

## Phase 6 — Dask-backed xarray

Four examples on the same API, lazy. Nothing about the vocabulary changes;
everything about when work happens does.

**OCS relevance:** stores are opened chunked, computation is deferred until a
write or an explicit compute, and the chunk layout at write time has to be legal
for zarr.

!!! note "Timings below are machine-dependent"
    Every wall-clock number in this phase varies between runs and between
    machines, sometimes substantially. The ratios and orders of magnitude are
    the lesson; the digits are not.

### `0601_chunked_open` — what "dask-backed" means

Source: [`xarray/examples/0601_chunked_open.py`](../../xarray/examples/0601_chunked_open.py)

**What it teaches.** How to tell a lazy dataset from an eager one, and that
opening a store reads metadata only.

The numpy baseline:

```python
ds = temperature_dataset(days=365, ny=128, nx=128)
```

```text
A synthetic year of daily temperature, fully in memory (numpy-backed):
  sizes:        {'time': 365, 'y': 128, 'x': 128}
  t2m backing:  numpy.ndarray
  t2m .chunks:  None  (None means: not chunked, all in memory)
  repr line:    t2m      (time, y, x) float64 48MB 28.1 27.89 28.51 ... 23.53 24.76 24.13
```

`.chunk()` swaps the backing:

```python
chunked = ds.chunk({"time": 30, "y": 64, "x": 64})
```

```text
.chunk() re-backs every variable with a dask array -- same values, new engine:
  t2m backing:  dask.array.core.Array
  t2m .chunks:  ((30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 5), (64, 64), (64, 64))
  block count:  52 blocks of at most (30, 64, 64)
  repr line:    t2m      (time, y, x) float64 48MB dask.array<chunksize=(30, 64, 64), meta=np.ndarray>
```

Three diagnostics, and it is worth having all three to hand:

| Check | numpy-backed | dask-backed |
|---|---|---|
| `da.data` type | `numpy.ndarray` | `dask.array.core.Array` |
| `da.chunks` | `None` | tuple of tuples |
| repr line | first and last values | `dask.array<chunksize=...>` |

The `.chunks` value is a tuple per dimension listing **every** block's size, not
the requested size. Twelve 30-day blocks and a final 5-day block, because
365 is not divisible by 30. That trailing remainder is legal for zarr — only the
last chunk may differ — and it is the seed of the trap in `0603`.

Writing and reopening:

```text
to_zarr() computes each dask block and writes it as one zarr chunk:
  wrote t2m.zarr in 0.130 s, 60 files on disk

open_zarr() is lazy by default -- it reads metadata files, zero data chunks:
  opened in 5.7 ms
  t2m backing:  dask.array.core.Array
  t2m .chunks:  ((30, 30, ..., 5), (64, 64), (64, 64))  (store chunking becomes dask chunking)

The data itself is read only when something forces computation:
  .compute() read + decompressed everything in 32.2 ms
  open was 6x faster than load: opening never touched the data
  loaded backing: numpy.ndarray
```

(Chunk tuple elided above; the full output repeats the 13-element list.)

**Why it matters.** The open-versus-load ratio is the entire argument for lazy
I/O. Here it is only 6x because the dataset is 48 MB and the disk is local. On a
100 GB store over object storage the open still takes milliseconds and the load
takes minutes — the ratio grows without bound, because opening cost is a
function of the *metadata* size, not the data size.

The other half is that the store's chunk grid becomes the dask chunk grid
automatically. The layout chosen at write time in `0503` propagates into every
subsequent computation's parallelism. Choosing chunks is choosing both the
storage layout and the compute layout at once, which is why it is the decision
that matters most.

**Traps.**

- **`.chunk()` on already-in-memory data does not save memory.** The data is
  already there; you have added a scheduler on top. It is useful for testing a
  pipeline's chunking, not for making a big array fit.
- **`.values` and `float()` force a full compute.** So does `print`ing values,
  `to_dataframe`, and most plotting. In a lazy pipeline these are the accidental
  materialisation points.
- **The chunk sizes reported are per block and irregular at the ends.** Do not
  assume `chunks[0][0]` describes them all.
- **Coordinates are usually not chunked.** They are small and loading them eagerly
  is what makes `sel` work without touching data. A pathologically large
  coordinate breaks that assumption.
- **`open_zarr` decodes by default** — CF times, `_FillValue`, scale factors. That
  decoding is why `time` comes back as `datetime64` rather than integers, and it
  can be turned off with `decode_cf=False` when you need the raw values.

### `0602_lazy_graphs` — building is free, executing is not

Source: [`xarray/examples/0602_lazy_graphs.py`](../../xarray/examples/0602_lazy_graphs.py)

**What it teaches.** That chained operations grow a task graph rather than doing
work, and the difference between the three materialisation verbs.

```python
ds = temperature_dataset(days=365, ny=128, nx=128).chunk({"time": 30, "y": 64, "x": 64})
t2m = ds.t2m

climatology = t2m.mean("time")
anomaly = t2m - climatology
monthly = anomaly.resample(time="MS").mean()
```

```text
One year of daily temperature, chunked into (30, 64, 64) blocks:
  tasks in the graph so far: 52 (one task per block)

Each operation appends tasks to the graph; wall time stays near zero:
  anomaly = t2m - t2m.mean('time')      ->  180 tasks, 2.0 ms
  monthly = anomaly.resample('MS').mean ->  460 tasks, 7.6 ms
  monthly is still lazy: backing = dask.array.core.Array, sizes = {'time': 12, 'y': 128, 'x': 128}
```

Fifty-two tasks (one per block) becomes 180 after the anomaly, 460 after the
resample. Total build time under 10 ms, and no data has been read or written.
The `monthly` object knows its shape — `{'time': 12, 'y': 128, 'x': 128}` — while
containing none of its values.

Then the three verbs:

```python
result = monthly.compute()    # new in-memory object; original stays lazy
obj = monthly.copy()
returned = obj.load()          # same object, filled in place; returns self
persisted = monthly.persist()  # computed, but still dask-backed
```

```text
.compute() runs the graph and returns a NEW numpy-backed object:
  ran 460 tasks in 0.048 s on the threaded scheduler
  result backing:  numpy.ndarray
  monthly backing: dask.array.core.Array  (the original is STILL lazy)
  January mean anomaly at cell (0, 0): +0.753 degC

.load() computes too, but mutates the object it is called on:
  returned is obj: True  (load returns self)
  obj backing after load: numpy.ndarray  (dask replaced by numpy in place)

.persist() runs the graph and keeps results in memory AS dask chunks:
  persisted backing: dask.array.core.Array  (still dask)
  tasks: 460 before persist -> 48 after
  (only the materialized blocks remain; the whole recipe collapsed)
  computing the persisted object: 1.4 ms vs 48.3 ms from scratch
```

The persist numbers are the clearest signal: the 460-task recipe collapses to 48
tasks (one per materialised block), and re-computing goes from 48.3 ms to
1.4 ms because there is nothing left to do but hand back blocks.

| Verb | Returns | Original | Result backing | Use when |
|---|---|---|---|---|
| `.compute()` | new object | untouched, still lazy | numpy | you want the values once |
| `.load()` | self | mutated in place | numpy | you want this object filled |
| `.persist()` | new object | untouched | dask, materialised | you will reuse it many times |

**Why it matters.** Two consequences shape how a pipeline is written.

**Intermediates are free.** Creating `climatology`, `anomaly`, and `monthly` as
named variables costs nothing. There is no reason to write dense one-liners to
"avoid intermediates"; readable step-by-step code has identical cost.

**Recomputation is the default.** `monthly` stays lazy after `compute()`, so
computing it again re-runs all 460 tasks. Using the same intermediate in three
outputs re-reads and re-derives it three times unless you `persist()` it. That
is the case `persist` exists for: it trades memory for not repeating work, which
is exactly the right trade for a branching pipeline and exactly the wrong one
for a linear one.

**Traps.**

- **`.compute()` pulls everything into the client's memory.** It is only safe
  when the *result* fits, not when the input does. `monthly` here is 12 x 128 x
  128; the input was 365 x 128 x 128.
- **`.persist()` fills memory silently.** It is the most common way to OOM a
  cluster: persist a large intermediate, forget it is there, persist another.
- **Accidental computes are everywhere.** `float()`, `bool()`, `.values`, `if
  da:`, printing values, most plots. Watch for them in logging code especially,
  where they hide in f-strings.
- **Task count is a rough proxy for graph complexity, not for cost.** A 460-task
  graph over 48 MB is trivial; a 460-task graph over 48 TB is not.
- **`resample` and `groupby` fragment chunks**, which is why 460 tasks appear for
  what looks like a simple operation. See the pitfalls section.

### `0603_rechunking` — the non-uniform chunk trap

Source: [`xarray/examples/0603_rechunking.py`](../../xarray/examples/0603_rechunking.py)

**What it teaches.** How ordinary operations produce chunk layouts that zarr
refuses to write, reproduced end to end with the real error and the real fix.

Rechunking itself is unremarkable and lazy:

```python
ds = temperature_dataset(days=60, ny=64, nx=64).chunk({"time": 30, "y": 64, "x": 64})
rechunked = ds.chunk({"time": 15})
```

```text
Rechunking is just .chunk() with a new layout -- lazily, like everything else:
  original chunks:  {'time': (30, 30), 'y': (64,), 'x': (64,)}
  after .chunk({'time': 15}): {'time': (15, 15, 15, 15), 'y': (64,), 'x': (64,)}
```

The trap is built in two steps that are both completely normal:

```python
north = ds.isel(y=slice(0, 40)).chunk({"y": 32})
south = ds.isel(y=slice(40, 64)).chunk({"y": 32})
combined = xr.concat([north, south], dim="y")
flipped = combined.isel(y=slice(None, None, -1))     # north-up flip
```

```text
The trap: concat unequal pieces, then flip the axis. Two tiles along y:
  north tile (40 rows, y chunked 32): y chunks (32, 8)
  south tile (24 rows, y chunked 32): y chunks (24,)
  concat along y:                     y chunks (32, 8, 24)  <- NON-UNIFORM interior
  north-up flip isel(y=::-1):         y chunks (24, 8, 32)
  Reversal keeps block boundaries but reverses their order -- the odd-sized
  chunk is now FIRST, so not even the 'last chunk may differ' rule saves us.
```

Follow the arithmetic. The north tile is 40 rows chunked at 32, giving `(32, 8)`
— legal, the remainder is last. The south tile is 24 rows, giving `(24,)` —
legal. Concatenating them gives `(32, 8, 24)`, where the 8 is now in the middle,
which is not legal. Then the north-up flip reverses to `(24, 8, 32)`, where even
the last chunk is the biggest.

The write fails:

```python
flipped.to_zarr(bad_store, consolidated=False)
```

```text
Zarr chunks must be uniform (only the final chunk may be smaller):
  to_zarr raised ValueError:
    Zarr requires uniform chunk sizes except for final chunk. Variable named 't2m' has incompatible dask chunks: ((30, 30), (24, 8, 32), (64,)). Consider rechunking using `chunk()`.
```

And the fix is one call:

```python
uniform = flipped.chunk({"y": 32})
uniform.to_zarr(good_store, consolidated=False)
```

```text
The fix: rechunk to a uniform layout before writing (OCS: _uniform_chunks):
  after .chunk({'y': 32}): y chunks (32, 32)
  to_zarr wrote good.zarr in 0.018 s
  reopened store chunks: {'time': (30, 30), 'y': (32, 32), 'x': (64,)}
```

**Why it matters.** Zarr stores one chunk shape per array in its metadata.
Dask stores a full list of block sizes per dimension. The formats are not
equivalent, and dask's is strictly more expressive — so there are dask layouts
with no zarr representation.

The two operations that produce them are the two most ordinary operations in
geospatial ingestion: concatenating tiles that arrived at different sizes, and
flipping latitude to be north-up. OCS carries a `_uniform_chunks` helper for
precisely this, and the shape of that helper is exactly the fix above: before
every write, force a uniform layout.

The general rule that follows: **rechunk immediately before the write, not
earlier**. Chunking chosen mid-pipeline gets destroyed by the next `concat` or
reversal. A final explicit `.chunk({...})` matching the store's encoding is the
only reliable place.

**Traps.**

- **The error arrives at write time**, arbitrarily far from the operation that
  caused it. Inspect `.chunks` after any concat or reversal.
- **`.chunk()` on a dask array is lazy but not free.** It inserts a rechunk into
  the graph, which shuffles bytes between blocks at compute time. It is the most
  expensive common operation in a dask pipeline.
- **Non-uniform chunks are perfectly fine for computation.** Nothing is wrong
  until you try to write. That is why they survive so long undetected.
- **`isel` with a step other than 1 also produces odd layouts**, not just
  reversal.
- **Making chunks uniform can be lossy for performance.** `chunk({"y": 32})` on
  a `(24, 8, 32)` layout has to move data across every block boundary.

### `0604_map_blocks_apply_ufunc` — escaping to numpy per block

Source: [`xarray/examples/0604_map_blocks_apply_ufunc.py`](../../xarray/examples/0604_map_blocks_apply_ufunc.py)

**What it teaches.** The two hatches for running code xarray has no lazy method
for, and how to choose between them.

The setup puts the reduced dimension in a single chunk, which is the
precondition:

```python
ds = temperature_dataset(days=365, ny=128, nx=128).chunk({"time": -1, "y": 64, "x": 64})
```

```text
A year of daily temperature; time in ONE chunk, space in 64x64 tiles:
  chunks: {'time': (365,), 'y': (64, 64), 'x': (64, 64)}
  (a reduced ('core') dim must live in a single chunk for apply_ufunc)
```

`apply_ufunc` wraps a plain numpy function:

```python
def p90_along_last_axis(values: np.ndarray) -> np.ndarray:
    """Return the 90th percentile along the trailing axis of a numpy array."""
    return np.asarray(np.percentile(values, 90.0, axis=-1))


warm = xr.apply_ufunc(
    p90_along_last_axis,
    t2m,
    input_core_dims=[["time"]],
    dask="parallelized",
    output_dtypes=[np.float64],
)
```

```text
xr.apply_ufunc: wrap a plain numpy function, parallelized over chunks:
  built lazily in 1.3 ms: backing = dask.array.core.Array, sizes = {'y': 128, 'x': 128}
  input_core_dims=[['time']] moved time to the last axis and consumed it
  computed in 0.028 s; max |diff| vs eager numpy: 0.00e+00
  sample: 90th-percentile temperature at cell (0, 0) = 31.07 degC
```

`max |diff| vs eager numpy: 0.00e+00` — bit-for-bit identical to computing the
whole thing in memory. That check is the reason to trust the hatch.

`input_core_dims=[["time"]]` is the key argument and it does two things:
transposes `time` to the last axis before calling the function (which is why the
function reduces `axis=-1`), and declares that the function consumes it, so it
does not appear in the output.

`map_blocks` hands over a labeled `DataArray` instead:

```python
def standardize_block(block: xr.DataArray) -> xr.DataArray:
    """Standardize one block per cell over its own time axis.

    Safe here only because time is a single chunk, so each block sees the
    full time series for its cells.
    """
    return (block - block.mean("time")) / block.std("time")


zscore = t2m.map_blocks(standardize_block, template=t2m)
```

```text
DataArray.map_blocks: your function receives a labeled DataArray per block,
so it can use coords, dim names, the whole xarray API:
  built lazily in 3.0 ms: backing = dask.array.core.Array
  (template=t2m declares the output layout, skipping trial inference)
  computed in 0.033 s
  per-cell check at (0, 0): mean = +1.07e-14, std = 1.000
```

A z-score has mean 0 and std 1 by construction; `+1.07e-14` and `1.000` confirm
the block function ran on real data with real labels.

And then the pitfall the docstring warns about:

```text
Pitfall: each block sees ONLY its block. A reduction over a chunked dim
inside map_blocks silently computes per-block answers, not global ones.
Here time was one chunk, so per-block mean/std over time was the real thing.
```

The decision guide:

```text
When each escape hatch fits:
  apply_ufunc + dask='parallelized' -- you have a numpy-signature function
    (ufunc-like, axis-based); core dims are consumed; fastest, least overhead
  map_blocks -- your logic needs labels: coords, .sel, groupby, resample
    per block; xarray-in, xarray-out; slightly more overhead per block
  neither -- if a native lazy xarray method exists, always prefer it
```

**Why it matters.** Custom per-pixel science — percentile-based indices, spell
lengths, fitted distributions, model code that only exists as a numpy function —
does not have a native lazy xarray method, and rewriting it as one is rarely
worth it. These two hatches let arbitrary code run per chunk while the pipeline
stays lazy and parallel.

The blockwise pitfall is the thing to be genuinely careful about, because it
fails **silently and plausibly**. If `time` had been chunked into twelve blocks,
`standardize_block` would have standardised each month against its own mean and
standard deviation, produced an array of exactly the right shape and dtype, and
been wrong. No error, no warning, and the values look reasonable. The rule is
absolute: **any dimension your block function reduces over must be in a single
chunk**, and `.chunk({"time": -1})` is how you guarantee it.

**Traps.**

- **Core dims must be single-chunk.** `apply_ufunc` raises if you forget;
  `map_blocks` does not.
- **`output_dtypes` is required with `dask="parallelized"`.** Dask cannot infer
  the dtype without running the function.
- **Without `template`, `map_blocks` runs the function once on a dummy** to infer
  the output structure. That is slow and breaks on functions with side effects or
  strict input validation.
- **`input_core_dims` reorders the axes.** The function sees core dims last, in
  the order given. Writing the function against the original order is a common
  mistake.
- **The function is pickled and shipped** in a distributed setting. Closures over
  large objects get serialised with it.
- **Prefer a native method whenever one exists.** They are better optimised and
  handle chunk boundaries correctly. Escape hatches are for what is genuinely
  missing.
---

## Phase 7 — Conventions and interop

Three examples on the metadata and edge-case handling that separate code that
works on your data from code that works on anyone's: CF attributes, calendars,
and missing values.

### `0701_cf_attrs_units` — inert metadata, and what to do about it

Source: [`xarray/examples/0701_cf_attrs_units.py`](../../xarray/examples/0701_cf_attrs_units.py)

**What it teaches.** What CF attributes are, that xarray does not interpret
them, how propagation actually behaves, and the ingest-time pattern that follows
from all of it.

The CF vocabulary on a variable:

```python
ds["t2m"].attrs["standard_name"] = "air_temperature"   # from the CF standard name table
```

```text
A CF-described variable carries units, long_name, and standard_name:
  t2m.attrs['units'] = 'degC'
  t2m.attrs['long_name'] = '2 metre temperature'
  t2m.attrs['standard_name'] = 'air_temperature'
  units/long_name are free text; standard_name must come from the CF table
```

The distinction in that last line matters. `long_name` is prose for humans;
`standard_name` is a controlled vocabulary term from the CF standard name table,
which is what makes automated interpretation possible across datasets from
different institutions.

Coordinates get the same treatment:

```python
ds["y"].attrs.update({"units": "degrees_north", "standard_name": "latitude", "axis": "Y"})
ds["x"].attrs.update({"units": "degrees_east", "standard_name": "longitude", "axis": "X"})
```

```text
Coordinates get the same treatment (units, standard_name, axis):
  y.attrs = {'units': 'degrees_north', 'standard_name': 'latitude', 'axis': 'Y'}
  x.attrs = {'units': 'degrees_east', 'standard_name': 'longitude', 'axis': 'X'}
  time gets no units attr: CF time units ('days since ...') live in encoding, applied at write time
```

The `axis` attribute is the CF mechanism for saying "this is the Y axis" without
relying on the variable's name — which is what lets a tool find latitude in a
file where it is called `nav_lat` or `rlat`.

Then the demonstration that all of it is decoration:

```python
celsius = ds["t2m"]
kelvin = (celsius + 273.15).assign_attrs(units="K", long_name="2 metre temperature")
mixed = celsius + kelvin
```

```text
xarray never reads attrs -- units are decoration, not behavior:
  celsius mean          =   26.30 [degC]
  kelvin mean           =  299.45 [K]
  celsius + kelvin mean =  325.74 -- nonsense, computed without complaint
  (a units-aware layer like pint-xarray can check this; plain xarray does not)
```

Propagation, and its blindness:

```text
Whether operations propagate attrs is governed by the keep_attrs option:
  celsius.mean().attrs = {'units': 'degC', 'long_name': '2 metre temperature', 'standard_name': 'air_temperature'}
    (this xarray keeps attrs by default; older releases dropped them on reductions)
  (celsius + kelvin).attrs = {'long_name': '2 metre temperature', 'standard_name': 'air_temperature'}
    (attrs the operands agree on survive; the conflicting 'units' was quietly dropped)
  with xr.set_options(keep_attrs=False): celsius.mean().attrs = {}
  the default has flipped across versions -- set the option explicitly, never assume

Propagation is a blind copy; it cannot know which operations invalidate metadata:
  celsius.mean().attrs['units'] = 'degC'  (correct: a mean keeps its units)
  celsius.var().attrs['units']  = 'degC'  (now wrong: variance is degC squared)
```

Three separate behaviours in that block, all worth noting. Reductions keep attrs
in this version. Binary operations keep only attrs the operands agree on, which
means a units conflict results in **no units at all** rather than an error.
And the copy is blind, so `var()` claims degrees Celsius for a quantity in
degrees Celsius squared.

The pattern that follows:

```python
ingested = (era5_kelvin - 273.15).assign_attrs(era5_kelvin.attrs, units="degC")
```

```text
Because attrs are inert, OCS converts units once at ingest and rewrites attrs to match:
  source:   mean  299.45 [K]
  ingested: mean   26.30 [degC]
  every store then speaks degC; downstream code trusts the convention, not the metadata
```

**Why it matters.** The reasoning chain is worth spelling out because it is the
justification for a lot of pipeline design.

Attrs are inert, so nothing downstream will convert for you. Different sources
use different units — ERA5 gives temperature in Kelvin, many observation
datasets give Celsius, precipitation arrives as m/day, mm/day, or kg/m2/s. If
units are normalised at every point of use, that logic is duplicated everywhere
and eventually one copy is wrong. So it must be normalised **once**, at ingest,
with the attrs rewritten to match, after which every store speaks the same
units and downstream code can trust the **convention** rather than reading the
metadata.

The last phrase is the point. The convention is enforced by ingest code and by
tests, not by the `units` string. The string is documentation.

**Traps.**

- **`keep_attrs` defaults have changed across xarray versions.** Set it
  explicitly in library code.
- **Conflicting attrs are dropped, not reconciled.** Losing `units` silently is
  worse than keeping a wrong one, because the next writer will not notice it is
  missing.
- **`var()`, `std()`, ratios, counts, and normalised indices all invalidate
  `units`.** Nothing warns.
- **`standard_name` must come from the CF table.** An invented value is worse
  than none, because tools trust it.
- **Attrs must be serialisable.** Nested structures fail at write time.
- **`pint-xarray` is the real answer if you need enforcement.** It attaches a
  unit system and makes `degC + K` an error.

### `0702_time_handling` — datetime64, `.dt`, and calendars

Source: [`xarray/examples/0702_time_handling.py`](../../xarray/examples/0702_time_handling.py)

**What it teaches.** What a proper time coordinate unlocks, the calendar-field
accessor, partial-string selection, the frequency vocabulary, and where
non-standard calendars come in.

```python
ds = temperature_dataset(days=91, ny=3, nx=4)   # 2024-01-01 .. 2024-03-31
time = ds["time"]
```

```text
A proper time coord is numpy datetime64, built here from pd.date_range:
  dtype: datetime64[us]
  span:  2024-01-01 .. 2024-03-31 (91 days)
  datetime64 is what unlocks .dt, partial-string selection, and resample
```

The `.dt` accessor exposes calendar fields as arrays:

```text
The .dt accessor exposes calendar fields as new DataArrays over time:
  time.dt.year[0]        = 2024
  time.dt.month (unique) = [1, 2, 3]
  time.dt.dayofyear      = 1 .. 91
  time.dt.season (unique)= ['DJF', 'MAM']  (meteorological: DJF, MAM, ...)
  these arrays are what groupby('time.month') and climatologies are built on
```

`DJF` and `MAM` are the meteorological seasons — December-January-February,
March-April-May — not the astronomical ones. A January day is in `DJF` along
with the *previous* December, which is what makes seasonal aggregation
awkward at year boundaries and is worth knowing before you trust a seasonal
groupby.

Partial-string selection:

```text
Partial string selection: a label like '2024-02' means the whole month:
  ds.sel(time='2024-02') -> 29 days  (not 28: 2024 is a leap year)
  ds.sel(time=slice('2024-01-15', '2024-02-15')) -> 32 days (both ends inclusive)
  mean t2m in February = 28.07 degC
```

Twenty-nine days, because the index knows the calendar. Positional arithmetic
would have needed to know that 2024 is a leap year.

The frequency vocabulary:

```text
pd.date_range frequency codes generate the axes you append along:
  freq='D'      -> 2024-01-01 00:00, 2024-01-02 00:00, 2024-01-03 00:00, 2024-01-04 00:00  (calendar day)
  freq='6h'     -> 2024-01-01 00:00, 2024-01-01 06:00, 2024-01-01 12:00, 2024-01-01 18:00  (every 6 hours)
  freq='W-MON'  -> 2024-01-01 00:00, 2024-01-08 00:00, 2024-01-15 00:00, 2024-01-22 00:00  (weekly, anchored on Mondays)
  freq='MS'     -> 2024-01-01 00:00, 2024-02-01 00:00, 2024-03-01 00:00, 2024-04-01 00:00  (month start)
  freq='ME'     -> 2024-01-31 00:00, 2024-02-29 00:00, 2024-03-31 00:00, 2024-04-30 00:00  (month end)
```

`MS` and `ME` produce the same buckets with different labels — month start
versus month end. `2024-02-29` in the `ME` row is the leap day again.

The example closes with prose on non-standard calendars, which is the part that
bites when you move from observations to model output:

```text
A word on non-standard calendars:
  Climate models often run on calendars real clocks do not: 'noleap' (no Feb 29)
  or '360_day' (12 x 30-day months). numpy datetime64 cannot represent those, so
  xarray decodes them into cftime objects instead (via the cftime package).
  Most of the API still works -- .dt, sel, resample -- but cftime coords are slower
  and do not mix with datetime64 axes. xr.date_range(..., calendar='noleap') builds
  them, and convert_calendar() moves data onto a standard calendar when needed.
  OCS sources (ERA5, CHIRPS) use the standard calendar, so datetime64 is the norm.
```

Verified independently, because "do not mix" understates it:

```python
noleap = xr.date_range("2024-01-01", periods=5, freq="D", calendar="noleap", use_cftime=True)
da = xr.DataArray(np.arange(5.0), dims="time", coords={"time": noleap})
print("dtype:", da.time.dtype, "element type:", type(noleap[0]).__name__)

result = temperature_dataset(days=5).t2m + da
print("result sizes:", dict(result.sizes))
```

```text
dtype: object element type: DatetimeNoLeap
result sizes: {'time': 0, 'y': 20, 'x': 30}
```

Zero time steps. No error. The two indexes have the same printed dates and share
not one label, so the inner join is empty. `convert_calendar("standard")` moves
a cftime axis onto `datetime64` and makes them comparable again.

**Why it matters.** Everything time-related in xarray — `sel` by date string,
`resample`, `groupby("time.month")`, `.dt` — requires a datetime-like
coordinate. An integer time axis or a string time axis has none of it, and the
first job of any ingest is to produce a proper one.

The calendar issue is the sharpest interop edge in the field. A pipeline built
against ERA5 (standard calendar, `datetime64`) that suddenly ingests CMIP model
output (`noleap` or `360_day`, `cftime`) does not crash — it produces empty
results, wherever the two meet. Calling `convert_calendar` at ingest, so that
every store speaks one calendar, is the same argument as normalising units, for
the same reason.

**Traps.**

- **cftime and datetime64 axes silently produce empty joins.** Above.
- **`datetime64[us]` here, `datetime64[ns]` elsewhere.** pandas 3 uses microsecond
  resolution by default and `convert_calendar` returned `datetime64[ns]`. Mixed
  resolutions usually align fine but can surprise on exact comparison.
- **Frequency aliases changed.** `"M"` -> `"ME"`, `"Y"` -> `"YE"`, `"H"` -> `"h"`.
- **`.dt.season` is meteorological**, and `DJF` spans a year boundary.
- **Time zones are a trap.** Climate data is conventionally UTC and naive. A
  tz-aware pandas index in a coordinate causes problems at write time.
- **`dayofyear` shifts by one after February in leap years**, which quietly
  corrupts day-of-year climatologies built across mixed leap and non-leap years.

### `0703_missing_data` — NaN in memory, `_FillValue` on disk

Source: [`xarray/examples/0703_missing_data.py`](../../xarray/examples/0703_missing_data.py)

**What it teaches.** The full missing-data lifecycle: how gaps get in, how to
measure them, two ways to repair them, and what happens at the storage boundary.

Gaps are introduced with `where`, simulating a sensor down every fifth day:

```python
t2m = temperature_dataset(days=30, ny=4, nx=5)["t2m"]
gappy = t2m.where(t2m.time.dt.day % 5 != 0)
```

```text
In memory, missing = NaN. where(cond) keeps values where cond holds, else NaN:
  full field:  600 values, dtype float64
  gappy field: every 5th day masked -> whole-day gaps at days 5, 10, 15, 20, 25, 30
  NaN only exists for float dtypes; masking an int array silently casts it to float
```

Measuring:

```text
isnull/notnull give boolean masks; count() tallies the non-missing values:
  gappy.isnull().sum()  = 120 missing (6 days x 4 y x 5 x)
  gappy.notnull().sum() = 480 present
  gappy.count(dim='time') -> every pixel has 24 of 30 days
```

`count(dim="time")` is the coverage map — the thing `0302` argued you should
always compute alongside an aggregate.

The `skipna` contrast again, now with structural gaps:

```text
Reductions skip NaN by default (skipna=True for float data):
  gappy.mean()             = 26.73  (mean of the 480 present values)
  gappy.mean(skipna=False) = nan  (one NaN poisons the whole thing)
  per-pixel mean with skipna=False -> 20 of 20 pixels NaN
```

All twenty pixels, because the gaps are whole days: every pixel is missing on
day 5.

Two repair strategies, compared on the same gap:

```python
series = gappy.isel(y=0, x=0)
filled = series.fillna(series.mean())
interp = series.interpolate_na(dim="time")
```

```text
Repairing gaps: fillna substitutes a constant, interpolate_na uses neighbors in time:
  around the day-5 gap (days 4..6):
    original       :  27.81,    nan,  28.66
    fillna(mean)   :  27.81,  28.63,  28.66
    interpolate_na :  27.81,  28.23,  28.66
  interpolate_na left 1 NaN: day 30 has no later neighbor to
  interpolate toward -- edge gaps need fill_value='extrapolate' or a fillna pass
```

`interpolate_na` produced `28.23`, the midpoint of `27.81` and `28.66`.
`fillna(mean)` produced `28.63`, the series mean, which happens to be close here
and would not be in a gap during a cold snap. And the trailing gap on day 30
survives interpolation, because there is no later neighbour to interpolate
toward.

The storage boundary:

```python
gappy.to_netcdf(path, encoding={"t2m": {"_FillValue": -9999.0}})
decoded = xr.load_dataset(path)
raw = xr.load_dataset(path, mask_and_scale=False)
```

```text
On disk there is no NaN convention -- a sentinel _FillValue stands in for missing:
  written with encoding={'t2m': {'_FillValue': -9999.0}}
  raw file value at a gap (mask_and_scale=False): -9999.0
  decoded value at the same gap:                  nan
  decoded t2m.encoding['_FillValue'] = -9999.0
  decoded t2m.attrs                  = {'units': 'degC', 'long_name': '2 metre temperature'}
  decoding moved _FillValue out of the data and into .encoding; the values became NaN
```

That is the clearest possible demonstration of the encoding/attrs split. The
same file read two ways: `mask_and_scale=False` gives the literal `-9999.0` on
disk; the default gives `nan` in memory plus a record of the sentinel in
`encoding`, and `attrs` is untouched by any of it.

**Why it matters.** Missing data in climate feeds is not exceptional, it is
constant — sensor dropouts, late arrivals, ocean pixels outside a land mask,
satellite swath gaps. The in-memory representation is NaN and the on-disk
representation is a sentinel, and xarray translates between them automatically
as long as `encoding` is right.

The choice between repair strategies is a domain decision with no default
answer. Interpolation is defensible for a smooth variable over a short gap and
indefensible for rainfall, where interpolating between two dry days across a
storm invents the wrong number confidently. Leaving NaN and reporting coverage
is usually more honest than filling.

The edge-gap behaviour is worth remembering: `interpolate_na` fills interior
gaps only. A gap at either end needs `fill_value="extrapolate"`, a `bfill`/`ffill`
pass, or a decision to leave it missing. Since the newest period of a store is
exactly where late-arriving data leaves gaps, this is the common case, not the
rare one.

**Traps.**

- **NaN is float-only.** Masking an int array upcasts silently. There is no
  integer NaN, so an integer variable with missing data needs a sentinel and
  explicit handling.
- **`_FillValue` belongs in `encoding`, not `attrs`.** Putting it in `attrs`
  writes a literal attribute that decoders may ignore.
- **`mask_and_scale=False` also disables `scale_factor`/`add_offset` decoding.**
  If a file uses packed integers, raw mode gives you the integers.
- **`interpolate_na` needs a numeric index along the dim** and interpolates in
  index space by default, so uneven time spacing is handled correctly only with
  `use_coordinate=True` (the default for a datetime coord).
- **`count()` counts non-NaN, `sum()` on a boolean counts `True`.** Different
  questions.
- **A `sum` over an all-NaN window returns `0.0`.** Use `min_count=1` to get NaN
  instead. This is the single most dangerous default in the missing-data area,
  because zero rainfall and unknown rainfall are very different claims.
---

## Pitfalls and gotchas

Seven behaviours that surprise people, collected in one place. Every one of them
fails **quietly** — a plausible-looking result rather than an exception — which
is exactly why they are worth memorising rather than looking up.

### 1. Attrs are inert

xarray never reads `attrs`. Not `units`, not `standard_name`, not anything.

```python
celsius + kelvin        # degC + K
```

```text
celsius mean          =   26.30 [degC]
kelvin mean           =  299.45 [K]
celsius + kelvin mean =  325.74 -- nonsense, computed without complaint
```

Three consequences:

- Unit conversion must be a deliberate step, and the `units` attr must be
  rewritten by hand at the same time.
- Any operation that changes semantics — `var()`, a ratio, a normalised index —
  leaves a correct array with a lying `units`.
- Conflicting attrs on binary operations are **dropped**, not reconciled, so
  `units` can vanish entirely without a word.

**Defence:** normalise units once at ingest, rewrite the attrs there, and treat
the convention as enforced by code and tests rather than by the metadata. Use
`pint-xarray` if you need real enforcement.

### 2. Alignment silently shrinks results

Every binary operation aligns indexes first, with an **inner** join.

```text
  (late.t2m - early.t2m).sizes = {'time': 7, 'y': 20, 'x': 30}
  21 days minus 17 days -> 7 days: only labels present in BOTH survive
```

Twenty-one days meets seventeen days and produces seven. No error, no warning.
If the two sources have drifted apart entirely, the result has zero time steps
and is still a perfectly valid object.

Float coordinates make it worse: two grids computed by different code paths can
differ in the last bit and share no labels at all.

**Defence:**

```python
with xr.set_options(arithmetic_join="exact"):
    diff = late - early
```

```text
AlignmentError: cannot align objects with join='exact' where index/labels/sizes
are not equal along these coordinates (dimensions): 'time' ('time',)
```

Or assert on the indexes before the operation. In a multi-source pipeline, one
of the two belongs at every point where two feeds meet.

### 3. groupby and resample fragment dask chunks

This one is invisible until it hits storage or performance, and it is severe.

```python
ds = temperature_dataset(days=365, ny=64, nx=64).chunk({"time": 30, "y": 64, "x": 64})
clim = ds.groupby("time.month").mean()
anom = ds.groupby("time.month") - clim
monthly = ds.resample(time="MS").mean()
```

```text
source chunks:                      {'time': (30, 30, ..., 5), 'y': (64,), 'x': (64,)}   # 13 time chunks
groupby('time.month').mean() chunks: {'month': (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ...}
resample('MS').mean() chunks:        {'time': (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ...}
anomaly chunks:                      {'time': (1, 1, 1, ... 365 of them ...), ...}
```

(Chunk tuples trimmed for readability; the real output prints every element.)

Thirteen chunks in, **365 chunks out** for the anomaly. Each group or bucket
becomes its own chunk. This is legal for zarr — all the chunks are the same size
— so `to_zarr` accepts it and writes 365 tiny files where 13 were intended.

**Defence:** rechunk explicitly after any groupby or resample, immediately
before the write.

```python
anom.chunk({"time": 30, "y": 64, "x": 64}).to_zarr(store, consolidated=False)
```

```text
after rechunk: {'time': (30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 5), ...}
```

Install `flox` as well: xarray uses it automatically for groupby reductions and
it changes the algorithm rather than just the layout.

### 4. Non-uniform chunks versus zarr

Zarr stores **one** chunk shape per array; only the final chunk along a
dimension may be smaller. Dask stores a full list of block sizes and has no such
rule. There are therefore dask layouts with no zarr representation, and two
completely ordinary operations produce them.

```text
  north tile (40 rows, y chunked 32): y chunks (32, 8)
  south tile (24 rows, y chunked 32): y chunks (24,)
  concat along y:                     y chunks (32, 8, 24)  <- NON-UNIFORM interior
  north-up flip isel(y=::-1):         y chunks (24, 8, 32)
```

```text
ValueError: Zarr requires uniform chunk sizes except for final chunk. Variable
named 't2m' has incompatible dask chunks: ((30, 30), (24, 8, 32), (64,)).
Consider rechunking using `chunk()`.
```

Concatenating differently-sized tiles, and reversing an axis to get north-up,
are the two most ordinary operations in geospatial ingestion. Nothing goes wrong
until the write, which can be many steps later.

**Defence:** an explicit `.chunk({...})` immediately before every `to_zarr`,
matching the store's encoding. OCS keeps a `_uniform_chunks` helper for exactly
this. Do not rely on chunking chosen earlier in the pipeline; the next `concat`
will destroy it.

### 5. Calendars

Model output on `noleap` or `360_day` calendars decodes to `cftime` objects, not
`datetime64`. The two do not mix, and the failure is silence:

```python
result = standard_datetime64_array + cftime_array
```

```text
result sizes: {'time': 0, 'y': 20, 'x': 30}
```

The printed dates look identical. The indexes share zero labels. The inner join
is empty and nothing raises.

Related, smaller edges:

- `datetime64[us]` (pandas 3 default) versus `datetime64[ns]` (what
  `convert_calendar` returns).
- `.dt.season` is meteorological: `DJF` spans a year boundary.
- `.dt.dayofyear` shifts by one after February in leap years, which corrupts
  day-of-year climatologies built across mixed years.
- Frequency aliases changed: `"M"` -> `"ME"`, `"Y"` -> `"YE"`, `"H"` -> `"h"`.

**Defence:** `convert_calendar("standard")` at ingest, so every store speaks one
calendar. Same argument as unit normalisation, same reason.

### 6. NaN versus `_FillValue`

In memory, missing is `NaN`. On disk, it is a sentinel recorded in `encoding`.
xarray translates automatically:

```text
  raw file value at a gap (mask_and_scale=False): -9999.0
  decoded value at the same gap:                  nan
  decoded t2m.encoding['_FillValue'] = -9999.0
  decoded t2m.attrs                  = {'units': 'degC', 'long_name': '2 metre temperature'}
```

The sharp edges:

- **NaN is float-only.** `where` on an integer array upcasts to `float64`
  silently. So does `reindex`, and so does an outer `align`.
- **`_FillValue` in `attrs` instead of `encoding`** writes a literal attribute
  that decoders may ignore, leaving sentinel values in your data.
- **`sum` over an all-NaN window returns `0.0`, not NaN.** An entirely missing
  month reports zero rainfall. `min_count=1` fixes it, and this is the most
  dangerous default in the area.
- **`count()` counts non-NaN; `mask.sum()` counts `True`.** Different questions.
- **`interpolate_na` fills interior gaps only.** The trailing edge — which is
  exactly where a store's newest, late-arriving period lives — stays NaN unless
  you ask for extrapolation.

**Defence:** always compute a coverage map beside an aggregate, and mask the
aggregate where coverage is too thin.

```python
monthly = daily.resample(time="1ME").mean()
coverage = daily.notnull().resample(time="1ME").sum()
expected = daily.notnull().resample(time="1ME").count()
monthly = monthly.where(coverage / expected >= 0.8)
```

### 7. `keep_attrs` and other version-sensitive defaults

```text
  celsius.mean().attrs = {'units': 'degC', 'long_name': ..., 'standard_name': ...}
    (this xarray keeps attrs by default; older releases dropped them on reductions)
  with xr.set_options(keep_attrs=False): celsius.mean().attrs = {}
```

The default has flipped across versions. Library code that depends on the answer
must set it:

```python
with xr.set_options(keep_attrs=True):
    result = pipeline(ds)
```

Other defaults in the same family, all worth stating explicitly rather than
inheriting:

| Setting | Why it matters |
|---|---|
| `keep_attrs` | metadata survival through operations |
| `arithmetic_join` | `"inner"` by default; `"exact"` catches drift |
| `use_flox` | changes groupby performance dramatically |
| `display_max_rows` | repr only, but reprs are your debugging tool |

### And two smaller ones

**`isel` slices are end-exclusive; `sel` slices are inclusive on both ends.**
Both are defensible, they are opposite, and mixing them produces off-by-one
errors that pass review because the code looks right.

**`ds.dims` is not `ds.sizes`.** On a `Dataset`, `dims` is a
`FrozenMappingWarningOnValuesAccess` whose `.values()` is deprecated; on a
`DataArray`, `dims` is a plain tuple of names. Use `ds.sizes` whenever you want
name-to-length.

---

## How this maps to open-climate-service

Each phase of this project was chosen because it explains a specific mechanism
in OCS. Collected concretely:

### Normalisation to `(time, y, x)`

Every OCS store is an `xr.Dataset` whose dimensions have been normalised to
`(time, y, x)`, whatever the source called them. That normalisation is the first
act of ingestion and it involves, in order:

1. `open_dataset` on the incoming netCDF or GRIB (phase 5,
   [`0501_netcdf.py`](../../xarray/examples/0501_netcdf.py)).
2. Renaming dimensions and variables to the canonical names.
3. Flipping latitude to north-up if the source is south-up — the `isel(y=::-1)`
   that seeds the chunk trap in
   [`0603_rechunking.py`](../../xarray/examples/0603_rechunking.py).
4. Converting units and rewriting the `units` attr, once, because attrs are
   inert ([`0701_cf_attrs_units.py`](../../xarray/examples/0701_cf_attrs_units.py)).
5. Ensuring a `datetime64` time coordinate on the standard calendar
   ([`0702_time_handling.py`](../../xarray/examples/0702_time_handling.py)).

After that step, every downstream process can assume one shape, one unit
system, one calendar. The synthetic helpers in
`src/ocs_stack_xarray/synthetic.py` produce data that is already in that state,
which is why every example in this project can start from `(time, y, x)` without
ceremony.

### `coarsen` as pyramid downsampling

GeoZarr multiscale pyramids are built by repeated mean-downsampling, and that is
exactly `coarsen`:

```python
level1 = level0.coarsen(y=2, x=2).mean()
level2 = level1.coarsen(y=2, x=2).mean()
```

```text
  level 1: shape=(10, 8, 16)  (16x32 -> 8x16, 4x fewer pixels)
  level 2: shape=(10, 4, 8)  (8x16 -> 4x8, 16x fewer than native)
  means preserved across levels: 26.2296 -> 26.2296 -> 26.2296
```

There is no pyramid library involved. The coordinates coarsen along with the
data — new cell centres are block means of the old ones — which is what keeps
overlaid levels geometrically aligned. Each level is written as its own array in
the store, and a client picks a level by zoom.

The `boundary=` policy matters here: grid sizes that do not divide by 2 need
`"trim"` or `"pad"`, and the two produce different extents. Pick one and be
consistent across levels.

### `concat` as the append-a-period pattern

Ingestion is a loop: a period arrives, it is normalised, it is appended.

```python
store = xr.concat([store, period], dim="time")
idx = store.indexes["time"]
assert idx.is_monotonic_increasing and idx.is_unique
```

`concat` never checks for overlap, and neither does zarr's `append_dim`. A
re-sent period produces duplicate timestamps that concat accepts silently and
that break downstream code far from the cause:

```text
  concat happily produces 98 days -- no error, no warning
  duplicated labels:    7
  selecting one duplicated day returns 2 entries -- downstream code breaks here
```

The robust form is to trim the incoming period to timestamps strictly after the
store's maximum, which makes a duplicate delivery an idempotent no-op rather
than something to repair later.

### zarr encoding, `append_dim`, and `region`

The three storage mechanics, in the order they appear in a store's life.

**Chunk encoding is chosen once, at the first write, and everything follows from
it.** OCS uses roughly one ingestion period per time chunk, spatial dims capped:

```text
  chunks=  (30, 64, 64)  ->   4 chunk files   (OCS-style: ~30 time steps, spatial capped)
```

versus 120 files for one-file-per-day and 16 for full-history spatial tiles. The
choice is tuned to two access patterns — "map for one month" and "series at one
place" — and to making appends cheap.

**`append_dim="time"` extends the store**, and because the period length matches
the time chunk, it adds whole new files without touching existing ones:

```text
Step 2 -- next period arrives; to_zarr(append_dim='time') extends the store:
  time axis:   60 days: 2024-01-01 .. 2024-02-29
  chunk files: 2 (append added exactly one new chunk file)
  the period length matches the time chunk, so old chunk files were never touched
```

**`region={"time": slice(...)}` rewrites a slice in place** for corrections and
reprocessing, leaving the shape and chunk grid untouched:

```text
  mean shift vs original -- day 9: +0.00, day 15: +5.00, day 25: +0.00
  time axis unchanged: 60 days: 2024-01-01 .. 2024-02-29
```

Two practical notes. Region payloads must drop coordinates that do not span the
region dimension (`drop_vars(["y", "x"])`), and region slices are **positional**,
computed from the store's time index rather than from dates.

And the gap this leaves is the reason the next project exists: neither write
mode is transactional. A crashed append leaves the store partially updated with
no record of what landed. `icechunk` closes that gap.

### Serving over plain HTTP

The reason OCS can serve stores from a static file server, with no query
protocol and no server-side compute, is the store layout from
[`0502_zarr_basics.py`](../../xarray/examples/0502_zarr_basics.py): a client
fetches an 810-byte `zarr.json`, computes which chunk indices it needs, and
fetches those paths directly.

```text
  t2m/c/0/0/0  exists=True
  t2m/c/1/0/0  exists=True
  fetching days 0-4 touches only t2m/c/0/0/0 -- one HTTP GET against a static file server
```

That is the entire cloud-native argument, and it is why chunk shape is
simultaneously a storage decision, a compute decision, and an API design
decision.

---

## Where to go next

- **[dask](dask.md)** — the execution layer underneath phase 6. Task graphs,
  blocked algorithms, schedulers, and the chunk-sizing measurements this project
  only gestures at. Its phase 5 revisits dask-backed xarray from the other side.
- **[The stack](../stack.md)** — how xarray, dask, dask-distributed, icechunk,
  and the capstone fit together, and where each layer stops.
- **[Storage](../storage.md)** — local filesystem versus object storage, and the
  specific property (conditional writes on a branch pointer) that decides which
  you need.
- **[API reference](../reference/xarray.md)** — the generated reference for this
  project's shared helper module.

If you are working through the repository in order, `dask` is the next project:
it takes the lazy behaviour from phase 6 and explains the machinery, including
the chunk-sizing rule of thumb and the same zarr uniform-chunk problem measured
from the dask side.

---

## Further reading

**xarray**

- User guide and full documentation: <https://docs.xarray.dev/>
- Tutorial, notebook-based and runnable: <https://tutorial.xarray.dev/>
- API reference: <https://docs.xarray.dev/en/stable/api.html>
- Terminology, if the vocabulary is the sticking point:
  <https://docs.xarray.dev/en/stable/user-guide/terminology.html>
- Working with dask: <https://docs.xarray.dev/en/stable/user-guide/dask.html>
- Reading and writing files:
  <https://docs.xarray.dev/en/stable/user-guide/io.html>

**Storage formats**

- Zarr specifications, including the v3 core spec:
  <https://zarr-specs.readthedocs.io/>
- Zarr Python documentation: <https://zarr.readthedocs.io/>
- netCDF documentation and the netCDF-C library:
  <https://docs.unidata.ucar.edu/netcdf-c/current/>
- netCDF4-python: <https://unidata.github.io/netcdf4-python/>

**Conventions**

- CF conventions, the metadata standard everything in climate assumes:
  <https://cfconventions.org/>
- CF standard name table, the controlled vocabulary for `standard_name`:
  <https://cfconventions.org/standard-names.html>
- GeoZarr specification, the emerging convention for geospatial zarr:
  <https://github.com/zarr-developers/geozarr-spec>

**Ecosystem worth knowing about**

- `flox` — fast groupby reductions for xarray:
  <https://flox.readthedocs.io/>
- `pint-xarray` — real unit enforcement, the answer to inert attrs:
  <https://pint-xarray.readthedocs.io/>
- `cftime` — non-standard calendars: <https://unidata.github.io/cftime/>
- `icechunk` — transactional versioned zarr, the next layer up:
  <https://icechunk.io/>
- Pangeo, the community where most of this stack comes from:
  <https://pangeo.io/>
