# climate-pipeline

**The capstone.** A miniature climate service: a deliberately messy source
normalized into one set of conventions, ingested into a versioned icechunk
store one period per commit, derived into climatological normals and climate
indices, and published with GeoZarr root attributes and a STAC collection
whose extents are read back off the store rather than declared.

Every other project in this repository studies one library in isolation.
`xarray` teaches the labeled data model, `dask` teaches lazy chunked
computation, `icechunk` teaches transactional versioned storage. This project
is where the three stop being separate subjects. It is modelled on
[open-climate-service](https://github.com/dhis2/open-climate-service) (OCS),
the DHIS2 climate data platform: each OCS instance is scoped to one country,
ingests from real sources like CHIRPS and ERA5, stores results as GeoZarr
inside icechunk, and exposes them through STAC, Zarr over HTTP, and openEO.
This project keeps the shape and throws away the scale.

```text
source  ->  normalize  ->  icechunk store  ->  indices  ->  publish
(K, lat/lon,   (degC,        (one commit      (hot days,   (GeoZarr attrs,
 south-up)      time/y/x)     per period)      SPI, ...)    STAC collection)
```

The unusual thing about this project, compared with the other four, is where
the lesson lives. In `xarray` and `dask` the examples carry the teaching and
there is barely a library to speak of. Here it is the reverse: the library in
`src/ocs_stack_climate_pipeline/` is a small but complete service, and the
examples are guided tours through it. Five modules, roughly 900 lines
including docstrings, and every one of them exists because a real pipeline
needs it:

| Module | Role |
|---|---|
| `sources.py` | Synthetic sources in deliberately awkward conventions, and period enumeration |
| `normalize.py` | Source conventions to canonical `(time, y, x)`, degC and mm, north-up |
| `ingest.py` | Streaming per-period append into icechunk, with resume |
| `indices.py` | Climatology, anomalies, hot and wet days, an SPI-like index, pyramid levels |
| `publish.py` | GeoZarr root attributes and the STAC collection document |

Ten examples in four phases run over that library. The last one,
`0401_full_pipeline`, runs the whole thing in about a second on a laptop and
prints six stages of evidence. If you read only one thing here, read
[the end-to-end walkthrough](#the-pipeline-end-to-end).

---

## Introduction: what a climate data service actually does

This section is long on purpose. The code in this project is not difficult;
almost every function is under twenty lines. What makes it worth studying is
the set of problems it was written against, and those problems are domain
problems, not programming problems. If you already know what a climatological
normal is, why rainfall is summed and temperature averaged, and what GeoZarr
adds to a Zarr store, skip to [Setup](#setup). Otherwise this is the section
that makes the rest of the page make sense.

### Raw climate data is not usable as delivered

Start with the thing nobody tells you: a climate dataset downloaded from an
authoritative source is not ready to use, and the reasons are almost never
scientific. The science is fine. The data is fine. What is not fine is that
every producer made a different set of defensible local choices, and you are
now holding four datasets that disagree with each other about everything
except the numbers.

Here is what actually differs, in roughly the order you will trip over it.

**Dimension names.** The spatial axes of a gridded climate dataset might be
called `lat` and `lon`, or `latitude` and `longitude`, or `y` and `x`, or --
if the grid is projected -- `northing` and `easting`. ERA5 as distributed by
Copernicus uses `latitude`/`longitude`. Many regional products use `lat`/`lon`.
Anything that has been through a GDAL-based tool tends to come out as `y`/`x`.
The time axis is usually `time`, but `valid_time` appears in ERA5 downloads and
`t` in some model output.

This sounds trivial and it is not, because it makes generic code impossible.
You cannot write `ds.mean("y")` in a service that serves six sources when three
of them call that axis something else. You either write a dispatch table at
every call site, which rots, or you rename once at the door, which is what
`normalize.py` does.

**Units.** Temperature arrives in Kelvin far more often than in Celsius,
because Kelvin is the SI unit and reanalysis output is machine output.
Precipitation arrives in metres in ERA5, in millimetres in most gauge-based
products, and in `kg m-2 s-1` (a rate, not a depth) in raw climate model
output. Wind arrives as `u` and `v` components rather than speed and
direction. None of these is wrong. All of them are surprising to a user who
asked for "rainfall" and got `0.0047`.

The unit problem has a nasty second half, which the first example in this
project spends real time on: in xarray, **attributes are inert**. The `units`
attribute is a string in a dictionary. Nothing in xarray reads it, validates
it, or updates it. Subtract 273.15 from a Kelvin array and you get a Celsius
array whose `units` attribute still says `"K"`. Nothing raises. Nothing warns.
The dataset is now wrong in a way that no type checker, no test of the numeric
kind, and no schema validator will catch, and the error surfaces months later
when somebody plots it.

**Axis direction.** Latitude can ascend or descend along its axis. If `lat[0]`
is the southernmost row, the grid is "south-up"; if `lat[0]` is the
northernmost, it is "north-up". Both are common. Both are correct as data.
But every raster rendering convention on Earth -- GeoTIFF, the GDAL affine
transform, web map tiles, and GeoZarr -- assumes row 0 is the top of the image,
which is to say the north edge. Hand a south-up array to a tile renderer
without flipping it and the map comes out upside down. Sierra Leone renders as
a reflection of Sierra Leone, which is obvious the moment somebody looks at it
and completely invisible until then.

**Calendars.** This one does not appear in this project, because the synthetic
sources use ordinary proleptic Gregorian time, but it is the reason
`cftime` exists and it deserves a mention. Climate *models* frequently do not
use the real calendar. A `noleap` (or `365_day`) calendar drops February 29
entirely. A `360_day` calendar gives every month thirty days, which makes the
arithmetic beautiful and the dates fictional. Mixing a `360_day` model run with
a real-calendar observational record requires an explicit decision about what
to do with the five and a quarter days per year that do not exist in one of
them. numpy's `datetime64` cannot represent a `360_day` calendar at all, which
is why xarray falls back to `cftime` objects and why some operations get slower
when it does.

**Missing data.** A gridded product covering a country has cells outside the
country, cells over water where a land-only product has nothing to say, and
cells where the satellite retrieval failed. These are encoded as NaN
sometimes, and as a sentinel `_FillValue` such as `-9999` other times. Sum a
dataset whose ocean is `-9999` without masking and you will get a total that
is confidently, catastrophically wrong.

**Chunking and file layout.** A netCDF file per day for forty years is 14,600
files. Opening that as one logical dataset means 14,600 file handles and
14,600 metadata reads before a single number comes back. That is the problem
ARCO formats exist to solve, which is the next subject.

### CF conventions: the agreement about metadata

The response to most of the above is the **Climate and Forecast (CF) metadata
conventions**, at <https://cfconventions.org/>. CF is not a file format; it is
an agreement about what attributes mean, layered on top of netCDF and,
increasingly, Zarr.

The parts that matter for a service like this one:

- `units` is a string parseable by UDUNITS: `"K"`, `"degC"`, `"mm"`,
  `"kg m-2 s-1"`. Because it is machine-parseable, tools like `cf-units` and
  `pint-xarray` can convert automatically rather than guessing.
- `standard_name` comes from a controlled vocabulary. `air_temperature` means
  air temperature everywhere, whatever the variable in the file happens to be
  called. A variable named `t2m` and one named `TMP_2maboveground` can both
  carry `standard_name: air_temperature`, and a tool that keys on the standard
  name works with both.
- `long_name` is free text for humans. `"2 metre temperature"`.
- `axis` marks a coordinate as `T`, `X`, `Y`, or `Z`, which is a machine-readable
  way of saying "this is the time axis" regardless of what it is named.
- `calendar` on the time coordinate says which of the calendars above is in use.
- `_FillValue` and `missing_value` mark the sentinel.
- `cell_methods` records how a value was reduced: `time: mean` for a daily
  mean, `time: sum` for an accumulation. This is exactly the intensive/extensive
  distinction discussed below, written into metadata.

This project writes `units` and `long_name` and stops there, which is a
simplification worth naming. A production service should write
`standard_name` and `cell_methods` too, because those are what make a dataset
self-describing to a tool that has never seen it. `xarray` project example
`0701_cf_attrs_units` covers the fuller picture.

The important cultural point about CF is that it is a *convention*, not a
constraint. Nothing enforces it. A file can claim `units: "K"` and hold
Celsius, and no reader will object. This is the same inertness described
above, at the level of the whole ecosystem: metadata is documentation that
happens to be machine-readable, not a contract that is checked.

### ARCO: analysis-ready, cloud-optimized

"ARCO" is the term the community settled on for data that has been prepared so
that analysis can start immediately and can run against object storage without
downloading everything first. It unpacks into two independent claims.

**Analysis-ready** means the normalization has already happened. Units are
consistent. Dimension names are consistent. The axis directions are consistent.
Missing data is encoded one way. The time axis is a single monotonic series
rather than a directory of files you have to concatenate yourself. Everything
described in the previous two sections has been dealt with once, by whoever
built the archive, instead of separately by every one of its users.

The economics here are the whole argument. If a hundred analysts each download
the same forty years of daily rainfall and each spend a day writing the same
unit conversion and the same latitude flip, that is a hundred days of work and
a hundred opportunities to get it subtly wrong -- and they will not all get it
wrong the same way, so their results will not be comparable. Normalizing once
at ingest costs one day and produces one answer.

**Cloud-optimized** means the storage layout supports partial reads over HTTP
without a server that understands the format. This is what Zarr provides: an
array is stored as many independent chunk objects plus a small JSON metadata
document, so a client that wants one month out of forty years issues a handful
of ranged GETs against exactly the chunks it needs. No index server, no
database, no download of the whole thing. The same property makes Zarr work
well for parallel writes, because separate workers can write separate chunk
objects without coordinating -- with one important caveat about chunk
boundaries that this project runs into head-first and which is covered under
[design decisions](#why-align_chunkstrue-is-mandatory).

Contrast this with the pre-ARCO way, which is still extremely common: a
directory of netCDF files, one per day or month, each a self-contained
container with its own header. Opening the collection means opening every
file. Reading a time series at one point means reading every file. The format
is fine; the layout is the problem, and it is a layout that made sense when
data lived on a shared filesystem next to the compute.

Chunk shape is where the two halves of ARCO meet, and it is a genuine
trade-off with no universally right answer. A dataset chunked
`(time=all, y=small, x=small)` serves time series at a point beautifully and
serves maps terribly. Chunked `(time=1, y=all, x=all)` it is the reverse. This
project chunks `(time=30, y<=512, x<=512)`: about a month of time per chunk,
spatial chunks capped so that a map tile is one or two reads. That choice is
tuned for a service that mostly serves maps and monthly aggregates, which is
what OCS mostly serves.

### The shape: ingest, store, derive, publish

Every climate data service, at every scale, has the same four stages. Naming
them is most of the value of this project.

**Ingest** pulls data from an upstream source and normalizes it. The key
structural decision, and the one this project takes most seriously, is that
ingest is *streaming and periodic*, not monolithic. The source does not hand
over a dataset; it enumerates the periods it can supply, and the framework
fetches them one at a time. A period is typically a month. Each period is
fetched, normalized, written, and committed on its own.

That granularity is not an implementation detail. Ingesting forty years of
daily data over a flaky network, from a source with rate limits, on a machine
that might get redeployed, is a job that *will* be interrupted. Making the
period the unit of work means an interruption costs at most one period, and
resuming means asking the store what it already holds. There is no job
database, no manifest, no progress file -- because anything that can disagree
with the data eventually will.

**Store** persists the normalized data in a format that supports appending,
versioning, and partial reads. This project uses icechunk over Zarr, as OCS
does. icechunk adds transactional commits on top of Zarr: a write is not
visible until `commit()` succeeds, and every commit is a snapshot you can read
back later. The combination of the two -- Zarr's chunk-level partial reads and
icechunk's atomic commits -- is what makes "append a month, atomically, to a
forty-year array, without a database" a sentence that means something.

**Derive** computes the products people actually want out of the stored series.
Nobody asks for the daily 2-metre temperature field. They ask how many hot days
a district got, or whether last season was unusually dry, or when the rains are
expected to start. Those are *indices*: usually small reductions over the
stored series, cheap to compute, and the actual deliverable. In OCS these are
exposed as openEO processes; here they are plain functions in `indices.py` so
that the arithmetic is visible.

**Publish** turns a store into a product. This is the stage people skip and
regret. A store full of correct numbers that nobody can find, place on a map,
or interpret the units of is not a product. Publishing means writing GeoZarr
root attributes so a renderer knows where on Earth the grid sits, and writing a
STAC collection so a catalogue can index it. Both are covered below.

The four stages compose in one direction and share one invariant: **everything
downstream trusts the normalization done at ingest.** That is why normalization
is the first module and the first example.

### Who consumes the output

It helps to be concrete about the audience, because it explains several
decisions that otherwise look arbitrary.

**A map viewer** in a browser wants tiles. It has a canvas of a few hundred
pixels and a zoom level, and it needs the field at roughly that resolution,
fast. It absolutely does not want to download a 4096-wide grid to paint a
512-pixel panel. This is why pyramids exist and why example `0203_pyramid`
spends its time on the byte counts.

**An analyst** wants a time series at a point, or a regional mean, computed
over the full record. They want to open the store lazily from Python and let
xarray and dask work out which chunks to read. This is why the store is Zarr
and why the chunking is what it is.

**A downstream model** -- a malaria transmission model, a crop yield model, an
early-warning system -- wants a specific derived quantity as input, usually an
anomaly or an index, on a specific grid, over a specific period. It wants it
to be *the same quantity* it was trained on, which means units and reduction
methods must be stable across releases. This is why the intensive/extensive
distinction below matters enough to have its own section.

**A catalogue** -- a STAC browser, another instance federating several -- wants
metadata it can index without touching the data. Extents, variables, units,
value ranges, and a link. This is why the STAC collection exists and why its
extents are computed from the store rather than typed in.

**A health or planning ministry**, in the OCS case, is the ultimate audience,
and they want a number they can act on. "Seventeen hot days last month, against
a normal of nine." Every layer above exists to make that sentence trustworthy.

### The domain vocabulary

Five terms come up constantly and are worth defining precisely, because the
code uses them as function names.

#### Climatological normal

A **normal** is the long-run average of a quantity for a particular part of the
year at a particular place. "The normal for March here is 29.9 degrees." It
answers the question *what is typical*.

The arithmetic is a grouped mean: take every day in the record, group by
calendar month, average within each group. Twelve numbers per grid cell. In
this project that is one line:

```python
def climatological_normal(ds: xr.Dataset, variable: str = "t2m") -> xr.DataArray:
    return _require(ds, variable).groupby("time.month").mean()
```

The subtlety is not the arithmetic, it is the *record length*. The World
Meteorological Organization convention is a **thirty-year normal**, updated
every decade -- 1961-1990, 1971-2000, 1981-2010, 1991-2020. Thirty years is
long enough that one freak month does not define "normal" and short enough
that a changing climate has not made the early years irrelevant. That
convention is why you will see "1991-2020 normals" on the axis of practically
every published anomaly chart.

This project computes normals over one to four years, because it is a teaching
project running on synthetic data in under a second. Example `0201_climatology`
says so out loud in its own output rather than pretending otherwise. The
arithmetic is identical; only the length of the record differs, and the
difference is entirely about how much you should trust the answer.

#### Anomaly

An **anomaly** is a departure from the normal. If March's normal is 29.9 and
this March was 31.4, the anomaly is +1.5 degrees. The arithmetic is subtraction,
grouped the same way as the normal:

```python
values = _require(ds, variable)
normal = values.groupby("time.month").mean()
anomaly = values.groupby("time.month") - normal
```

Anomalies are the single most-published class of climate product, and the
reason is comparability. Consider what an absolute temperature does and does
not tell you:

- 29 degrees Celsius in Freetown is an ordinary afternoon. 29 degrees in Oslo
  is a heatwave that makes the news. The absolute number carries no information
  about whether it is unusual *there*.
- A warm January and a warm July are both interesting, but the July reading is
  larger in absolute terms simply because July is warmer. Comparing them
  directly compares seasons, not weather.
- The seasonal cycle is by far the loudest signal in a raw temperature series
  and also by far the least interesting -- everybody knows summer is warmer.
  Subtracting it is how anything else becomes visible, including trend.

There is an arithmetic property worth internalizing, which example
`0201_climatology` checks explicitly: **anomalies centre on zero by
construction**, both within each month and overall. Subtracting each group's
own mean from that group forces the group mean to zero. It is a free
consistency check on your implementation. If your anomaly field does not
average to something within floating-point noise of zero, you have grouped
wrong.

The real run produces this, which is as close to exactly zero as
double-precision arithmetic over 366 x 12 x 12 values gets:

```text
  grand mean of the anomaly field: -1.93e-16 degC  (zero, to floating point)
```

#### Climate index

An **index** is a derived quantity constructed to answer a question a
non-specialist actually asks. The distinguishing feature is that it usually
involves a *threshold and a count* rather than an average.

- **Hot days**: how many days in the month exceeded some temperature. Heat
  stress is not about the mean; a month that sat at 31 every day and a month
  that alternated 25 and 37 can share a mean and be completely different to
  live through. The count is sensitive to the shape of the distribution; the
  mean is not.
- **Wet days**: how many days had at least 1 mm of rain. Two hundred millimetres
  over twenty days is a growing season; two hundred millimetres over three days
  is a flood. Same total, different index, entirely different consequence.
- **Standardized Precipitation Index (SPI)**: monthly rainfall expressed in
  standard deviations from that calendar month's own long-run distribution.
  Dimensionless, so the same threshold means the same thing in a rainforest and
  a savanna, which is why drought monitoring uses it everywhere.

The threshold values are conventions with reasons behind them. The 1 mm wet-day
cutoff exists because below 1 mm a reading is not distinguishable from dew,
gauge wetting, or a satellite retrieval's noise floor. Count sub-millimetre
days and "rainy days per year" starts measuring instrument sensitivity rather
than climate -- and stops being comparable between stations, which defeats the
entire purpose of having an index.

The real, standardized index catalogue lives in **xclim**
(<https://xclim.readthedocs.io/>), which implements hundreds of indices from
ETCCDI, ICCLIM, and the agricultural and health literature, with proper unit
handling via `pint`, calendar handling via `cftime`, and dask-aware
implementations throughout. **In production you should use xclim, not
hand-rolled index functions.** This project implements five by hand for exactly
one reason: so the arithmetic is on the page rather than behind an API. Once
you have read `indices.py` and understood what `resample(time="1ME").sum()`
does to a threshold mask, reading xclim's source is easy, and using it is
obviously the right call.

#### Intensive versus extensive

This is the distinction that causes the most silent errors in climate data
work, and it is why `indices.py` has both `monthly_total` and
`climatological_normal` doing structurally identical things with different
reductions.

A quantity is **intensive** if it has a value at every instant and averaging
two of them yields a meaningful quantity of the same kind. Temperature is
intensive. The mean of Monday's 28 degrees and Tuesday's 30 degrees is 29
degrees, and 29 degrees is a temperature.

A quantity is **extensive** if it accumulates, so that the meaningful
aggregate over a period is the sum. Rainfall is extensive. The rain that fell
in August is the sum of the rain that fell on each August day. A reservoir
fills from the total, not from the daily rate.

The trap is that both reductions are always defined, always run without error,
and always produce a plausible-looking float carrying a plausible-looking unit
attribute. Example `0202_indices` demonstrates both failure directions
explicitly. Summing temperature over a month:

```text
  Temperature is INTENSIVE -- summing it gives 862 'degC' for the month,
  a quantity with no physical meaning. But it is a float, it carries a unit
  attribute, it plots, and nothing in xarray or zarr will ever object. That
  is what makes the intensive/extensive mix-up a silent error, not a crash.
```

And averaging rainfall:

```text
  Rainfall is EXTENSIVE -- it accumulates. 2024-Aug received 307 mm; its
  daily mean of 9.90 mm/day is a true number that answers no question
  anyone asked, and it shrinks if you lengthen the month while the rain that
  fell stays the same.
```

That last clause is the sharpest way to see it. Take a month's rain and split
the month into two halves. The *sum* over the two halves adds back to the
month's total. The *mean* over the two halves does not reconstruct anything,
and worse, a mean over a 28-day February and a mean over a 31-day March are
comparable in a way that hides the fact that March had three more days to rain
in. The extensive aggregate carries the period length; the intensive one
deliberately does not.

CF has a place to record which one you did -- `cell_methods: "time: sum"` versus
`cell_methods: "time: mean"`. Writing it is cheap insurance. This project does
not, which is one of its acknowledged simplifications.

#### Pixel registration

The last piece of vocabulary is geometric. A grid cell has a centre and it has
edges. A coordinate array in a climate dataset almost always holds **cell
centres**: `y[0]` is the latitude of the middle of the first row.

An affine transform, on the other hand, describes the mapping from array index
to ground coordinate, and by convention its origin is the **outer edge** of the
first cell -- half a cell away from the first centre. This is called *pixel
registration* (as opposed to *grid registration*, where the origin is the
centre).

Get it wrong and every rendered tile shifts by half a pixel. On a country-scale
grid with cells a few kilometres across, half a pixel is a kilometre or two:
small enough that nobody notices in review, large enough to put a rendered
value over the wrong clinic. Example `0301_geozarr` checks the offset
numerically against the coordinate arrays rather than trusting it.

### GeoZarr: putting the grid on Earth

A Zarr store is an array of numbers in chunks plus a small metadata document.
Nothing in it says which coordinate reference system the numbers are in, which
way is up, or where cell `[0, 0]` sits. The coordinate arrays hold degrees,
which helps a Python client, but a tile server rendering a chunk is not going
to read and interpolate a coordinate array to work out geography -- it wants
one affine transform and one CRS, declared once, at the root.

**GeoZarr** is the specification for those root attributes:
<https://github.com/zarr-developers/geozarr-spec>. It is deliberately small.
The keys this project writes are:

```python
{
    "spatial:transform": [step_x, 0.0, origin_x, 0.0, step_y, origin_y],
    "spatial:dimensions": ["y", "x"],
    "spatial:shape": [ny, nx],
    "spatial:bbox": [west, south, east, north],
    "proj:code": "EPSG:4326",
    "zarr_conventions": [{"name": "geozarr", "version": "0.4"}],
}
```

Two of these carry traps large enough that the project has an example about
each.

The **transform** is six numbers in the order
`[stepX, rotX, originX, rotY, stepY, originY]`. The origin values are outer
edges, per the pixel registration discussion above. `stepY` is *negative* for a
north-up grid, because walking down rows walks south. A positive `stepY` is how
a store ends up rendering the country upside down.

**`spatial:dimensions` is in array order, y first.** A client reads it
positionally: the first entry names the slow axis of the buffer. Name them
x-first and every raster read from the store is transposed. On a square grid
the wrong shape still fits, so the bug renders silently -- which is why
example `0301_geozarr` uses a deliberately non-square 12 x 16 grid.

GeoZarr also specifies a **multiscale** convention: sibling groups named `0`,
`1`, `2` holding the same field at halving resolutions, plus a `multiscales`
root attribute naming the levels and the factor between them. That is the
pyramid a map viewer needs. This project computes the pyramid arrays
(`pyramid_levels`) but does not write them into the store as groups, which is
one of its deliberate omissions -- see
[how this maps to OCS](#how-this-maps-to-open-climate-service).

### STAC: making the dataset discoverable

Storing data is not publishing it. **STAC**, the SpatioTemporal Asset Catalog
specification (<https://stacspec.org/>), is how a client finds out what an
instance holds without knowing anything about its internals.

The STAC data model has three levels. An **Item** is one asset with one
footprint and one timestamp -- a single satellite scene, typically. A
**Collection** groups Items and describes their shared extent and properties.
A **Catalog** is a tree of Collections. For a gridded time series like this
one, the natural unit is a Collection: one document per dataset, describing the
whole thing.

A STAC Collection is a JSON document with a fixed shape. The parts that carry
weight here:

- `id` -- the primary key of the entire service. In OCS it names the store on
  disk *and* appears in the URL `/stac/collections/{id}`. A client that has seen
  the id once can come back to it forever.
- `extent.spatial.bbox` -- a *list* of boxes, because a collection may be
  scattered across several footprints. A single-grid dataset still nests its box
  one level deep, which is a small surprise the first time.
- `extent.temporal.interval` -- likewise a list of `[start, end]` pairs in ISO
  8601.
- `summaries` -- per-variable units, long names, and value ranges. Units are the
  half of the contract that the data alone cannot carry: 27.4 is a plausible
  temperature in degC and an absurd one in K. The min and max let a client build
  a colour ramp, or notice a source has gone wrong, before downloading a single
  chunk.
- `assets` -- where the bytes actually are. This is the one field that points
  back at the store.

The design decision this project takes seriously is that **every extent is
derived from the store, never declared**. `bounding_box(ds)` reads the
coordinate arrays. `temporal_extent(ds)` reads the committed time coordinate.
Neither takes an argument saying what the extent ought to be. That means
re-publishing after the next month lands moves the end date automatically, and
it makes the catalogue structurally incapable of drifting from the data. It
also means the STAC bbox and the GeoZarr bbox are the same numbers by
construction, so a catalogue search and a rendered tile agree about the
footprint. Example `0302_stac` asserts that equality rather than assuming it.

### What this project is not

Worth stating plainly before you read the code.

The sources are **synthetic**. `sources.py` generates plausible West African
temperature and rainfall with numpy, seeded deterministically. There is no
network, no authentication, no rate limiting, no CDS API, no CHIRPS FTP. The
messiness is real messiness -- Kelvin, `lat`/`lon`, south-up, metres -- but it
is messiness that was put there on purpose so that normalization has something
to do.

The indices are **hand-rolled**. Use [xclim](https://xclim.readthedocs.io/) for
anything real.

There is **no HTTP layer**. OCS serves STAC over HTTP, Zarr over HTTP, and
openEO process graphs. This project builds the documents and prints them.

There is **no openEO**. OCS exposes its derivations as openEO processes, which
is what makes them composable and remotely executable. Here they are Python
functions.

The scale is **tiny**: 12 x 12 to 16 x 16 grids, one to four years, a handful
of megabytes. Every example runs in under five seconds. That is a feature for
learning and a limitation for extrapolating -- the chunk-alignment problem is
real at any scale, but the performance characteristics of a 16 x 16 grid tell
you nothing about a 4000 x 4000 one.

---

## Setup

The project follows the same template as everything else in this repository:
Python 3.13, `uv` with the `uv_build` backend, src layout, a `Makefile` with
the standard targets.

```bash
cd climate-pipeline
make install
```

`make install` is `uv sync`. On a warm cache it takes a couple of seconds; on a
cold one it resolves and downloads xarray, dask, zarr, icechunk, netCDF4 and
scipy, which is a few hundred megabytes.

Run one example:

```bash
make run EXAMPLE=0401_full_pipeline
```

Run all ten, in order:

```bash
make run-all
```

Run the tests:

```bash
make test
```

That gives, on the run behind this page:

```text
>>> Running tests
.......................                                                  [100%]
23 passed in 0.79s
```

Twenty-three tests in `tests/test_pipeline.py`, grouped into `TestNormalize`,
`TestIngest`, `TestIndices`, `TestPublish`, and `TestEndToEnd`. They are worth
reading alongside the library, because several of them pin down behaviour that
the examples only demonstrate -- that the pyramid halves cleanly from 6 to 3 to
1, that a one-cell grid raises rather than producing a degenerate transform,
that resume skips exactly the periods already committed.

The full lint pass is `make lint`, which runs `ruff format`, `ruff check --fix`,
then **both** mypy and pyright in strict mode. `make ci` is lint plus test.

If you would rather not go through make, every example is an ordinary script:

```bash
cd climate-pipeline
uv run python examples/0301_geozarr.py
```

Timings for the ten examples on the machine that produced this page, an Apple
Silicon laptop -- **machine-dependent, quoted only for a sense of scale**:

| Example | Wall time |
|---|---|
| `0101_normalize` | 0.50s |
| `0102_streaming_ingest` | 0.67s |
| `0103_resume` | 0.85s |
| `0201_climatology` | 2.23s |
| `0202_indices` | 4.20s |
| `0203_pyramid` | 0.67s |
| `0301_geozarr` | 0.66s |
| `0302_stac` | 0.72s |
| `0401_full_pipeline` | 0.92s |
| `0402_second_dataset` | 1.35s |

The two slow ones are slow for the same reason: `0201` ingests twelve monthly
periods and `0202` ingests forty-eight of them into two separate stores. That
is 48 icechunk commits, and a commit is not free even on a local filesystem.
It is a useful calibration -- the compute here is negligible and the
transaction overhead is everything, which is exactly the profile a real ingest
has too.

Everything writes into a `tempfile.TemporaryDirectory()` and cleans up after
itself. Nothing in this project leaves a store behind.

---

## The library, module by module

This is the heart of the page. Unlike the other projects here, where the
library is a thin bag of helpers and the examples carry the teaching, in
`climate-pipeline` the library *is* the lesson. Read these five modules and you
have read a climate service.

The public surface is re-exported from the package root, so every example
imports from `ocs_stack_climate_pipeline` directly:

```python
from ocs_stack_climate_pipeline import (
    Period,
    bounding_box,
    climatological_normal,
    committed_periods,
    enumerate_periods,
    fetch_precipitation,
    fetch_temperature,
    geozarr_attrs,
    grid_transform,
    hot_days,
    ingest,
    ingest_period,
    monthly_anomaly,
    monthly_total,
    normalize,
    pyramid_levels,
    spi_like,
    stac_collection,
    store_path,
    temporal_extent,
    wet_days,
)
```

Twenty-one names. That is the whole service.

### `sources.py` — deliberately messy inputs

Source: [`../../climate-pipeline/src/ocs_stack_climate_pipeline/sources.py`](../../climate-pipeline/src/ocs_stack_climate_pipeline/sources.py)

**Responsibility.** Stand in for real upstream sources. Two things: enumerate
the periods a source can supply, and produce one period's worth of data on
demand, in conventions that are awkward on purpose.

The module docstring says it plainly:

```python
"""Synthetic data sources, deliberately messy.

Each source stands in for a real one and arrives in its own conventions, so
the normalization step has something to actually do. Periods are enumerated
the way open-climate-service's streaming ingest does it: the plugin lists the
periods it can supply, and the framework fetches them one at a time.
"""
```

#### The extent

```python
# A Sierra Leone-ish extent, matching the country-scoped instances OCS deploys.
BBOX = (-13.5, 6.9, -10.3, 10.0)
```

That is `(west, south, east, north)` in degrees on WGS 84, covering roughly
Sierra Leone. It is not arbitrary: OCS deploys one instance per country, and
a country-scale extent is what makes the grid sizes here (a few hundred to a
few thousand cells) realistic in shape even though they are tiny in count.

Note that this is the *centre* extent -- the coordinate arrays run from -13.5
to -10.3 inclusive, so the outer edges of the raster reach half a cell further
in each direction. That half-cell shows up again in `publish.py`.

#### `Period`

```python
@dataclass(frozen=True)
class Period:
    """One ingestable period.

    Attributes:
        period_id: Stable identifier, such as ``"2024-01"``.
        start: First day of the period, as an ISO date string.
        days: Number of daily steps in the period.
    """

    period_id: str
    start: str
    days: int
```

Three fields, frozen, hashable. The `period_id` is the load-bearing one: it is
the key that `committed_periods()` reconstructs from the store's time
coordinate, so resume works by comparing period ids. Making it a stable string
like `"2024-01"` rather than an index or a timestamp is what allows that
comparison to be a set operation.

`days` is separate from `start` because months are not a fixed size, which is
the point the next function makes.

#### `enumerate_periods`

```python
def enumerate_periods(year: int = 2024, months: int = 12) -> list[Period]:
    if not 1 <= months <= 12:
        raise ValueError(f"months must be between 1 and 12, got {months}")
    periods: list[Period] = []
    for month in range(1, months + 1):
        start = pd.Timestamp(year=year, month=month, day=1)
        periods.append(
            Period(
                period_id=f"{year}-{month:02d}",
                start=start.strftime("%Y-%m-%d"),
                days=int(start.days_in_month),
            )
        )
    return periods
```

The interesting line is `int(start.days_in_month)`, which delegates the
calendar to pandas rather than hand-rolling a leap-year rule. Run it over two
years:

```python
>>> [p.days for p in enumerate_periods(2024, 12)]
[31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
>>> [p.days for p in enumerate_periods(2023, 12)]
[31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
```

2024 is a leap year and February has 29 days; 2023 has 28. Those two numbers
are the direct cause of the chunk-alignment problem covered under
[design decisions](#why-align_chunkstrue-is-mandatory): the store's time chunk
is a fixed 30 days, and no month is 30 days except April, June, September and
November. Every other append lands on a partial chunk.

This is also the OCS ingestion contract in miniature. A source plugin does not
return data; it returns a list of what it *could* return. The framework decides
what to actually fetch, in what order, and how many times to retry -- which is
exactly the separation that lets `ingest()` implement resume without the source
knowing anything about it.

#### `_period_seed` — the determinism trap

This is a five-line private function that deserves a full section, because the
bug it avoids is one of the most confusing in Python.

```python
def _period_seed(period: Period) -> int:
    """Derive a stable per-period seed from its id.

    Python randomizes string hashing per process, so ``hash(period_id)`` would
    give different data on every run. crc32 is stable across processes and
    machines, which is what reproducible examples need.
    """
    return zlib.crc32(period.period_id.encode()) % 10_000
```

Since Python 3.3, string hashing is salted with a per-process random seed, as a
defence against hash-collision denial-of-service attacks. `hash("2024-01")`
returns a different value in every interpreter you start. Three consecutive
runs on the machine behind this page:

```text
hash("2024-01") = 6147987242643249052  crc32 = 6962
hash("2024-01") =  814182303813762383  crc32 = 6962
hash("2024-01") = 7707634371377113861  crc32 = 6962
```

`zlib.crc32` is a fixed checksum with no salt. It gives the same answer in
every process, on every machine, forever.

Why this matters here: the seed derived from the period id feeds
`np.random.default_rng`, which generates the synthetic data. Use `hash()` and
January 2024 holds different temperatures every time you run an example. The
documented outputs stop matching, tests that assert on values fail
intermittently, and -- worst of all -- ingesting the same period twice produces
*different data*, which quietly destroys the idempotence that the whole resume
story depends on.

The general lesson generalizes well past this project: **`hash()` is not a
checksum.** If you need a stable identifier derived from a string, use
`zlib.crc32`, `hashlib.md5`, `hashlib.sha256`, or anything else with a defined
output -- never the built-in `hash()`. The failure mode is especially nasty
because it is invisible within a single process; everything is consistent right
up until you restart.

#### `_grid`

```python
def _grid(ny: int, nx: int, ascending_y: bool) -> tuple[np.ndarray, np.ndarray]:
    west, south, east, north = BBOX
    y = np.linspace(south, north, ny) if ascending_y else np.linspace(north, south, ny)
    x = np.linspace(west, east, nx)
    return y, x
```

`np.linspace` is inclusive of both endpoints, so a grid built this way has its
first and last coordinates exactly on the bbox corners. Those are cell centres.
The `ascending_y` switch is how the "south-up" messiness is injected: both
source functions call it with `ascending_y=True`, so the raw data always needs
flipping.

#### `fetch_temperature`

```python
def fetch_temperature(period: Period, ny: int = 24, nx: int = 24, seed: int = 0) -> xr.Dataset:
    """Fetch one period of temperature, in a deliberately awkward source format.

    This source publishes Kelvin on a south-up ``lat``/``lon`` grid -- exactly
    the kind of thing normalization exists to fix.
    """
    rng = np.random.default_rng(seed + _period_seed(period))
    time = pd.date_range(period.start, periods=period.days, freq="D")
    lat, lon = _grid(ny, nx, ascending_y=True)

    day_of_year = time.dayofyear.to_numpy().reshape(-1, 1, 1)
    seasonal = 3.0 * np.sin(2 * np.pi * day_of_year / 365.25)
    gradient = np.linspace(-1.5, 1.5, ny).reshape(1, ny, 1)
    kelvin = 273.15 + 27.0 + seasonal + gradient + rng.normal(0.0, 0.7, size=(period.days, ny, nx))

    return xr.DataArray(
        kelvin,
        dims=("time", "lat", "lon"),
        coords={"time": time, "lat": lat, "lon": lon},
        name="t2m",
        attrs={"units": "K", "long_name": "2 metre temperature"},
    ).to_dataset()
```

The signal has three components layered on a 27-degree base: a seasonal
sinusoid with a 3-degree amplitude keyed to day of year, a 3-degree
north-south gradient, and Gaussian noise with a 0.7-degree standard deviation.
That is a deliberately shallow annual cycle, which is realistic for a tropical
grid and shows up in the climatology example as an annual range of 5.72 degrees.

The broadcasting is worth a second look because it is idiomatic numpy that
xarray users often reach for xarray to avoid. `day_of_year` is reshaped to
`(days, 1, 1)` and `gradient` to `(1, ny, 1)`; adding them to a `(days, ny, nx)`
noise array broadcasts each along the axes it does not vary over. Doing the same
thing with xarray would be more readable and slower, and since this is
generating a source rather than analysing one, raw numpy is the right call.

Everything wrong with this dataset is wrong on purpose:

- dims are `(time, lat, lon)`, not `(time, y, x)`
- `units` is `"K"`
- `lat` ascends, so row 0 is the southernmost

Reading one back raw:

```text
<xarray.Dataset> Size: 4kB
Dimensions:  (time: 31, lat: 4, lon: 4)
Coordinates:
  * time     (time) datetime64[us] 248B 2024-01-01 2024-01-02 ... 2024-01-31
  * lat      (lat) float64 32B 6.9 7.933 8.967 10.0
  * lon      (lon) float64 32B -13.5 -12.43 -11.37 -10.3
Data variables:
    t2m      (time, lat, lon) float64 4kB 298.1 298.5 298.3 ... 302.2 303.1
```

`lat` runs 6.9 to 10.0 -- ascending, south first. The values start near 298 K.

#### `fetch_precipitation`

```python
def fetch_precipitation(period: Period, ny: int = 24, nx: int = 24, seed: int = 1) -> xr.Dataset:
    rng = np.random.default_rng(seed + _period_seed(period))
    time = pd.date_range(period.start, periods=period.days, freq="D")
    lat, lon = _grid(ny, nx, ascending_y=True)

    # West African rainfall peaks mid-year; zero-inflated, like the real thing.
    month = time.month.to_numpy().reshape(-1, 1, 1)
    wetness = np.clip(np.sin((month - 2) / 12 * np.pi), 0.05, None)
    wet = rng.random((period.days, ny, nx)) < wetness
    amounts_mm = rng.gamma(2.0, 5.0, size=(period.days, ny, nx))
    metres = np.where(wet, amounts_mm, 0.0) / 1000.0

    return xr.DataArray(
        metres,
        dims=("time", "lat", "lon"),
        coords={"time": time, "lat": lat, "lon": lon},
        name="tp",
        attrs={"units": "m", "long_name": "total precipitation"},
    ).to_dataset()
```

The rainfall generator is a better model than the temperature one, and the
reason is that rainfall has a fundamentally different statistical shape.

It is generated as a **two-stage process**, which is how real precipitation is
usually modelled. First an occurrence: does it rain at all today, at this cell?
That is a Bernoulli draw against a seasonal probability `wetness`, which peaks
mid-year and is floored at 0.05 so the dry season is dry but not impossible.
Then an amount, but only where it rained: a gamma draw with shape 2 and scale
5, giving a right-skewed distribution with a mean around 10 mm and a long tail.

The result is **zero-inflated**: a large fraction of cell-days are exactly zero,
and the non-zero values are skewed. Example `0402_second_dataset` measures it:

```text
  Note the shape of the data: 95% of January's cell-days are exactly zero,
  against 3% of July's. Rainfall is zero-inflated and strongly seasonal;
  temperature is neither, and that difference is what makes the derived
  products further down diverge.
```

That difference in shape is the reason rainfall gets different derived products
from temperature. A mean over a zero-inflated distribution is dominated by the
zeros and tells you almost nothing; a *count* of days over a threshold tells you
a great deal. It is also the reason real SPI fits a gamma distribution before
standardizing rather than assuming normality, which `spi_like` acknowledges and
does not do.

The unit is metres, which is what ERA5 publishes and what nobody thinks in.
`0.0588` m of rain in a day is a real number and an unreadable one; 58.8 mm is
a heavy day's rain. Same value, 1000x apart.

---

### `normalize.py` — one convention, bought once

Source: [`../../climate-pipeline/src/ocs_stack_climate_pipeline/normalize.py`](../../climate-pipeline/src/ocs_stack_climate_pipeline/normalize.py)

**Responsibility.** Take whatever a source produced and return the service's
canonical form: dims `(time, y, x)`, a north-up y axis, degC and mm, a sorted
and duplicate-free time axis.

This is the module that buys uniformity, and the reason it is worth having at
all is stated in its docstring:

```python
"""Normalizing source data into the service's own conventions.

Real sources disagree about everything: dimension names (``lat``/``lon`` vs
``latitude``/``longitude``), units (Kelvin vs Celsius, m vs mm), and axis
direction (south-up vs north-up). open-climate-service resolves all of that at
ingest so that every stored dataset looks identical to everything downstream,
which is what makes its API uniform across sources.
"""
```

The design principle is: **normalize once, at the door, and never ask again.**
The alternative -- handling conventions at each point of use -- is the design
that produces a service where half the endpoints work for half the datasets.

#### The alias tables

```python
Y_ALIASES = ("y", "lat", "latitude", "Latitude", "LAT")
X_ALIASES = ("x", "lon", "longitude", "Longitude", "LON")
TIME_ALIASES = ("time", "t", "valid_time", "date")

CANONICAL_UNITS = {"t2m": "degC", "tp": "mm"}
```

Plain tuples, ordered so the canonical name is checked first. The capitalized
variants are there because they genuinely appear in the wild -- some
GDAL-derived and some ArcGIS-derived products capitalize. `valid_time` is what
recent ERA5 downloads from the Copernicus CDS call their time axis, which
catches people out because older downloads called it `time`.

This is a lookup table, not a heuristic. It is easy to reach for something
cleverer -- guess by which coordinate looks like latitudes, or read the CF
`axis` attribute. A table is better here for two reasons: it fails loudly on
input it does not recognize, and adding a source means adding a string, which
is a change anyone can review.

#### `rename_dims`

```python
def rename_dims(ds: xr.Dataset) -> xr.Dataset:
    """Rename whatever the source calls its axes to ``(time, y, x)``.

    Raises:
        ValueError: If a spatial or time axis cannot be identified.
    """
    mapping: dict[str, str] = {}
    for aliases, target in ((TIME_ALIASES, "time"), (Y_ALIASES, "y"), (X_ALIASES, "x")):
        found = next((name for name in aliases if name in ds.dims or name in ds.coords), None)
        if found is None:
            raise ValueError(f"no dimension matching {target!r} among {tuple(ds.dims)}")
        if found != target:
            mapping[found] = target
    return ds.rename(mapping) if mapping else ds
```

Three details worth pointing at.

The membership test is `name in ds.dims or name in ds.coords`. Checking both
matters because a coordinate is not always a dimension -- a dataset can carry a
scalar `time` coordinate with no `time` dimension, or 2-D curvilinear
`lat`/`lon` coordinates over `y`/`x` dims.

The `if mapping else ds` guard means an already-normalized dataset passes
through untouched rather than going through a no-op rename. That makes
`normalize()` idempotent, which the duplicate-handling below depends on.

And it **raises rather than guessing**. Feed it something unrecognizable:

```python
>>> rename_dims(xr.DataArray(np.zeros((2, 2)), dims=("foo", "bar")).rename("v").to_dataset())
ValueError: no dimension matching 'time' among ('foo', 'bar')
```

The error names both what was wanted and what was available, which is the
difference between a two-minute fix and a twenty-minute one. There is a test
for exactly this in `TestNormalize.test_rejects_unrecognized_dims`.

#### `orient_north_up`

```python
def orient_north_up(ds: xr.Dataset) -> xr.Dataset:
    """Ensure the y axis descends, so row 0 is the northernmost.

    GeoZarr places a raster with an affine whose y step is negative for a
    north-up grid. A south-up source silently renders upside down, so the
    orientation is fixed here rather than trusted.
    """
    if "y" not in ds.coords or ds.sizes.get("y", 0) < 2:
        return ds
    if float(ds["y"][0]) < float(ds["y"][-1]):
        return ds.isel(y=slice(None, None, -1))
    return ds
```

Four lines of logic, and the whole point is in the docstring: the failure this
prevents is **silent**. Nothing errors when you render a south-up array with a
north-up transform. You get a picture. The picture is a vertical mirror of the
truth, and whether anyone notices depends on whether the country happens to be
vertically symmetric, which Sierra Leone is not but plenty of grid cells are.

The `< 2` guard handles degenerate grids -- a single-row selection has no
direction to speak of, and `y[0] < y[-1]` on a length-1 array compares an
element with itself.

The flip is `isel(y=slice(None, None, -1))`, which is xarray's way of writing
`[::-1]` along a named dimension. This is a *lazy* operation on a dask-backed
array: it rewrites the graph, not the bytes. On a numpy-backed array it produces
a reversed view where it can.

Note that this function fixes the orientation and does *nothing* to the values.
The data was always correct; only the row order was surprising. Reversing the
rows and reversing the coordinate together keeps every value attached to its
own latitude.

#### `convert_units`

```python
def convert_units(ds: xr.Dataset) -> xr.Dataset:
    """Convert known variables to the service's canonical units.

    Kelvin becomes Celsius and metres of precipitation become millimetres.
    Attributes are inert in xarray, so the ``units`` attribute is rewritten by
    hand -- forgetting that is how a dataset ends up labelled ``K`` while
    holding Celsius.
    """
    out = ds.copy()
    for name, var in ds.data_vars.items():
        units = str(var.attrs.get("units", "")).strip()
        if units in ("K", "kelvin", "Kelvin"):
            out[name] = var - 273.15
            out[name].attrs = {**var.attrs, "units": "degC"}
        elif units in ("m", "metre", "meters") and name == "tp":
            out[name] = var * 1000.0
            out[name].attrs = {**var.attrs, "units": "mm"}
        else:
            out[name].attrs = dict(var.attrs)
    return out
```

The most important line in this module is `out[name].attrs = {**var.attrs,
"units": "degC"}`, and the reason is the assignment on the line above it.

**Attributes in xarray are inert. Nothing reads them, and nothing updates
them.** `var - 273.15` changes every number in the array and does not touch the
`units` string. Nothing raises. Nothing warns. Example `0101_normalize`
demonstrates it side by side:

```text
  demo: (raw - 273.15).attrs = {'units': 'K', 'long_name': '2 metre temperature'}  <- still says K
        normalize()  .attrs = {'units': 'degC', 'long_name': '2 metre temperature'}
```

The left-hand line is a dataset that is now in Celsius and is labelled Kelvin.
It will serialize, it will plot, it will pass every schema check, and it is
wrong. The error surfaces months later in somebody's chart.

It is worth being precise about *when* attributes survive, because the rules
are subtle enough that memorizing them is a mistake. On xarray 2026.7.0:

```python
>>> a = xr.DataArray(np.ones((2, 2)), dims=("y", "x"), attrs={"units": "K", "long_name": "t"})
>>> b = xr.DataArray(np.ones((2, 2)), dims=("y", "x"), attrs={"units": "m"})
>>> dict((a - 273.15).attrs)     # scalar arithmetic: attrs survive intact
{'units': 'K', 'long_name': 't'}
>>> dict((a - b).attrs)          # binary op on two arrays: only common attrs survive
{'long_name': 't'}
>>> dict(a.mean().attrs)         # reduction: attrs survive intact
{'units': 'K', 'long_name': 't'}
```

Three operations, three different outcomes, and none of them is *correct* in
the sense of producing metadata that describes the result. Scalar arithmetic
keeps a units string that the arithmetic just invalidated. A binary op silently
drops the units because the two operands disagreed. A reduction keeps units
that are right for a mean and wrong for a count. There is also a global option,
`xr.set_options(keep_attrs=...)`, which changes the defaults and therefore
changes which of these bugs you get.

The only safe assumption is that **you must set the attributes yourself,
explicitly, every time you change what a variable means.** `convert_units` does
both edits -- the values and the units string -- in the same three lines, so
they cannot drift apart. The same discipline shows up in `indices.py`, where
`hot_days` sets `units = "days"` outright because carrying `"degC"` through
onto a count would be worse than useless.

The `and name == "tp"` guard on the metres branch is a small piece of
defensiveness worth noticing. `"m"` is a legitimate unit for lots of things --
geopotential height, snow depth, sea level -- and multiplying all of them by
1000 because they are in metres would be a disaster. The conversion is scoped
to the variable it was written for. That is a scaling limit of this approach:
a real service either keys conversions on `standard_name` or uses a real unit
library like `pint-xarray` that understands dimensional analysis.

The `.strip()` is there because units strings in the wild have trailing
whitespace surprisingly often.

#### `sort_time`

```python
def sort_time(ds: xr.Dataset) -> xr.Dataset:
    """Sort along time and drop duplicate timestamps, keeping the last.

    A re-fetched period arrives with timestamps the store may already hold.
    Appending it blindly produces a store with duplicate coordinates that
    cannot be indexed sanely, so duplicates are resolved here.
    """
    if "time" not in ds.dims:
        return ds
    ds = ds.sortby("time")
    index = ds.indexes["time"]
    if index.has_duplicates:
        keep = ~index.duplicated(keep="last")
        ds = ds.isel(time=np.flatnonzero(keep))
    return ds
```

Two jobs: sort, then deduplicate.

The sort exists because sources do not always return their periods in order.
A directory listing, a paginated API, a `glob` -- any of them can produce
timestamps out of sequence, and an unsorted time axis breaks `sel` with a
slice, breaks `resample`, and breaks any monotonicity assumption downstream.

The deduplication has a specific policy: **keep the last**. `index.duplicated(keep="last")`
marks all but the final occurrence of each timestamp; the negation keeps the
final one. The consequence is that a re-fetched period acts as a *correction* --
if a source republishes a day with revised values, the revised copy wins over
the one already held. That is the right default for climate data, where
reanalysis products are routinely revised and the later version is the better
one.

`np.flatnonzero(keep)` converts the boolean mask into integer positions for
`isel`. Using positions rather than a boolean mask avoids a subtlety in how
xarray handles boolean indexing along a dimension that also has an index.

Example `0101_normalize` exercises this by concatenating a period with itself:

```text
  concat of a period with itself: time=62 steps
  unique timestamps?             : False
  after normalize()              : time=31 steps
  unique timestamps?             : True
```

#### `normalize`

```python
def normalize(ds: xr.Dataset) -> xr.Dataset:
    """Run the full normalization pipeline on a source dataset.

    Returns:
        A dataset with dims ``(time, y, x)``, a north-up y axis, canonical
        units, and a sorted, duplicate-free time axis.
    """
    ds = rename_dims(ds)
    ds = orient_north_up(ds)
    ds = convert_units(ds)
    ds = sort_time(ds)
    return ds.transpose("time", "y", "x", ...)
```

Four steps and a transpose, in an order that is not arbitrary.

`rename_dims` must be first, because everything after it refers to `y` and
`time` by their canonical names. `orient_north_up` comes next because it is a
pure reordering. `convert_units` and `sort_time` are independent of each other
and could be swapped.

The final `transpose("time", "y", "x", ...)` fixes the *memory layout*, not
just the labels. xarray does not care about dimension order -- `ds.mean("y")`
works regardless -- but Zarr does, because the chunk shape and the on-disk
byte order follow the dimension order. Putting `time` first means a chunk holds
a contiguous run of days for a spatial tile, which is what appending along time
wants. The trailing `...` is xarray's "and any remaining dimensions in their
existing order", which makes the call safe for datasets that carry an extra
dimension such as `level` or `ensemble`.

Round-tripping the raw dataset from earlier:

```text
Dimensions:  (time: 31, y: 4, x: 4)
Coordinates:
  * time     (time) datetime64[us] 248B 2024-01-01 2024-01-02 ... 2024-01-31
  * y        (y) float64 32B 10.0 8.967 7.933 6.9
  * x        (x) float64 32B -13.5 -12.43 -11.37 -10.3
Data variables:
    t2m      (time, y, x) float64 4kB 27.62 28.42 28.14 ... 27.23 26.92 27.84
```

`lat` became `y` and now runs 10.0 down to 6.9 -- north first. Values went from
298-ish to 27-ish. That is the whole job.

Aliases work too:

```python
>>> alt = raw.rename({"lat": "latitude", "lon": "longitude", "time": "valid_time"})
>>> tuple(normalize(alt)["t2m"].dims)
('time', 'y', 'x')
```

---

### `ingest.py` — one period, one commit

Source: [`../../climate-pipeline/src/ocs_stack_climate_pipeline/ingest.py`](../../climate-pipeline/src/ocs_stack_climate_pipeline/ingest.py)

**Responsibility.** Move periods from a source into an icechunk store, one
transaction each, skipping what is already there and surviving what fails.

This is the module that most directly reproduces an OCS design decision, and
its docstring is the thesis of the whole project:

```python
"""Streaming ingest: one period at a time, committed as it lands.

This is the heart of the open-climate-service ingestion contract. The source
enumerates periods; the framework fetches each one, normalizes it, appends it
to the store, and commits. Because every period is its own transaction, an
interrupted ingest leaves a store that is complete up to the last commit --
never half a period -- and resuming is a matter of asking the store what it
already holds.
"""
```

#### Chunking constants

```python
# The chunk sizes a country-scale daily dataset wants: about a month of time
# per chunk, and spatial chunks capped so a map tile is one or two reads.
TIME_CHUNK = 30
SPATIAL_CHUNK_CAP = 512
```

Thirty days per time chunk, spatial dimensions capped at 512. These are the
two numbers that determine the read and write performance of the whole store,
and both are chosen for the workload OCS has.

Thirty days along time means a monthly aggregate reads roughly one chunk, and
appending a month writes roughly one chunk. It is also, deliberately, *not*
28, 29, or 31 -- which means it never lines up exactly with a month, which is
what forces the alignment problem into the open rather than letting it hide
until some later month.

512 on the spatial axes is a cap rather than a size. Small grids stay whole;
large grids get tiled. 512 x 512 float64 is 2 MB per chunk before compression,
which is in the range everybody recommends -- large enough that per-chunk
overhead is amortized, small enough that a client reading one tile does not pull
tens of megabytes.

#### `Fetcher`

```python
Fetcher = Callable[[Period], xr.Dataset]
```

The entire source plugin interface: a callable from `Period` to `Dataset`.
Nothing else. No base class, no registration, no lifecycle.

This is why every example can pass a lambda:

```python
ingest(repo, periods, lambda p: fetch_temperature(p, ny=16, nx=16))
```

and why `0103_resume` can simulate an outage with a plain function:

```python
def flaky_temperature(period: Period) -> xr.Dataset:
    if period.period_id == "2024-03":
        raise RuntimeError("source unavailable: upstream returned HTTP 503")
    return small_temperature(period)
```

A one-function protocol is worth defending. Everything a source needs to do --
authenticate, page, retry, cache, decompress -- happens inside that callable and
is invisible to the framework. Everything the framework does -- ordering,
resume, error collection, transaction boundaries -- happens outside it and is
invisible to the source. Neither can break the other.

#### `IngestReport`

```python
@dataclass
class IngestReport:
    """What one ingest run did.

    Attributes:
        ingested: Period ids written during this run.
        skipped: Period ids already present and therefore skipped.
        failed: Period ids that raised, with the error message.
        snapshots: Snapshot id produced by each ingested period.
    """

    ingested: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    failed: dict[str, str] = field(default_factory=dict)
    snapshots: dict[str, str] = field(default_factory=dict)

    @property
    def total(self) -> int:
        """Number of periods considered in this run."""
        return len(self.ingested) + len(self.skipped) + len(self.failed)
```

Three categories, and the three-way split is the point. A run that reports "12
periods processed" is nearly useless. A run that reports "9 ingested, 2 skipped,
1 failed with `RuntimeError: source unavailable`" tells an operator whether to
retry, whether anything is missing, and what to look at.

`snapshots` maps period id to icechunk snapshot id, which is a provenance
record for free: you can point at the exact commit in which any given month
landed.

`total` is a derived property rather than a counter, so it cannot disagree with
the lists.

#### `chunking_for`

```python
def chunking_for(ds: xr.Dataset) -> dict[str, int]:
    """Choose chunk sizes for a dataset, capping the spatial dimensions."""
    chunks = {"time": min(TIME_CHUNK, int(ds.sizes.get("time", 1)))}
    for dim in ("y", "x"):
        if dim in ds.sizes:
            chunks[dim] = min(SPATIAL_CHUNK_CAP, int(ds.sizes[dim]))
    return chunks
```

The `min` calls are the whole function. A chunk larger than the array is
allowed by dask but produces a confusing repr and, for Zarr, wastes space in
the final partial chunk. Capping at the actual size keeps things honest.

```python
>>> chunking_for(normalize(fetch_temperature(p, ny=1024, nx=1024)))
{'time': 30, 'y': 512, 'x': 512}
>>> chunking_for(normalize(fetch_temperature(p, ny=16, nx=16)))
{'time': 30, 'y': 16, 'x': 16}
```

A 1024-wide grid gets tiled into 512-wide chunks; a 16-wide grid stays whole.

#### `committed_periods`

```python
def committed_periods(repo: Any, *, period_type: str = "month") -> set[str]:
    """Return the period ids already committed to a store.

    The store's time coordinate is authoritative: whatever is committed is
    what exists, regardless of what any external bookkeeping claims. That is
    what makes resume safe after a crash.
    """
    if period_type != "month":
        raise ValueError(f"unsupported period_type: {period_type!r}")
    try:
        ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    except Exception:
        # A repository with no committed data yet has no group to open.
        return set()
    if "time" not in ds.coords or ds.sizes.get("time", 0) == 0:
        return set()
    stamps = pd.DatetimeIndex(ds["time"].values)
    return {f"{ts.year}-{ts.month:02d}" for ts in stamps}
```

This is the function that makes resume work, and its design is the single
most transferable idea in the project: **the store is the source of truth.**

Look at what it does *not* do. It does not read a manifest. It does not query a
job database. It does not consult a progress file, a lock file, or a Redis key.
It opens the store, reads the time coordinate, and derives the set of period
ids from the timestamps that are actually present.

The reason this matters is that anything else can disagree with the data. A
manifest written after a successful commit is fine until the process dies
between the commit and the manifest write. A manifest written before is fine
until the commit fails. A job database on another machine is fine until the
network partitions. Every one of those is a real failure mode, and each of them
leaves you with two sources of truth that say different things and no way to
tell which is right.

Deriving the answer from the data has none of those problems, because the
question "what does the store hold" and the question "what should I ingest
next" are answered by the same read. There is nothing to keep in sync.

The `except Exception: return set()` is doing something specific: a freshly
created icechunk repository has an initial commit but no zarr group, so
`open_zarr` raises. An empty store means no periods, which is exactly the right
answer for a first run. There is a test for it,
`TestIngest.test_committed_periods_empty_for_new_repo`.

The cost is a full read of the time coordinate on every ingest run. For a
forty-year daily series that is about 14,600 int64 values, a few hundred
kilobytes. Cheap. It also happens to be exactly the read that would need to
happen anyway to validate a manifest.

The `period_type` parameter with its single supported value is deliberate
scaffolding for a generalization that has not been written. It raises on
anything else rather than silently doing the wrong thing.

#### `ingest_period`

```python
def ingest_period(repo: Any, period: Period, fetch: Fetcher) -> str:
    """Fetch, normalize, and commit a single period.

    Returns:
        The snapshot id of the commit.
    """
    ds = normalize(fetch(period))
    ds = ds.chunk(chunking_for(ds))

    session = repo.writable_session("main")
    existing = _has_data(session)
    if existing:
        # align_chunks=True is not optional here. Months are 28-31 days and the
        # store's time chunk is 30, so after a few appends the final zarr chunk
        # is partial and the incoming period straddles it. Without alignment
        # xarray refuses the write outright -- "would overlap multiple Dask
        # chunks" -- because a parallel write across a shared chunk can corrupt
        # it. Alignment rechunks the incoming data to the store's boundaries.
        ds.to_zarr(session.store, append_dim="time", consolidated=False, align_chunks=True)
    else:
        ds.to_zarr(session.store, mode="w", zarr_format=3, consolidated=False)
    return str(session.commit(f"ingest {period.period_id}"))
```

Eight lines of code and a six-line comment, which is the correct ratio here.

The **open-or-create** branch is a pattern that recurs everywhere in this
stack. There is no "create if missing" mode that also appends, so the code has
to ask whether data exists and pick a call. On the create path it passes
`mode="w"` and `zarr_format=3`, pinning the Zarr v3 format explicitly rather
than taking whatever the installed version defaults to. On the append path it
passes `append_dim="time"`.

`consolidated=False` appears on both. Consolidated metadata is a Zarr v2
optimization -- a single `.zmetadata` file caching every array's metadata so a
client makes one request instead of N. icechunk does not need it, because its
snapshot already contains the full metadata tree, and asking for it produces a
warning. This is a small thing that trips up everyone coming from plain Zarr.

`align_chunks=True` is the important one, and it gets its
[own section below](#why-align_chunkstrue-is-mandatory) because the failure it
prevents is instructive.

The commit message is `f"ingest {period.period_id}"`, which is what makes
`repo.ancestry()` readable as a log -- printed in full under
[`0102_streaming_ingest`](#0102_streaming_ingest-one-period-one-commit).

#### `_has_data`

```python
def _has_data(session: Any) -> bool:
    """Report whether a session's store already holds data variables."""
    try:
        ds = xr.open_zarr(session.store, consolidated=False)
    except Exception:
        return False
    return bool(ds.data_vars)
```

Checks `data_vars` rather than merely whether the group opens, because a store
can have coordinates and attributes written without any data variables. The
try/except is the same "fresh repository has no group" case as in
`committed_periods`.

#### `ingest`

```python
def ingest(
    repo: Any,
    periods: list[Period],
    fetch: Fetcher,
    *,
    resume: bool = True,
    stop_after: int | None = None,
) -> IngestReport:
    """Ingest a list of periods, one commit each."""
    report = IngestReport()
    present = committed_periods(repo) if resume else set()

    for period in periods:
        if stop_after is not None and len(report.ingested) >= stop_after:
            break
        if period.period_id in present:
            report.skipped.append(period.period_id)
            continue
        try:
            snapshot = ingest_period(repo, period, fetch)
        except Exception as exc:
            report.failed[period.period_id] = f"{type(exc).__name__}: {exc}"
            continue
        report.ingested.append(period.period_id)
        report.snapshots[period.period_id] = snapshot

    return report
```

The driver loop, and the three decisions in it are all about failure.

**`present` is computed once, before the loop.** Not per period. The set does
not change during the run except by this run's own writes, and those are
tracked in `report.ingested`. One store read instead of N.

**A failing period does not abort the run.** The `except Exception` catches,
records `f"{type(exc).__name__}: {exc}"` in `report.failed`, and continues. The
justification is stated in the example: one bad month is not a reason to
discard four good ones. A source outage for March should not cost you April
and May, especially when each of those is a network round trip you have already
paid for.

Recording the type name alongside the message is a small thing that pays off
in logs. `"source unavailable"` is ambiguous; `"RuntimeError: source
unavailable: upstream returned HTTP 503"` is actionable.

**`stop_after` is a first-class parameter, not a test hook.** It exists so
`0103_resume` can simulate a crash honestly. The check is
`len(report.ingested) >= stop_after`, counting *ingested* periods rather than
loop iterations, so skipped periods do not consume the budget. Resume a
half-done run with `stop_after=2` and it does two *new* periods rather than
stopping after two skips.

`resume=True` is the default, which makes the safe behaviour the easy one.
Setting `resume=False` re-ingests everything, which for an append-only store
means duplicating timestamps -- there is no good reason to do it, and it is
available because being able to say "no, really, everything" is occasionally
necessary.

#### `store_path`

```python
def store_path(base: Path | str, dataset_id: str) -> Path:
    """Return the on-disk path for a dataset's store.

    Mirrors the open-climate-service layout,
    ``{data_dir}/downloads/{dataset_id}.icechunk``.
    """
    return Path(base) / "downloads" / f"{dataset_id}.icechunk"
```

Four lines that exist to keep one convention in one place. The layout is OCS's:
an instance has a data directory, stores live under `downloads/`, and each is
named for its dataset id with an `.icechunk` suffix.

The reason it is a function rather than an f-string at each call site is that
the dataset id is also the STAC collection id and the `{id}` in the HTTP route.
Having one function that maps id to path makes the correspondence explicit and
makes changing the layout a one-line change.

What lands on disk is not a file but a directory tree:

```text
temperature.icechunk/
  chunks/
  manifests/
  snapshots/
  transactions/
  overwritten/
  repo
```

`chunks/` holds the compressed array data. `manifests/` holds the maps from
chunk coordinates to chunk objects. `snapshots/` holds the commits.
`transactions/` holds the write-ahead records. `repo` is the repository
configuration. That structure is what makes an icechunk commit atomic: the
chunk objects are written first and are inert until a snapshot references them.

A twelve-month, 16 x 16 store on this machine occupies about 1.1 MB on disk for
753 kB of logical data -- **machine-dependent**, and dominated by per-commit
metadata at this scale rather than by the arrays.

---

### `indices.py` — the products people actually ask for

Source: [`../../climate-pipeline/src/ocs_stack_climate_pipeline/indices.py`](../../climate-pipeline/src/ocs_stack_climate_pipeline/indices.py)

**Responsibility.** Turn the stored daily series into the derived quantities a
user requests.

```python
"""Climate indices: the derived products a service actually publishes.

Raw temperature and rainfall are inputs, not answers. What a health ministry
or planning office asks for is "how many hot days", "was this month unusually
dry", "when does the rainy season start" -- indices computed from the stored
series. These are the shape of the processes open-climate-service exposes over
openEO, implemented directly here so the arithmetic is visible.
"""
```

Seven functions, all of them small, and the interesting thing about them is
which reduction each one chooses and why.

#### `_require`

```python
def _require(ds: xr.Dataset, variable: str) -> xr.DataArray:
    """Fetch a variable or raise a clear error naming what is available."""
    if variable not in ds.data_vars:
        raise KeyError(f"dataset has no variable {variable!r}; it has {tuple(ds.data_vars)}")
    return ds[variable]
```

Every public function in the module goes through this. The value is entirely in
the error message:

```python
>>> hot_days(ds, variable="nope")
KeyError: "dataset has no variable 'nope'; it has ('t2m',)"
```

versus what plain `ds["nope"]` gives you, which is a `KeyError: 'nope'` with no
indication of what would have worked. Naming the available variables turns a
guessing game into a typo fix. It is three lines. There is a test,
`TestIndices.test_missing_variable_raises`.

#### `climatological_normal`

```python
def climatological_normal(ds: xr.Dataset, variable: str = "t2m") -> xr.DataArray:
    """Return the per-month mean over all years: the climatological normal.

    Returns:
        A ``(month, y, x)`` array of long-run monthly means.
    """
    return _require(ds, variable).groupby("time.month").mean()
```

One line. `groupby("time.month")` uses xarray's **virtual datetime accessor**:
`time.month` is not a coordinate that exists, it is derived from the `time`
coordinate on the fly. The same syntax gives you `time.year`, `time.season`,
`time.dayofyear`, and the rest of the pandas `.dt` vocabulary.

The output dimension is named `month` and carries values 1 through 12. Note the
shape change: `(time, y, x)` becomes `(month, y, x)`. From `0201_climatology`:

```text
  normal dims = ('month', 'y', 'x'), sizes = {'month': 12, 'y': 12, 'x': 12}
  the daily (time, y, x) series collapsed to (month, y, x): 366 steps -> 12
```

366 daily maps become 12 monthly maps. That is a 30x reduction in size and it
is the entire product. The seasonal cycle it produces on one synthetic year is
printed in full under [`0201_climatology`](#0201_climatology-normals-and-anomalies):
a 5.72-degree annual range, which is a plausibly shallow tropical cycle.

#### `monthly_anomaly`

```python
def monthly_anomaly(ds: xr.Dataset, variable: str = "t2m") -> xr.DataArray:
    """Return each timestep's departure from its month's normal.

    Anomalies, not absolute values, are what make two places or two years
    comparable -- which is why nearly every published climate product is one.
    """
    values = _require(ds, variable)
    normal = values.groupby("time.month").mean()
    anomaly = values.groupby("time.month") - normal
    anomaly.attrs = {**values.attrs, "long_name": f"{variable} anomaly"}
    return anomaly
```

The line that does the work is `values.groupby("time.month") - normal`. That is
xarray's **grouped broadcast**: the left side is a GroupBy object, the right
side is an array indexed by `month`, and the subtraction aligns each group with
its matching entry and subtracts. Writing it out by hand would be a loop over
twelve months with twelve boolean masks.

The shape is *preserved*, not reduced: `(time, y, x)` in, `(time, y, x)` out.
Each daily value has had its own month's normal removed.

The attrs line matters for the same reason it mattered in `convert_units`.
`units` is carried through from the input -- an anomaly in degC is still in
degC, since it is a difference of temperatures -- while `long_name` is rewritten
so a chart legend says "t2m anomaly" rather than "2 metre temperature". Getting
this backwards, and labelling an anomaly field with the original long name, is
how a chart ends up claiming that Sierra Leone is at -2 degrees.

The self-check from `0201_climatology`:

```text
  anomaly dims = ('time', 'y', 'x'), shape matches the input: True
  units carried through: 'degC', long_name 't2m anomaly'

  grand mean of the anomaly field: -1.93e-16 degC  (zero, to floating point)
```

The per-month breakdown, all twelve within 1e-15 of zero, is printed under
[`0201_climatology`](#0201_climatology-normals-and-anomalies).

There is a test asserting this to `abs=1e-9`,
`TestIndices.test_monthly_anomaly_centres_on_zero`.

#### `hot_days` and `wet_days`

```python
def hot_days(ds: xr.Dataset, threshold: float = 30.0, variable: str = "t2m") -> xr.DataArray:
    """Count days per month above a temperature threshold."""
    values = _require(ds, variable)
    counts = (values > threshold).resample(time="1ME").sum()
    counts.attrs = {"units": "days", "long_name": f"days above {threshold} degC"}
    return counts

def wet_days(ds: xr.Dataset, threshold: float = 1.0, variable: str = "tp") -> xr.DataArray:
    """Count days per month with rainfall at or above a threshold.

    One millimetre is the conventional cutoff for a "wet day": below it, the
    reading is indistinguishable from dew or gauge noise.
    """
    values = _require(ds, variable)
    counts = (values >= threshold).resample(time="1ME").sum()
    counts.attrs = {"units": "days", "long_name": f"days with at least {threshold} mm"}
    return counts
```

Structurally identical, and the pattern is worth naming: **compare to get a
boolean array, then sum the booleans over a resampled period.** Summing a
boolean array counts the `True` values, because `True` is 1. It is a one-line
idiom that replaces an explicit count.

Two details that are not cosmetic.

The comparison operators differ: `>` for hot days, `>=` for wet days. Strictly
above the temperature threshold, at or above the rainfall threshold. That is
the convention in the index literature -- a "hot day" exceeds the line, and a
"wet day" reaches 1 mm. A 1.0 mm day is wet. A 30.0 degree day is not hot.
Small, and exactly the kind of thing that makes two implementations of the same
index disagree by a few days a year.

`time="1ME"` is **month-end** frequency in modern pandas. It used to be `"M"`,
which pandas deprecated in 2.2 because `"M"` was ambiguous between month-start
and month-end. The resampled time coordinate carries the *last* day of each
month as its label, which is why the display code throughout the examples
formats `str(stamp)[5:7]` to recover the month rather than assuming the first.

The attrs are set completely rather than carried through, because the output is
a *count*, not a temperature. `units` becomes `"days"`. Carrying `"degC"`
through onto a count of days would be worse than useless.

The payoff shows up when you print the monthly mean beside the count, which
[`0202_indices`](#0202_indices-the-products-a-user-asks-for) does in full.
March and April share a mean to two decimal places, 29.85 both, and differ in
hot-day count by nearly a day -- 23.2 against 22.5. February and May share a
mean of 29.12 and score 15.8 and 16.5. The mean is a first moment; the count is
sensitive to the whole distribution. That is exactly why the count is the
published product: a health ministry plans around how many days crossed the
line, not around the average.

#### `monthly_total`

```python
def monthly_total(ds: xr.Dataset, variable: str = "tp") -> xr.DataArray:
    """Sum a variable per month -- the right reduction for rainfall.

    Temperature is intensive and gets averaged; rainfall is extensive and gets
    summed. Using the wrong one is a classic and silent error.
    """
    values = _require(ds, variable)
    totals = values.resample(time="1ME").sum()
    totals.attrs = {**values.attrs, "long_name": f"monthly total {variable}"}
    return totals
```

The one-liner whose docstring is longer than its body, and rightly so. The
whole content of this function is the choice of `.sum()` over `.mean()`, and
that choice is the [intensive/extensive distinction](#intensive-versus-extensive)
made executable.

Note that `units` *is* carried through here, unlike in `hot_days`. A monthly
total of millimetres is still millimetres. The dimensions of the quantity did
not change; only the period it covers did. That asymmetry -- counts get new
units, totals keep theirs -- is a small thing that tells you the author was
paying attention.

#### `spi_like`

```python
def spi_like(ds: xr.Dataset, variable: str = "tp") -> xr.DataArray:
    """Standardize monthly rainfall totals against their own month's history.

    A simplified standardized precipitation index: for each calendar month,
    subtract that month's long-run mean and divide by its standard deviation,
    so -2 means "far drier than this month usually is". The real SPI fits a
    gamma distribution first; the standardization idea is the same.
    """
    totals = monthly_total(ds, variable)
    grouped = totals.groupby("time.month")
    mean = grouped.mean()
    std = grouped.std()
    # Guard against a month with no variation, which would divide by zero.
    safe_std = std.where(std > 0, np.nan)
    index = (totals.groupby("time.month") - mean).groupby("time.month") / safe_std
    index.attrs = {"units": "1", "long_name": "standardized precipitation index (simplified)"}
    return index
```

The most involved function in the module, and the one with the most to say.

It composes on `monthly_total`, which means the extensive/intensive decision is
made once and inherited. Then it groups the monthly totals by calendar month --
so all four Januaries in a four-year record form one group -- and computes each
group's mean and standard deviation. The index is
`(value - group_mean) / group_std`.

The double `groupby` in the last expression looks redundant and is not. A
GroupBy object is consumed by the operation applied to it, so the subtraction
produces an ordinary DataArray and the division needs its own grouping to align
against `safe_std`. Writing it as one chained expression would not work.

`safe_std = std.where(std > 0, np.nan)` is the guard, and its behaviour is a
design decision worth defending. If a calendar month has no variation -- which,
with a one-year record, is *every* month, because each has exactly one sample
and a standard deviation of zero -- the division would produce `inf` or a
`RuntimeWarning` and a nonsense number. Replacing the zero with NaN makes the
result NaN, which propagates cleanly and is honest.

`0402_second_dataset` demonstrates exactly this:

```text
  spi_like over one year -> (12, 16, 16), finite values: 0
  All NaN, and correctly so: with one year, each calendar month has exactly
  one sample, its standard deviation is zero, and 'unusual compared to what?'
  has no answer. The library refuses rather than dividing by zero.
```

That is the right answer. "How unusual was this January?" with one January on
record has no answer, and returning NaN says so. Returning 0.0, which is what a
naive implementation that skipped the guard would produce for the mean-centred
numerator over a tiny epsilon, would say "perfectly normal" -- a confident lie.

`units = "1"` marks the result as **dimensionless**, which is the CF convention
for a pure number. That is the whole point of standardizing: a value of -1.5
means the same thing in a rainforest and a savanna, so drought monitoring can
use one threshold everywhere.

Over four years at one grid cell -- the full table is under
[`0202_indices`](#0202_indices-the-products-a-user-asks-for) -- June receives
220 mm and scores -1.59, flagged as unusually dry, while February receives 22 mm
and scores +1.63, flagged as unusually wet. Ten times less rain, and it is the
*wetter* month by this measure, because 22 mm is a lot for a February at this
cell and 220 mm is not much for a June. That inversion is the entire value of
standardization, and it is impossible to see in millimetres.

Two caveats the code cannot fix and the examples state anyway.

First, **a short record puts a hard ceiling on the index.** xarray's `std()`
uses the population standard deviation (`ddof=0`), and with `n` samples the
largest z-score any one of them can achieve is `sqrt(n - 1)`, no matter how
extreme it is. Three years gives `sqrt(2) = 1.414`; four years gives
`sqrt(3) = 1.732`; thirty years gives `sqrt(29) = 5.385`:

```python
>>> for n in (3, 4, 5, 30):
...     v = np.zeros(n); v[0] = 1000.0          # one absurd outlier
...     print(n, round(float(np.abs((v - v.mean()) / v.std()).max()), 4))
3 1.4142
4 1.7321
5 2.0
30 5.3852
```

`0402_second_dataset` runs SPI over exactly three years and hits the ceiling
precisely:

```text
  Range over the whole cube: -1.41 .. +1.41.
```

That is not a property of the rainfall; it is a property of having three
samples. Since operational drought monitoring raises a flag around -1.5, an SPI
computed over three years **cannot ever raise one**. A short record does not
make the index noisy -- it makes it structurally incapable of reporting the
thing it exists to report. That is the sharpest possible argument for the
thirty-year convention.

Second, **real SPI fits a gamma distribution first.** Monthly rainfall is
skewed and zero-inflated rather than normal, so a plain z-score against a
Gaussian assumption misstates the tails -- and the tails are the entire part
drought monitoring cares about. The proper procedure fits a gamma to the
monthly totals for each calendar month, evaluates the cumulative distribution
function at the observed value, and maps that probability through the inverse
normal CDF. The standardization *idea* -- how unusual is this, in units of its
own month's spread -- is unchanged, which is why `spi_like` is a useful thing
to read even though it is not a useful thing to deploy. Again: use
[xclim](https://xclim.readthedocs.io/) for anything real.

#### `pyramid_levels`

```python
def pyramid_levels(ds: xr.Dataset, levels: int = 3) -> list[xr.Dataset]:
    """Build coarser resolutions by repeated 2x2 mean downsampling.

    This is how open-climate-service builds the multiscale GeoZarr pyramid a
    map viewer needs: level 0 is full resolution, each subsequent level halves
    both spatial dimensions so a zoomed-out tile reads a small array instead
    of the whole grid.

    Raises:
        ValueError: If levels is less than 1.
    """
    if levels < 1:
        raise ValueError(f"levels must be at least 1, got {levels}")
    out = [ds]
    current = ds
    for _ in range(levels - 1):
        if current.sizes.get("y", 1) < 2 or current.sizes.get("x", 1) < 2:
            break
        # xarray injects the reduction methods onto Coarsen at runtime, so
        # neither type checker can see .mean(); go through Any deliberately.
        coarsened: Any = current.coarsen(y=2, x=2, boundary="trim")
        current = coarsened.mean()
        out.append(current)
    return out
```

`coarsen(y=2, x=2)` groups the array into non-overlapping 2 x 2 blocks and
`.mean()` reduces each block to one value. Applied repeatedly, that is an image
pyramid.

`boundary="trim"` decides what happens when a dimension is odd. The
alternatives xarray offers are `"exact"` (raise), `"trim"` (drop the remainder),
and `"pad"` (fill with NaN and include it). Trimming is correct here and the
reason is arithmetic: a mean of means equals the mean of the whole *only when
the blocks are equal in size*. Padding a 15-row grid to 16 with NaN and taking
`mean` would fold a half-weight row into the last coarse cell, producing a value
that is not the mean of anything real. Dropping the odd row loses a sliver of
data and keeps every remaining value honest.

The early `break` when a dimension drops below 2 means `levels` is a *maximum*,
not a promise. Asking for eight levels on a 16 x 16 grid gives you five, because
there is nothing left to halve. Returning fewer levels rather than raising is
the right call for a caller that just wants "as deep as this grid supports".

The `coarsened: Any` annotation with its comment is a real note from the field:
xarray attaches the reduction methods to `Coarsen` dynamically at import time,
so neither mypy nor pyright can see `.mean()` on it. Going through `Any`
deliberately, with a comment saying why, is better than a bare `# type: ignore`
because it says which of the two possible problems it is.

[`0203_pyramid`](#0203_pyramid-multiscale-levels-and-why-a-viewer-needs-them)
verifies the reduction by hand -- a level-1 cell equals the mean of its four
level-0 parents to within 1e-12, and a level-2 cell equals the mean of the
corresponding 4 x 4 block -- and prints the size of every level.

Each level is a quarter of the one above. The infinite series
`1 + 1/4 + 1/16 + ...` converges to 4/3, so a complete pyramid costs at most 33
percent more storage than level 0 alone -- and the real run confirms 159.4 KB
against 120.0 KB.

Note that **time is never coarsened**. Only `y` and `x` appear in the `coarsen`
call. Zoom is a spatial question; a viewer zoomed out over the country still
wants a specific day.

---

### `publish.py` — turning a store into a product

Source: [`../../climate-pipeline/src/ocs_stack_climate_pipeline/publish.py`](../../climate-pipeline/src/ocs_stack_climate_pipeline/publish.py)

**Responsibility.** Produce the metadata that lets somebody who has never seen
this store place it on Earth and find it in a catalogue.

```python
"""Publishing: GeoZarr attributes and STAC metadata.

Storing the data is not the same as publishing it. A client that finds this
store needs to know where on Earth the grid sits, in which CRS, what time
range it covers, and what the variables mean. GeoZarr answers the first two
with root attributes; STAC answers the rest with a collection document that a
catalogue can index.
"""
```

One constant:

```python
CRS = "EPSG:4326"
```

Everything in this project is on WGS 84 geographic coordinates -- plain degrees
of latitude and longitude. A real service supporting projected grids would need
to carry the CRS per dataset; this one hard-codes the single case it handles,
which is honest about its scope.

#### `grid_transform`

```python
def grid_transform(ds: xr.Dataset) -> list[float]:
    """Return the affine transform placing a north-up grid on Earth.

    The six values are ``[stepX, rotX, originX, rotY, stepY, originY]`` with
    the origin on the OUTER EDGE of the first cell -- pixel registration, not
    cell centres -- and a negative y step for a north-up grid. Getting the
    half-cell offset wrong shifts every rendered tile by half a pixel.

    Raises:
        ValueError: If the grid has fewer than two cells on an axis.
    """
    if ds.sizes.get("x", 0) < 2 or ds.sizes.get("y", 0) < 2:
        raise ValueError("a transform needs at least two cells on each axis")

    x = ds["x"].values
    y = ds["y"].values
    step_x = float(x[1] - x[0])
    step_y = float(y[1] - y[0])
    origin_x = float(x[0]) - step_x / 2.0
    origin_y = float(y[0]) - step_y / 2.0
    return [step_x, 0.0, origin_x, 0.0, step_y, origin_y]
```

Six numbers, and each one is a decision.

The **step** is derived from the first two coordinates, which assumes a regular
grid. That is a real assumption. A Gaussian grid -- which is what a spectral
model natively produces, with latitudes clustered toward the equator -- is not
regular, and an affine transform cannot describe it. Such grids have to be
regridded before they can be published this way, which is a whole subject
(`xesmf`, conservative remapping) that this project does not touch.

The **origin** is `first_centre - step / 2`. That is the pixel registration
discussed above. The subtraction is signed, so it works for both axes: with
`step_y` negative, subtracting half of it *adds* half a cell northward, which
lands on the north edge of row 0. Getting this to work without a special case
for the y axis is the small elegance of the function.

**`rotX` and `rotY` are hard zero.** An affine transform can express a rotated
or sheared grid, and this one never does -- the grid is axis-aligned by
construction. Emitting the zeros anyway keeps the six-element shape that every
consumer expects.

The **guard**. A one-cell axis has no step to measure, and `x[1]` would raise
`IndexError` -- an error that says nothing about what went wrong. Raising
`ValueError("a transform needs at least two cells on each axis")` is a
better failure. There is a test for it, `TestPublish.test_transform_needs_a_real_grid`.

[`0301_geozarr`](#0301_geozarr-putting-a-grid-of-numbers-somewhere-on-earth)
takes the six coefficients apart one at a time on a real 12 x 16 store and
checks the half-cell offset numerically against the coordinate arrays with
`assert np.isclose(...)`, so a wrong transform fails the run rather than
shipping.

#### `bounding_box`

```python
def bounding_box(ds: xr.Dataset) -> list[float]:
    """Return ``[west, south, east, north]`` covering the grid's outer edges."""
    step_x, _, origin_x, _, step_y, origin_y = grid_transform(ds)
    far_x = origin_x + step_x * ds.sizes["x"]
    far_y = origin_y + step_y * ds.sizes["y"]
    return [min(origin_x, far_x), min(origin_y, far_y), max(origin_x, far_x), max(origin_y, far_y)]
```

Built **on top of** `grid_transform` rather than beside it, which is the
important structural choice. The bbox and the transform cannot disagree,
because one is computed from the other. Compute the bbox independently from
`y.min()` and `y.max()` and you would get the *centre* extent, half a cell
smaller on every side, and you would have a catalogue that claims a slightly
different footprint from the one the renderer draws.

`origin + step * n` walks the full width of the grid from the outer edge of the
first cell to the outer edge of the last -- `n` cells, not `n - 1`, because the
origin is already an edge.

The `min`/`max` pair normalizes the order regardless of step sign. `far_y` is
*south* of `origin_y` because `step_y` is negative, so a naive
`[origin_x, origin_y, far_x, far_y]` would emit `[west, north, east, south]` --
a bbox with its latitudes inverted, which some consumers silently accept and
render as an empty rectangle. `TestPublish.test_bbox_orders_west_south_east_north`
asserts `west < east and south < north`.

#### `geozarr_attrs`

```python
def geozarr_attrs(ds: xr.Dataset) -> dict[str, Any]:
    """Build the GeoZarr root attributes for a dataset."""
    return {
        "spatial:transform": grid_transform(ds),
        # Array order, y first: read positionally by clients. Naming these
        # x-first transposes every raster that reads the store.
        "spatial:dimensions": ["y", "x"],
        "spatial:shape": [int(ds.sizes["y"]), int(ds.sizes["x"])],
        "spatial:bbox": bounding_box(ds),
        "proj:code": CRS,
        "zarr_conventions": [{"name": "geozarr", "version": "0.4"}],
    }
```

Six keys, all JSON-serializable primitives, which they must be because they are
going into Zarr attributes.

The comment marks the trap. `["y", "x"]` and `[ny, nx]` are in **array order** --
the order the axes appear in the buffer, slow axis first. A client reads them
positionally. Emit `["x", "y"]` and `[nx, ny]` and every raster that reads the
store is transposed.

The `int()` casts around `ds.sizes[...]` are not decorative. `ds.sizes` values
can be numpy integers depending on how the dataset was constructed, and
`json.dumps` refuses `np.int64`. Casting at the boundary is cheaper than
debugging a serialization error three layers down.

`zarr_conventions` declares which version of the spec these attributes follow,
so a reader can tell 0.4 semantics from a future revision's.

`0301_geozarr` performs the transposition on a deliberately non-square 12 x 16
grid and reads the wrong number out loud. Same bytes, same chunks, different
geography -- only the stride the client walks the buffer with changed.

#### `temporal_extent`

```python
def temporal_extent(ds: xr.Dataset) -> list[str]:
    """Return the ISO 8601 start and end of a dataset's time axis.

    Raises:
        ValueError: If the dataset has no time values.
    """
    if "time" not in ds.coords or ds.sizes.get("time", 0) == 0:
        raise ValueError("dataset has no time coordinate to describe")
    stamps = pd.DatetimeIndex(np.asarray(ds["time"].values))
    return [stamps[0].isoformat() + "Z", stamps[-1].isoformat() + "Z"]
```

The interesting thing is `stamps[0]` and `stamps[-1]` -- **first and last by
position**, not `min()` and `max()` by value.

That is a deliberate choice with a consequence, and it connects directly to a
finding from `0103_resume`. Backfilling a gap by appending March after May
leaves the time axis complete but non-monotonic, and `temporal_extent` on such
a store reports the interval ending at the last *appended* value rather than
the latest *date*:

```text
  store after the retry: periods=[...all five...] steps=152 span=2024-01-01 .. 2024-03-31
```

That is arguably a bug and is arguably the honest answer -- it reports what the
axis literally is, which surfaces the disorder rather than papering over it. The
robust fix is not `min`/`max` in this function; it is not letting the axis go
non-monotonic in the first place, which is what "ingest in order and retry
early" means.

The `+ "Z"` appends a UTC designator. That is correct here because the stored
timestamps are naive `datetime64` values that the pipeline treats as UTC
throughout, and STAC requires an RFC 3339 timestamp with an offset. It would be
wrong for a dataset carrying genuinely local timestamps, which is another
simplification worth knowing about.

#### `stac_collection`

```python
def stac_collection(
    ds: xr.Dataset,
    dataset_id: str,
    *,
    title: str | None = None,
    description: str = "",
    zarr_href: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a STAC Collection document describing a published dataset."""
    bbox = bounding_box(ds)
    interval = temporal_extent(ds)
    stamp = (now or datetime.now(UTC)).isoformat()

    variables: dict[str, Any] = {}
    for name, var in ds.data_vars.items():
        values = np.asarray(var.values, dtype="float64")
        variables[str(name)] = {
            "units": var.attrs.get("units", "unknown"),
            "long_name": var.attrs.get("long_name", str(name)),
            "min": round(float(np.nanmin(values)), 4),
            "max": round(float(np.nanmax(values)), 4),
        }

    collection: dict[str, Any] = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": dataset_id,
        "title": title or dataset_id,
        "description": description,
        "license": "proprietary",
        "extent": {
            "spatial": {"bbox": [bbox]},
            "temporal": {"interval": [interval]},
        },
        "summaries": {"variables": variables, "proj:code": [CRS]},
        "properties": {"published": stamp},
        "links": [],
    }
    if zarr_href:
        collection["assets"] = {
            "zarr": {
                "href": zarr_href,
                "type": "application/vnd+zarr",
                "roles": ["data"],
                "title": "Zarr store",
            }
        }
    return collection
```

The longest function in the library, and mostly it is a dictionary literal. The
decisions are in the first six lines and in the shape of the nesting.

**Extents are derived.** `bounding_box(ds)` and `temporal_extent(ds)` read the
dataset. There is no `bbox=` parameter, no `start=`/`end=` parameter. You
cannot publish a collection claiming an extent the store does not have, because
the function does not accept one. That is the design decision the whole project
is proudest of, and it gets its own section under
[design decisions](#why-extents-are-read-back-rather-than-declared).

**`now` is injectable.** `now: datetime | None = None`, defaulting to
`datetime.now(UTC)`. Every example pins it:

```python
PUBLISHED_AT = datetime(2024, 7, 1, 12, 0, tzinfo=UTC)
```

which makes the emitted JSON byte-identical on every run. That is the same
concern as the crc32 seed: a documented output that changes every run is not a
documented output. It also makes the function testable without freezing time.

**The double nesting.** `"bbox": [bbox]` and `"interval": [interval]`, both
lists of lists. This is STAC, not an accident: a collection may cover several
disjoint footprints or several disjoint time ranges, so both fields are lists.
A single-grid dataset still nests one level deep. It is the single most common
STAC mistake and `0302_stac` calls it out.

**Summaries.** For each data variable: units, long name, and the value range,
rounded to four decimals. `np.nanmin`/`np.nanmax` rather than `min`/`max`, so a
grid with masked cells does not report NaN as its minimum.

Note that computing min and max **loads the whole array**. `var.values` on a
dask-backed dataset triggers a full read and compute. For the sizes here that
is a few hundred kilobytes and instant; for a forty-year, 4000 x 4000 dataset
it is a full scan of the archive every time you publish. A production version
would either compute the range incrementally at ingest and store it as an
attribute, or accept an approximation from a subsample. It is the one place in
this library where the toy scale hides a real cost, and it is worth naming.

**`assets` is conditional.** If no `zarr_href` is given, the key is absent
entirely rather than present and null. A document describing a store that is
not served is a legitimate thing -- an archived dataset, a staged one -- and an
absent asset says that more clearly than a null href.

The full document, from a real run of `0302_stac`:

```json
{
  "type": "Collection",
  "stac_version": "1.0.0",
  "id": "temperature-daily-sle",
  "title": "Daily 2m temperature, Sierra Leone",
  "description": "Daily mean 2 metre air temperature, normalized to degC on a north-up WGS 84 grid.",
  "license": "proprietary",
  "extent": {
    "spatial": {
      "bbox": [
        [
          -13.606666666666666,
          6.759090909090915,
          -10.193333333333339,
          10.14090909090909
        ]
      ]
    },
    "temporal": {
      "interval": [
        [
          "2024-01-01T00:00:00Z",
          "2024-06-30T00:00:00Z"
        ]
      ]
    }
  },
  "summaries": {
    "variables": {
      "t2m": {
        "units": "degC",
        "long_name": "2 metre temperature",
        "min": 23.6641,
        "max": 33.9401
      }
    },
    "proj:code": [
      "EPSG:4326"
    ]
  },
  "properties": {
    "published": "2024-07-01T12:00:00+00:00"
  },
  "links": [],
  "assets": {
    "zarr": {
      "href": "https://climate.example.org/zarr/temperature-daily-sle",
      "type": "application/vnd+zarr",
      "roles": [
        "data"
      ],
      "title": "Zarr store"
    }
  }
}
```

`links` is an empty list, which is a real gap: a conformant catalogue wants
`self`, `root`, and `parent` links, and those depend on where the document is
served from, which this project does not know because it has no HTTP layer.

---

## Phase 1 — Ingest

Three examples covering the left half of the pipeline: what a source hands
over, what normalization does to it, how it lands in the store, and what
happens when the run does not finish.

### `0101_normalize` — what is wrong with a raw source

Source: [`../../climate-pipeline/examples/0101_normalize.py`](../../climate-pipeline/examples/0101_normalize.py)

**What it teaches.** The four things wrong with a raw climate dataset --
dimension names, units, axis direction, duplicate timestamps -- demonstrated one
at a time against the actual synthetic sources, and then fixed by `normalize()`
with the correction checked numerically.

The example is built around a small helper that prints the four facts a service
cares about:

```python
def describe(ds: xr.Dataset, variable: str, label: str) -> None:
    var = ds[variable]
    y_name = "y" if "y" in ds.coords else "lat"
    y_first, y_last = float(ds[y_name][0]), float(ds[y_name][-1])
    direction = "ascending (south-up)" if y_first < y_last else "descending (north-up)"
    print(f"  {label} dims      : {var.dims}")
    print(f"  {label} units attr: {var.attrs.get('units')!r}")
    print(f"  {label} y axis    : {y_first:.2f} -> {y_last:.2f}  {direction}")
    print(f"  {label} value mean: {float(var.mean()):.3f}")
```

Note the `y_name` line: the helper has to sniff which spelling the dataset uses
in order to describe it, which is itself a demonstration of the problem. Generic
code that wants to work before and after normalization cannot name the axis.

The raw dataset:

```text
  raw    dims      : ('time', 'lat', 'lon')
  raw    units attr: 'K'
  raw    y axis    : 6.90 -> 10.00  ascending (south-up)
  raw    value mean: 300.949
```

Three problems, and the example names each one with its consequence:

```text
  1. dims are (time, lat, lon). Generic code cannot write ds.mean('y')
     when half the sources spell that axis 'lat' and half 'latitude'.
  2. units are Kelvin. An API that promises degC would publish 300.
  3. y ascends, so row 0 is the SOUTHERNMOST. A GeoZarr affine assumes
     row 0 is north; render this as-is and the map is upside down.
```

After `normalize()`:

```text
  normal dims      : ('time', 'y', 'x')
  normal units attr: 'degC'
  normal y axis    : 10.00 -> 6.90  descending (north-up)
  normal value mean: 27.799
```

and the arithmetic is checked rather than asserted in prose:

```text
  check: 300.949 K - 273.15 = 27.799 == 27.799 degC
  check: row 0 of the raw grid is lat 6.90 (south);
         row 0 of the normalized grid is y 10.00 (north)
```

**Why it matters.** This is the example that justifies the existence of the
whole normalization module, and it does it by being concrete about the cost of
skipping it. A service serving six sources without normalization needs
per-source handling at every endpoint, forever, and gets it wrong for the
seventh source. Normalizing at the door means every function downstream --
`hot_days`, `grid_transform`, `stac_collection` -- can assume `(time, y, x)`
in degC on a north-up grid, and none of them needs a parameter saying which
convention it is dealing with.

The precipitation half of the example makes the units point in a way that
temperature cannot:

```text
  metres are a reanalysis habit; a mm total of 2103 mm reads as rainfall,
  where 2.103 m does not. Same numbers, 1000x apart.
```

Nobody is confused by a Kelvin temperature -- 300 is obviously not Celsius. But
`0.00047` m of rain and `0.47` mm of rain look equally plausible on a chart
axis with no units label, and the 1000x error is entirely silent.

**The traps.**

*The attrs trap.* This is the one the example spends the most words on, and
rightly so:

```text
Why conversion and attribute rewriting happen in the same function:
  xarray never reads the 'units' attribute. Subtracting 273.15 changes the
  numbers and leaves attrs['units'] saying 'K'. Nothing raises. The dataset
  is now Celsius labelled Kelvin, and the error only surfaces months later
  in somebody's chart. convert_units() does both edits together so they
  cannot drift apart.
  demo: (raw - 273.15).attrs = {'units': 'K', 'long_name': '2 metre temperature'}  <- still says K
        normalize()  .attrs = {'units': 'degC', 'long_name': '2 metre temperature'}
```

The demo line is the important one, because it shows the failure actually
happening rather than describing it. `raw_t["t2m"] - 273.15` is now Celsius and
still says `K`.

*The duplicate-timestamp trap.* The last section concatenates a period with
itself to simulate a re-fetch:

```python
doubled = xr.concat([norm_t, norm_t], dim="time")
```

```text
  concat of a period with itself: time=62 steps
  unique timestamps?             : False
  after normalize()              : time=31 steps
  unique timestamps?             : True
  sort_time() keeps the LAST duplicate, so a re-fetch acts as a correction:
  the newer copy of a day wins over the one already held.
```

A store with duplicate time coordinates is not merely untidy. `ds.sel(time="2024-01-15")`
on such a store returns a 2-element array instead of a scalar, which breaks
every caller that expected a scalar. `resample` produces doubled sums.
`is_monotonic_increasing` still reports `True` for a sorted axis with
duplicates, so the obvious sanity check does not catch it. Resolving duplicates
at normalization, before the append, is the only place where it is cheap.

*The trap the example does not cover.* `normalize()` does nothing about
missing-data sentinels. A source publishing `-9999` for ocean cells would be
carried straight through into the store, and `climatological_normal` would
happily average it. Real normalization needs a `_FillValue`-to-NaN step; this
project's synthetic sources have no missing data, so it does not have one.

---

### `0102_streaming_ingest` — one period, one commit

Source: [`../../climate-pipeline/examples/0102_streaming_ingest.py`](../../climate-pipeline/examples/0102_streaming_ingest.py)

**What it teaches.** The ingestion contract: a source enumerates its periods,
the framework fetches them one at a time, and each one becomes its own commit.
The example then reads the commit history back and explains why the commit
boundary is where it is.

It opens by making the point that a source does not hand over a dataset:

```python
periods: list[Period] = enumerate_periods(2024, 5)
for period in periods:
    print(f"  {period.period_id}  start={period.start}  days={period.days}")
```

```text
  2024-01  start=2024-01-01  days=31
  2024-02  start=2024-02-01  days=29
  2024-03  start=2024-03-01  days=31
  2024-04  start=2024-04-01  days=30
  2024-05  start=2024-05-01  days=31

  5 periods, 152 daily steps in total.
  Note the day counts: 31, 29, 31, 30, 31. Months are not a fixed size.
```

That last line is planted deliberately. Those five numbers are why
`align_chunks=True` exists, and the example returns to it at the end.

The ingest loop itself is four lines:

```python
for period in periods:
    snapshot = ingest_period(repo, period, lambda p: fetch_temperature(p, ny=12, nx=12))
    stored = read_store(repo)
    last_day = str(stored["time"].values[-1])[:10]
    print(f"  {period.period_id:>8}  {stored.sizes['time']:>10}  {last_day:>12}  {snapshot[:12]}...")
```

and the store grows a month at a time, with the reopened store read back after
each commit rather than tracked in a variable:

```text
    period  time steps      last day  snapshot
   2024-01          31    2024-01-31  69453YARNRCB...
   2024-02          60    2024-02-29  SBG4BZZJNCFD...
   2024-03          91    2024-03-31  ZD2JTW4GSN9N...
   2024-04         121    2024-04-30  FS706ZQX64CQ...
   2024-05         152    2024-05-31  75T85KYM1KMC...

  final store: dims={'time': 152, 'y': 12, 'x': 12}, units='degC'
  time axis monotonic: True
  time axis unique   : True
```

31, 60, 91, 121, 152 -- the cumulative day counts. The reopen-after-each-commit
pattern matters: the numbers printed are what a *reader* would see, not what
the writer believes it wrote. That distinction is the whole reason for using a
transactional store.

Then the history:

```python
history = list(repo.ancestry(branch="main"))
for info in history:
    print(f"  {str(info.id)[:12]}...  {info.written_at:%Y-%m-%d %H:%M:%S}  {info.message}")
```

```text
  75T85KYM1KMC...  2026-08-17 17:48:31  ingest 2024-05
  FS706ZQX64CQ...  2026-08-17 17:48:31  ingest 2024-04
  ZD2JTW4GSN9N...  2026-08-17 17:48:31  ingest 2024-03
  SBG4BZZJNCFD...  2026-08-17 17:48:31  ingest 2024-02
  69453YARNRCB...  2026-08-17 17:48:31  ingest 2024-01
  1CECHNKREP0F...  2026-08-17 17:48:31  Repository initialized

  6 snapshots for 5 periods (the extra one is repo creation).
```

`ancestry` walks backwards from the branch tip, so the newest commit is first.
The timestamps are all in the same second because the whole run takes under a
second on this grid -- **machine-dependent**, and on a real ingest they would be
minutes or hours apart, which is what makes the history useful.

`1CECHNKREP0F` is icechunk's fixed initial-snapshot id, identical in every
repository.

**Why it matters.** The commit boundary is the single most consequential design
choice in the ingest module, and the example argues for it directly:

```text
Why per-period commits and not one commit at the end:
  A month of daily data is many chunk writes. If the process dies partway
  through one, an ordinary zarr store on disk is left holding some new
  chunks and a time axis that may or may not have been extended -- torn.
  icechunk only publishes a snapshot when commit() succeeds, so a crash
  leaves the store exactly as of the last completed period. Never half a
  month. Resume then becomes a question you ask the store, not a log file.
  The cost is one snapshot per period instead of one per run, which is
  cheap and doubles as provenance: you can point at when each month landed.
```

The word "torn" is the right one. A plain Zarr store being appended to is not a
database; a crash mid-append leaves chunk objects on disk with no coherent
metadata pointing at them, or metadata claiming a longer array than the chunks
support. Reading such a store gives you either an error or, worse, silently
wrong values from a chunk that was never fully written.

**The traps.**

*The alignment trap*, which the example closes on:

```text
One implementation detail worth knowing -- ingest() passes align_chunks=True:
  The store's time chunk is fixed at 30 days. Months are 28-31. So after
  the first append the final chunk on disk is partial, and the next month's
  data straddles it. xarray refuses that write outright ('would overlap
  multiple Dask chunks') because two writers touching one chunk can corrupt
  it. align_chunks=True rechunks the incoming period onto the store's
  existing boundaries first, which is why 29-day February appends cleanly.
  stored chunk shape: (30, 12, 12), time length 152
```

Full treatment [below](#why-align_chunkstrue-is-mandatory), including the real
`ValueError`.

*The cost of a commit.* One snapshot per period is cheap in bytes and not free
in time. The 48-period ingest in `0202_indices` takes 4.2 seconds -- **machine-dependent**
-- and almost none of that is arithmetic. If you were ingesting hourly rather
than monthly periods, the commit overhead would start to dominate, and the
right answer would be to batch several periods per transaction and accept a
coarser recovery granularity. The trade-off is explicit: recovery granularity
against transaction overhead.

*Silencing the Rust logs.* Every example that touches icechunk starts with:

```python
icechunk.set_logs_filter("error")
```

Without it, icechunk's Rust core emits `WARN`-level chatter on every ordinary
append -- the local-filesystem-storage concurrency warning in particular --
which drowns out the example's own output. The warning is real and worth
knowing about (local filesystem storage is not safe for concurrent commits),
but printing it fifty times during a demonstration teaches nothing.

---

### `0103_resume` — let the store tell you where to restart

Source: [`../../climate-pipeline/examples/0103_resume.py`](../../climate-pipeline/examples/0103_resume.py)

**What it teaches.** Two distinct failure modes and the one recovery rule that
handles both: an interrupted run, and a source that breaks for one period.

The example fakes a crash with `stop_after=2`:

```python
first = ingest(repo, periods, small_temperature, stop_after=2)
```

```text
  report.ingested = ['2024-01', '2024-02']
  report.skipped  = []
  report.failed   = {}
  store after run 1: periods=['2024-01', '2024-02'] steps=60 span=2024-01-01 .. 2024-02-29
```

Then, before restarting, it asks the store what survived:

```python
present = committed_periods(repo)
missing = [p.period_id for p in periods if p.period_id not in present]
```

```text
  committed_periods(repo) = ['2024-01', '2024-02']
  still missing           = ['2024-03', '2024-04', '2024-05']
  Because the last commit is the last thing that succeeded, this answer is
  true even if the process died mid-write on the very next period.
```

That closing clause is the whole argument. There is no window in which the
answer is wrong. A crash during the write of March leaves no commit for March,
so March is missing, so March gets re-fetched. A crash *after* the commit
succeeds but before anything else happens leaves March present, so March is
skipped. There is no third case, because the commit is atomic.

The resumed run:

```python
second = ingest(repo, periods, small_temperature)
```

```text
  report.ingested = ['2024-03', '2024-04', '2024-05']
  report.skipped  = ['2024-01', '2024-02']
  report.total    = 5 periods considered
  store after run 2: periods=['2024-01', '2024-02', '2024-03', '2024-04', '2024-05'] steps=152 span=2024-01-01 .. 2024-05-31
```

Same call, same arguments as the first run. Nothing was passed in to say where
to start. And a third run is a complete no-op:

```text
  run 3: ingested=[] skipped=['2024-01', '2024-02', '2024-03', '2024-04', '2024-05']  <- idempotent
```

Idempotence is worth pausing on. It means a retry is always safe, which means
an operator never has to reason about whether a job already ran. That is the
property that makes `ingest` safe to put behind a cron entry, a Kubernetes
CronJob, or a nervous human hitting a button twice.

The second half of the example uses a source that fails for exactly one month:

```python
def flaky_temperature(period: Period) -> xr.Dataset:
    if period.period_id == "2024-03":
        raise RuntimeError("source unavailable: upstream returned HTTP 503")
    return small_temperature(period)
```

```text
  report.ingested = ['2024-01', '2024-02', '2024-04', '2024-05']
  report.failed   = {'2024-03': 'RuntimeError: source unavailable: upstream returned HTTP 503'}
  report.total    = 5
  store after the flaky run: periods=['2024-01', '2024-02', '2024-04', '2024-05'] steps=121 span=2024-01-01 .. 2024-05-31

  Four months landed; one did not. The run did not abort at the first
  error, because one bad month is not a reason to discard four good ones.
```

And the retry, once the source recovers, is again the same call:

```text
  retry: ingested=['2024-03'] skipped=['2024-01', '2024-02', '2024-04', '2024-05']
```

**Why it matters.** This is the operational core of the whole project. Ingesting
forty years of daily data from a rate-limited upstream over a network you do not
control is a job that fails. The question is never "will it fail" but "what does
failure cost", and the answer here is: at most one period of re-fetching, with no
manual intervention and no bookkeeping to reconcile.

Contrast with the design where progress is tracked externally. A job table says
"periods 1 through 40 done". The store says something else, because the last
commit failed after the row was written. Now you have two answers and no
principled way to pick one. Deriving progress from the store eliminates the
category of bug rather than handling it.

**The traps.**

*The non-monotonic axis*, which is the most instructive thing in the example
and the one genuine wart in the design:

```text
  store after the retry: periods=[...all five...] steps=152 span=2024-01-01 .. 2024-03-31

  all five periods present: True
  time axis monotonic     : False
  Note the span above ends at 2024-03-31: March was appended AFTER May, so
  the time axis is complete but out of order. Appending only ever adds to
  the end. Backfilling a gap in the middle is a rewrite, not an append --
  which is the argument for ingesting periods in order and retrying early.
```

Every period is present. The data is all correct. And the time coordinate reads
`Jan, Feb, Apr, May, Mar` because appends only ever add to the end. Full
discussion [below](#why-appends-leave-a-non-monotonic-axis-when-backfilling).

*`stop_after` counts ingests, not iterations.* Subtle and correct. Resume a
half-finished run with `stop_after=2` and you get two *new* periods, because
skipped periods do not consume the budget. Counting loop iterations instead
would make the parameter behave differently on a fresh store than on a resumed
one, which is exactly the kind of inconsistency that produces a bug report six
months later.

*Failures are recorded, not raised, which means you have to look.* `ingest`
returns normally whether or not anything failed. A caller that ignores the
report will not notice that March is missing. That is the right library
behaviour -- the library should not decide whether a partial ingest is
acceptable -- but it puts the burden on the caller, and a production wrapper
should check `report.failed` and alert on it.

---

## Phase 2 — Derive

Three examples on the middle of the pipeline: turning a stored daily series
into the products people ask for.

### `0201_climatology` — normals and anomalies

Source: [`../../climate-pipeline/examples/0201_climatology.py`](../../climate-pipeline/examples/0201_climatology.py)

**What it teaches.** What a climatological normal is, what an anomaly is, why
the anomaly is the published product, and how to check that your grouping is
right.

It ingests a full year first -- twelve periods, twelve commits -- so that there
is something to average:

```text
  ingested 12 periods, failed 0
  store: dims={'time': 366, 'y': 12, 'x': 12} units='degC'
  span : 2024-01-01 .. 2024-12-31
```

366 days, because 2024 is a leap year.

Then the normal:

```python
normal = climatological_normal(ds, "t2m")
```

```text
  normal dims = ('month', 'y', 'x'), sizes = {'month': 12, 'y': 12, 'x': 12}
  the daily (time, y, x) series collapsed to (month, y, x): 366 steps -> 12
```

The dimension *changed name*, from `time` to `month`, which is xarray telling
you honestly that the output is no longer a time series. It is twelve maps of
what is typical. Trying to `sel(time=...)` on it will fail, and that is a
feature.

Rendered as a seasonal cycle with an ASCII bar chart:

```text
  month  normal degC  seasonal cycle
    Jan        27.80  ####################
    Feb        29.12  ###########################
    Mar        29.85  ##############################
    Apr        29.85  ##############################
    May        29.12  ###########################
    Jun        27.77  ####################
    Jul        26.23  ############
    Aug        24.89  #####
    Sep        24.15  ##
    Oct        24.14  ##
    Nov        24.92  ######
    Dec        26.29  ############

  warmest normal: March at 29.85 degC
  coolest normal: October at 24.14 degC
  annual range  : 5.72 degC -- a tropical grid has a shallow cycle
```

A 5.72-degree annual range is realistic for the tropics. It also has a
consequence that matters for the indices example: with a shallow cycle, a
threshold of 29 or 30 degrees is crossed in some months and not others, which
makes hot-day counts vary dramatically across the year even though the mean
barely moves.

The example is explicit about the sample-size problem rather than quiet about
it:

```text
  Caveat worth stating plainly: with one year of data, 'the normal for
  January' is just this January. A real normal is a 30-year mean (the WMO
  convention), which is what keeps one freak month from defining normal.
  The arithmetic is identical; only the length of the record differs.
```

Then the anomaly, and the self-check:

```text
  anomaly dims = ('time', 'y', 'x'), shape matches the input: True
  units carried through: 'degC', long_name 't2m anomaly'

  grand mean of the anomaly field: -1.93e-16 degC  (zero, to floating point)
  That is not a coincidence -- subtracting each group's own mean forces
  every group to average to zero. It is the arithmetic checking itself.
```

Per month, all twelve within floating-point noise of zero:

```text
  month   mean anomaly   coldest day   warmest day
    Jan       3.96e-16         -2.65          3.06
    Feb       7.66e-17         -2.68          2.77
    Mar      -6.73e-16         -2.31          2.74
    Apr      -2.99e-16         -2.63          2.34
    May       7.97e-16         -2.72          2.45
    Jun      -3.47e-16         -2.62          2.56
    Jul      -4.39e-16         -3.02          2.65
    Aug       1.05e-16         -3.18          2.71
    Sep      -7.25e-16         -2.21          2.35
    Oct      -5.80e-16         -2.32          2.45
    Nov      -2.75e-16         -2.89          2.71
    Dec      -4.24e-16         -2.85          3.33
```

The closing demonstration is the best argument in the example for why anomalies
get published:

```text
  daily spread around the normal: 0.76 degC standard deviation.
  the single most anomalous cell-day: 3.33 degC above normal, on 2024-12-31.
  In absolute terms that day is lost in the pack: the field spans 20.5 to
  33.7 degC across the whole year, so no single reading looks wrong.
  That is the point -- the anomaly finds the outlier the absolute value hides.
```

31 December 2024 was 3.33 degrees above its normal -- more than four standard
deviations of daily spread. In absolute terms it is somewhere in the middle of
a 20.5 to 33.7 range and looks entirely unremarkable. The anomaly finds it; the
absolute value cannot.

**Why it matters.** Anomalies are the single most common form in which climate
information is published, and the reasons are stated compactly at the end of the
example:

```text
  - Comparable across places: +2 degC means the same in Freetown and Oslo,
    while 29 degC does not.
  - Comparable across seasons: a warm January and a warm July are both
    visible once the seasonal cycle is removed.
  - The seasonal cycle is the loudest signal in the raw series and is also
    the least interesting one; subtracting it is how the trend shows up.
  - Downstream models (malaria risk, crop yield) are fitted on departures
    from local normal, so anomalies are what they expect as input.
```

The last point is the operational one. If a malaria transmission model was
fitted on anomalies against a 1991-2020 baseline, feeding it absolute
temperatures produces garbage, and feeding it anomalies against a *different*
baseline produces a systematic bias that is very hard to spot. The baseline is
part of the contract.

**The traps.**

*One year is not a normal.* The example says so, and it bears repeating because
the code does not enforce it. `climatological_normal` will happily compute a
"normal" from a single month. Nothing in the type system, the docstring, or the
runtime distinguishes a thirty-year normal from a one-month one. The record
length is metadata the caller has to carry.

*The baseline period must be recorded.* This project does not record it
anywhere -- not in the anomaly's attrs, not in the STAC summaries. That is a
real omission. A published anomaly field without its baseline is unusable,
because you cannot compare it to anyone else's.

*`groupby("time.month")` pools across years, which is what you want, and
`groupby("time.dayofyear")` does not do what you probably want.* Day-of-year
grouping over a record containing leap years puts 31 December on day 366 in
leap years and day 365 otherwise, which smears the last week of the year. The
standard fix is a smoothed day-of-year climatology, which is more machinery than
this project needs.

*Anomalies centring on zero is a check on the grouping, not on the data.* If
your anomaly field averages to zero, you grouped correctly. It says nothing
about whether the underlying values are right.

---

### `0202_indices` — the products a user asks for

Source: [`../../climate-pipeline/examples/0202_indices.py`](../../climate-pipeline/examples/0202_indices.py)

**What it teaches.** Four indices -- hot days, wet days, monthly totals, and a
standardized rainfall index -- and the two distinctions that govern all of them:
threshold counts versus averages, and intensive versus extensive.

This is the largest example in the project, at 256 lines, and the slowest at
4.2 seconds -- **machine-dependent**. The cost is entirely ingest: it builds
four years of daily data for two variables into two separate stores, which is
48 periods times 2 = 96 icechunk commits.

```python
YEARS = (2021, 2022, 2023, 2024)
REPORT_YEAR = 2024
HOT_THRESHOLD = 29.0
CELL_Y, CELL_X = 6, 6
```

Four years is the minimum that makes a standardized index mean anything at all,
and the example says why:

```text
Ingesting 48 monthly periods -- 2021 through 2024 -- into two
stores, one per variable. Several whole years matter here: an index that says
'unusually dry' needs more than one sample of what usual looks like.

  temperature: dims={'time': 1461, 'y': 12, 'x': 12} units='degC'
  rainfall   : dims={'time': 1461, 'y': 12, 'x': 12} units='mm'
```

1461 days -- four years including one leap day.

Note the structural decision: **one store per variable.** Temperature and
rainfall go into separate icechunk repositories under separate dataset ids.
That mirrors OCS, where each dataset is its own store with its own STAC
collection, and it is the right granularity because the two variables come from
different sources with different update cadences and different failure modes.
A store holding both would have to ingest both or neither.

#### Index 1: hot days

```python
hot = hot_days(temperature, threshold=HOT_THRESHOLD)
hot_area = hot.mean(dim=("y", "x"))
monthly_mean_t = temperature["t2m"].resample(time="1ME").mean().mean(dim=("y", "x"))
```

Printing the monthly mean beside the count is the whole rhetorical move:

```text
      month  mean degC  hot days   of
   2024-Jan      27.80       5.6   31
   2024-Feb      29.12      15.8   29
   2024-Mar      29.85      23.2   31
   2024-Apr      29.85      22.5   30
   2024-May      29.12      16.5   31
   2024-Jun      27.77       5.1   30
   2024-Jul      26.23       0.4   31
   2024-Aug      24.89       0.0   31
   2024-Sep      24.15       0.0   30
   2024-Oct      24.14       0.0   31
   2024-Nov      24.92       0.0   30
   2024-Dec      26.29       0.3   31
```

March and April: identical means to two decimals (29.85), counts of 23.2 and
22.5. February and May: identical means (29.12), counts of 15.8 and 16.5. The
example draws the conclusion:

```text
  Two months can share a mean and differ completely in hot-day count, because
  a count is sensitive to the shape of the distribution and a mean is not.
  The count is the thing a health ministry can plan around.
```

Look also at the nonlinearity. From January to March the mean rises 2.05
degrees, from 27.80 to 29.85, and the hot-day count rises from 5.6 to 23.2 --
more than quadrupling. A threshold count near the middle of a distribution is
extremely sensitive to small shifts in the mean, which is exactly why threshold
indices are the right way to communicate heat risk and exactly why the threshold
choice matters so much.

Note that the whole-record figure divides carefully:

```python
print(f"  whole record: {float(hot.sum()) / (hot.sizes['y'] * hot.sizes['x']) / len(YEARS):.0f} hot days")
```

```text
  whole record: 89 hot days
  in an average year at an average cell, against a 29.0 degC line.
```

Summing the count over every cell and every month, then dividing by the number
of cells and the number of years, gives hot days per cell per year. Getting that
normalization wrong is easy and produces a number that is off by a factor of 144.

#### Index 2: wet days

The same construction on rainfall, with a second column showing what happens if
you drop the threshold to zero:

```python
wet = wet_days(rainfall, threshold=1.0)
drizzle = wet_days(rainfall, threshold=0.0).mean(dim=("y", "x"))
```

```text
      month  wet days >=1mm  days with any trace
   2024-Jan             1.5                 31.0
   2024-Feb             1.3                 29.0
   2024-Mar             7.7                 31.0
   2024-Apr            14.6                 30.0
   2024-May            21.7                 31.0
   2024-Jun            25.7                 30.0
   2024-Jul            29.4                 31.0
   2024-Aug            30.4                 31.0
   2024-Sep            28.6                 30.0
   2024-Oct            26.4                 31.0
   2024-Nov            20.5                 30.0
   2024-Dec            15.4                 31.0
```

The "any trace" column is *every day of every month*, because `wet_days` uses
`>=` and the threshold is 0.0, so a day with exactly zero rain counts. That is
a slightly unfair comparison and it makes the point brutally: with a zero
threshold, "rainy days per year" is 365 everywhere, forever, and the index
carries no information at all.

```text
  On average 11.8 days a month register something but less than 1 mm.
  1 mm is the conventional wet-day cutoff because below it a reading is
  indistinguishable from dew, gauge wetting, or a satellite retrieval's noise
  floor. Count those and 'rainy days per year' measures instrument
  sensitivity rather than climate -- and stops being comparable between
  places, which is the entire purpose of an index.
```

That last clause is the operational argument. Two stations with different
instruments have different noise floors. Count sub-millimetre days and you are
comparing instruments; count days over 1 mm and you are comparing climates.

#### Index 3: the intensive/extensive demonstration

The example computes all four combinations -- rainfall summed and averaged,
temperature summed and averaged -- and reads the wrong ones out loud.

```text
      month   total mm  mean mm/day
   2024-Jan       14.6         0.47
   2024-Feb       14.2         0.49
   2024-Mar       78.1         2.52
   2024-Apr      151.7         5.06
   2024-May      219.7         7.09
   2024-Jun      260.2         8.67
   2024-Jul      301.1         9.71
   2024-Aug      306.8         9.90
   2024-Sep      289.0         9.63
   2024-Oct      265.6         8.57
   2024-Nov      210.1         7.00
   2024-Dec      157.2         5.07
```

```text
  Rainfall is EXTENSIVE -- it accumulates. 2024-Aug received 307 mm; its
  daily mean of 9.90 mm/day is a true number that answers no question
  anyone asked, and it shrinks if you lengthen the month while the rain that
  fell stays the same.
  Temperature is INTENSIVE -- summing it gives 862 'degC' for the month,
  a quantity with no physical meaning. But it is a float, it carries a unit
  attribute, it plots, and nothing in xarray or zarr will ever object. That
  is what makes the intensive/extensive mix-up a silent error, not a crash.
```

862 degrees Celsius. It is the sum of 31 daily means around 27.8 each, and it
is meaningless -- there is no physical process for which "the total temperature
of March" is a quantity. And nothing anywhere in the stack objects. It is a
float64, it has a `units` attribute saying `degC`, `to_zarr` will write it, and
a chart will render it.

The January/February pair is worth noticing too: 14.6 mm total in January
against 14.2 in February, but the daily means are 0.47 and 0.49 -- February
comes out *higher* on the mean and *lower* on the total, purely because February
has 29 days instead of 31. That is the "a mean quietly makes a 28-day month look
like a 31-day one" effect, visible in real numbers.

#### Index 4: a standardized index

Read at a single grid cell, because a map mean would average the local
variability away:

```text
      month   total mm     SPI  reading
   2024-Jan        5.1   -0.26  near normal
   2024-Feb       22.2    1.63  wetter than usual
   2024-Mar       72.8   -0.87  near normal
   2024-Apr      177.1    0.58  near normal
   2024-May      246.1    0.98  near normal
   2024-Jun      219.8   -1.59  drier than usual
   2024-Jul      296.8   -0.06  near normal
   2024-Aug      281.2    0.05  near normal
   2024-Sep      297.1   -0.65  near normal
   2024-Oct      272.1   -0.96  near normal
   2024-Nov      224.7    1.31  wetter than usual
   2024-Dec      152.7    0.08  near normal
```

June: 220 mm, scored -1.59, flagged dry. February: 22 mm, scored +1.63, flagged
wet. Ten times less rain, and it is the wetter month.

The example then reads the two extremes of the whole four-year record:

```text
  - 2021-Apr scored -1.71: that month was 1.71 standard
    deviations DRIER than a typical April at this cell, receiving
    56 mm against a April average of 146 mm.
  - 2022-Jan scored 1.70: 1.70 standard deviations
    WETTER than a typical January, receiving 27 mm against a
    January average of 8 mm.
  Notice that the two are not comparable in millimetres and are perfectly
  comparable in SPI. That is the whole trick: each month is judged against
  its own season, so a wet dry-season month can outscore a wet monsoon one.
```

56 mm scores -1.71 and 27 mm scores +1.70. In millimetres, 56 is twice 27; in
SPI they are nearly opposite. This is the clearest demonstration in the project
of what standardization buys.

Both extremes sit at about 1.7, which is not a coincidence: it is
`sqrt(4 - 1) = 1.732`, the ceiling imposed by having four samples per calendar
month.

**Why it matters.** Every index here is a small reduction over the stored
series, and that is what a service publishes. The stored daily field is
infrastructure; the index is the product. Understanding which reduction belongs
to which quantity is the difference between a service that is useful and one
that is confidently wrong.

**The traps.**

*The intensive/extensive mix-up produces no error.* Stated three times in the
example because it deserves it.

*Threshold conventions are load-bearing.* 1 mm for a wet day. 30 degrees, or
35, or the local 90th percentile, for a hot day. Different choices give
different, incomparable answers, and the choice belongs in the published
metadata.

*Sample size caps a standardized index.* Discussed [above](#spi_like).

*Area means hide local variability.* The example reads SPI at one grid cell and
says why: averaging SPI over the grid averages away the very thing an index
exists to detect. That is a general property of anomaly-type products -- spatial
means of anomalies tend toward zero because the anomalies have opposite signs in
different places.

*`resample(time="1ME")` labels months by their last day.* Every table in the
example formats `str(stamp)[5:7]` to recover the month number rather than
assuming the label is the first of the month. Getting this wrong shifts your
whole series by a month.

---

### `0203_pyramid` — multiscale levels, and why a viewer needs them

Source: [`../../climate-pipeline/examples/0203_pyramid.py`](../../climate-pipeline/examples/0203_pyramid.py)

**What it teaches.** How a multiscale pyramid is built by repeated 2 x 2 mean
coarsening, what it costs in storage, that the coordinates coarsen with the
data, and why a map viewer cannot work without one.

```python
GRID = 16
LEVELS = 4
```

A 16 x 16 grid coarsens exactly four times to 2 x 2, which makes every level a
clean power of two and the arithmetic checkable by hand.

The sizes:

```text
  level     y     x     cells    size KB  vs level 0
      0    16    16       256      120.0      1.000x
      1     8     8        64       30.0      0.250x
      2     4     4        16        7.5      0.062x
      3     2     2         4        1.9      0.016x

  time is untouched: 60 steps at every level -- only space coarsens.
  whole pyramid: 159.4 KB against 120.0 KB for level 0 alone,
  an overhead of 33 percent. The series 1 + 1/4 + 1/16 + ...
  converges to 4/3, so a full pyramid never costs more than a third extra.
```

Thirty-three percent is the number to remember. A complete pyramid, however
deep, costs at most a third more storage than the base level, because
`sum(1/4^k)` converges to 4/3. That is a very cheap price for turning a
zoomed-out map from a full-grid read into a tiny one.

Note the "time is untouched" line: 60 timesteps at every level. Only `y` and
`x` are coarsened, because zoom is a spatial question. A viewer zoomed out over
the whole country still wants a specific day.

The coordinates coarsen too, which is the part people forget:

```text
  level 0 first y centres: [10.0, 9.7933, 9.5867, 9.38]
  level 1 first y centres: [9.8967, 9.4833]
  mean of the first pair : 9.8967
  level 0 cell height: 0.2067 deg
  level 1 cell height: 0.4133 deg  (twice as tall)
```

The first level-1 centre, 9.8967, is exactly the mean of the first two level-0
centres, 10.0 and 9.7933. That is what keeps the coarse grid geographically
true: a coarse cell centre sits between the fine centres it covers, and its
cell is twice as tall. A pyramid that coarsened the data but kept the original
coordinates would render every level shifted.

The arithmetic verified by hand:

```python
block = levels[0]["t2m"].isel(time=0, y=slice(0, 2), x=slice(0, 2))
expected = float(block.mean())
actual = float(levels[1]["t2m"].isel(time=0, y=0, x=0))
```

```text
  level 0 block values : [28.843, 28.6113, 29.0484, 27.4411]
  their mean           : 28.485955
  level 1 cell value   : 28.485955
  equal to 1e-12       : True

  and two levels down, one cell covers a 4x4 block of level 0:
  mean of the 4x4 block: 28.119441
  level 2 cell value   : 28.119441
  equal to 1e-12       : True
```

The two-levels-down check is the important one, because it tests a property
that is *not* generally true:

```text
  A mean of means is only the mean of the whole when the blocks are equal
  in size, which they are here. On a grid with an odd dimension the last
  row is trimmed instead of being folded in at half weight:
  pyramid over y=15: next level is y=7 (boundary='trim')
```

Level 2 is a mean of four level-1 values, each of which is a mean of four
level-0 values. That equals the mean of all sixteen level-0 values *only
because the blocks are equal in size*. Coarsen a 15-row grid with
`boundary="pad"` instead of `"trim"` and the last coarse row would be a mean
over one real row and one NaN, and every subsequent level would inherit the
distortion.

**Why it matters.** The byte argument is the whole justification:

```text
  A viewer showing the whole country in a 512-pixel-wide panel has no use
  for a 4096-wide grid: 63 of every 64 values it downloads get averaged
  away before a pixel is painted. Reading level 3 instead moves 1/64 of
  the bytes and produces the same picture.
  Doing the averaging server-side per request costs the same read every
  time and does not cache. Precomputing it once, at ingest, turns a
  zoomed-out map into a small static read -- which is why every tiled map
  system, from raster tiles to COG overviews, is built this way.
```

Two separate arguments there. The first is bandwidth: a client asking for a
4096-wide grid to paint 512 pixels wastes 63/64 of what it downloads. The
second, and the one people miss, is **caching**. A server that downsamples on
demand does the full read on every request and produces a response that is a
function of the request parameters, so it caches badly. A precomputed level is a
static object with a stable URL. It caches perfectly, at every layer -- CDN,
browser, proxy.

The example closes by describing the GeoZarr shape it is aiming at:

```text
This is exactly the GeoZarr multiscale convention open-climate-service
writes. In the store the levels become sibling groups under one dataset:
  temperature.icechunk/
    0/  t2m  (time, y, x)   full resolution
    1/  t2m  (time, y/2, x/2)
    2/  t2m  (time, y/4, x/4)
  with a 'multiscales' attribute on the root naming the levels and the
  factor between them, so a client can pick a level without guessing.
  pyramid_levels() computes the arrays; example 0301 writes the attributes
  that make them a valid GeoZarr.
```

**The traps.**

*A mean is not always the right coarsening.* It is right for temperature and
for rainfall *rate*. It is wrong for a categorical field -- land cover, a
drought category -- where the correct downsample is a mode or a nearest-neighbour
pick, because the mean of "forest" and "urban" is not a land cover class. It is
also wrong for a maximum-type field, where you want the max of the block, not the
mean, because a pyramid of means makes extremes disappear as you zoom out. This
project's `pyramid_levels` hard-codes `.mean()`, which is fine for its two
variables and would need a parameter for anything else.

*`levels` is a maximum, not a promise.* The loop breaks when a dimension drops
below 2. Ask for eight levels on a 16 x 16 grid and get five.

*The pyramid is computed, not stored.* `pyramid_levels` returns a list of
in-memory datasets. Nothing writes them into the store as GeoZarr multiscale
groups. That is one of the project's deliberate omissions, discussed under
[how this maps to OCS](#how-this-maps-to-open-climate-service).

*Trimming loses data at the edge.* A 4096-row grid coarsened four times with
`boundary="trim"` at each step is fine, because 4096 is divisible by 16. A
4095-row grid loses a row at level 1, then two more rows' worth at level 2, and
so on -- and the loss is always at the same edge, so a deep pyramid over an
awkwardly sized grid can be visibly short at the south. Padding the base grid to
a power of two before building the pyramid is the usual answer.

---

## Phase 3 — Publish

Two examples on the right-hand end of the pipeline: making the store placeable
and making it discoverable.

### `0301_geozarr` — putting a grid of numbers somewhere on Earth

Source: [`../../climate-pipeline/examples/0301_geozarr.py`](../../climate-pipeline/examples/0301_geozarr.py)

**What it teaches.** Every key in the GeoZarr root attributes, taken apart one
at a time: the affine transform coefficient by coefficient, pixel registration
verified numerically against the coordinate arrays, why `stepY` is negative,
the array-order trap demonstrated with real values, the bounding box, the CRS,
and finally writing the attributes onto the store and reading them back.

The grid is deliberately not square:

```python
# A deliberately non-square grid: on a square grid an axis-order bug is
# invisible, because the wrong shape happens to still fit.
NY = 12
NX = 16
```

It opens by showing what a Zarr store carries on its own, which is nothing:

```text
A zarr store is an array of numbers in chunks. It is not a map.
  stored variable: t2m('time', 'y', 'x') shape (60, 12, 16)
  root attrs on the freshly written store: {}
  Nothing there says which CRS, which way is up, or where cell [0, 0] sits.
  The y/x coordinate arrays hold degrees, but a tile server rendering a
  chunk will not read a coordinate array to work out geography -- it wants
  one affine transform and one CRS, declared at the root.
```

`{}`. An empty dictionary. The store holds 60 x 12 x 16 correct temperature
values and has no idea where they are.

The six keys:

```text
  spatial:transform      = [0.21333333333333293, 0.0, -13.606666666666666, 0.0, -0.2818181818181813, 10.14090909090909]
  spatial:dimensions     = ['y', 'x']
  spatial:shape          = [12, 16]
  spatial:bbox           = [-13.606666666666666, 6.759090909090915, -10.193333333333339, 10.14090909090909]
  proj:code              = EPSG:4326
  zarr_conventions       = [{'name': 'geozarr', 'version': '0.4'}]
```

Taken apart:

```text
-- spatial:transform --
  Order is [stepX, rotX, originX, rotY, stepY, originY]:
    stepX   = +0.213333  degrees of longitude per column
    rotX    = +0.000000  row rotation, zero for an axis-aligned grid
    originX = -13.606667  west edge of column 0
    rotY    = +0.000000  column rotation, zero here too
    stepY   = -0.281818  degrees of latitude per row -- NEGATIVE
    originY = +10.140909  north edge of row 0
```

#### Pixel registration, checked rather than trusted

This is the best part of the example, because it verifies a claim instead of
making one:

```python
x = np.asarray(ds["x"].values, dtype="float64")
assert np.isclose(origin_x, x[0] - step_x / 2)
assert np.isclose(origin_y, y[0] - step_y / 2)
far_x = origin_x + step_x * ds.sizes["x"]
assert np.isclose(far_x, x[-1] + step_x / 2)
```

```text
-- pixel registration, checked against the coordinate arrays --
  The coordinates are cell CENTRES. The transform origin is a cell EDGE:
    x[0] (centre of column 0) = -13.500000
    origin_x                  = -13.606667
    x[0] - stepX / 2          = -13.606667  <- matches origin_x
  The offset is exactly half a cell: 0.106667 = stepX/2 = 0.106667
  Walking the full width lands on the far edge: origin_x + stepX*nx = -10.193333
  which is half a cell past the last centre x[-1] = -10.300000.
  Treat the origin as a centre instead and every rendered tile shifts by
  half a pixel -- small enough to ship, big enough to misplace a clinic.
```

Three checks, all with real `assert` statements that would fail the run if the
transform were wrong. The origin is half a cell west of the first centre.
Walking `nx` steps from the origin lands half a cell east of the last centre.
The full width is `nx * stepX`, not `(nx - 1) * stepX`.

The last line is the argument for caring. Half a cell on a country-scale grid
with 0.21-degree cells is about eleven kilometres of longitude at this latitude.
"Small enough to ship, big enough to misplace a clinic" is precisely the failure
profile: it will pass review and it will be wrong on the ground.

#### Why stepY is negative

```text
  y[0] = +10.000000 (north)   y[-1] = +6.900000 (south)
  Row 0 is the NORTHERNMOST row, so walking down rows walks south, so the
  y step is negative. That is the north-up convention every raster renderer
  assumes. A positive stepY on this store would draw the country upside down.
```

This is where `orient_north_up` from phase 1 pays off. The source was south-up;
normalization flipped it; the transform can therefore assume north-up and emit a
negative step. If normalization had not run, this transform would be a
confident, precise lie.

#### The array-order trap

The example does not describe the transposition bug, it *performs* it:

```python
plane = np.asarray(ds["t2m"].isel(time=0).values, dtype="float64")
wrong = plane.ravel().reshape(NX, NY)
row, col = 3, 5
flat_index = row * NX + col
```

```text
  spatial:dimensions = ['y', 'x']
  spatial:shape      = [12, 16]  (ny=12 rows, nx=16 columns)
  A client reads these positionally: first entry indexes the slow axis.
  Correct read, shape (12, 16): cell [3, 5] = 27.107 degC
  x-first read, shape (16, 12): cell [3, 5] = 27.400 degC
  The real cell [3, 5] is element 53 of the buffer, which the
  x-first reader places at [4, 5] instead.
  Same bytes, same chunks, different geography: only the stride the client
  walks the buffer with changed, and every pixel landed somewhere else.
  On a square grid (12x12) the transposed shape still fits, so the bug
  renders silently -- it survives until someone compares a tile to a map.
```

Element 53 of the flat buffer is `[3, 5]` when the row stride is 16 and `[4, 5]`
when it is 12. Nothing was corrupted; the same bytes were walked with a
different stride, and every value landed at the wrong latitude and longitude.

And the last two lines are why the example uses a 12 x 16 grid. On a square
grid the wrong shape still fits, so there is no error, no exception, no shape
mismatch -- just a transposed picture that nobody notices until they overlay a
coastline.

#### Persisting the attributes

```python
session: Any = repo.writable_session("main")
group = zarr.open_group(session.store, mode="a")
group.attrs.update(attrs)
session.commit("add geozarr root attributes")
```

```text
-- persisting the attributes onto the zarr group root --
  wrote 6 keys onto the root group and committed them
  read back from a fresh session: 6 keys
    spatial:shape     -> [12, 16]
    spatial:transform -> [0.2133, 0.0000, -13.6067, 0.0000, -0.2818, 10.1409]
    proj:code         -> EPSG:4326
  xarray surfaces the same keys as ds.attrs: ['proj:code', 'spatial:bbox', 'spatial:dimensions'] ...
```

Two things to notice. It drops to the `zarr` API -- `zarr.open_group(session.store, mode="a")`
-- rather than going through xarray, because xarray's `to_zarr` writes whole
datasets and here the goal is to touch only the group attributes. And **the
attribute write is its own commit**, just like an ingest. Metadata changes are
versioned exactly like data changes, which means you can point at the snapshot
in which a dataset became publishable.

The read-back is from a *fresh* session, not the one that wrote, which proves
the attributes were persisted rather than merely set in memory.

**Why it matters.** Without these six keys a Zarr store is unrenderable by
anything that does not already know what it is. With them, a tile server that
has never seen this store can place it, orient it, and draw it. That is the
difference between a dataset and a product.

**The traps.**

*Array order.* Performed rather than described, above.

*Pixel registration.* Half a cell, silent, permanent.

*A positive `stepY`.* Upside down, silent.

*The transform assumes a regular grid.* `step_x = x[1] - x[0]` and nothing
checks that `x[2] - x[1]` is the same. A Gaussian grid, or any irregularly
spaced one, produces a transform that is right at the origin and progressively
wrong across the grid. There is no validation for this in the library.

*Rotation is assumed to be zero.* Correct for every grid this project produces
and not correct in general.

---

### `0302_stac` — the document a client discovers the dataset through

Source: [`../../climate-pipeline/examples/0302_stac.py`](../../climate-pipeline/examples/0302_stac.py)

**What it teaches.** The STAC Collection document, field by field, with each
extent checked against the store it claims to describe.

```python
PUBLISHED_AT = datetime(2024, 7, 1, 12, 0, tzinfo=UTC)
DATASET_ID = "temperature-daily-sle"
ZARR_HREF = "https://climate.example.org/zarr/temperature-daily-sle"
```

The pinned timestamp makes the emitted JSON byte-identical on every run, which
is the same determinism concern as the crc32 seed in `sources.py`.

The dataset id is worth a look: `temperature-daily-sle`. Variable, cadence,
country. That is the OCS naming pattern, and the reason it is structured rather
than a UUID is that it appears in a URL that humans read and type.

The example ingests six months and prints the full document, which is quoted in
[the `stac_collection` section above](#stac_collection). Then it walks the
fields.

#### Identity

```text
  id          = temperature-daily-sle
  title       = Daily 2m temperature, Sierra Leone
  license     = proprietary
  The id is the primary key of the whole service: it names the store on
  disk, and it is the {id} in /stac/collections/{id}, so a client that
  has seen the id once can come back to it forever (temperature-daily-sle).
```

The id doing triple duty -- filesystem path via `store_path`, collection id,
URL segment -- is the reason `store_path` exists as a function.

`license: "proprietary"` is the STAC vocabulary's way of saying "not one of the
SPDX identifiers". A real deployment would put an actual licence here, and it
matters: a catalogue user filtering for openly licensed data will not see this
collection.

#### Spatial extent, checked against GeoZarr

```python
stac_bbox = list(collection["extent"]["spatial"]["bbox"][0])
geo_bbox = list(geozarr_attrs(ds)["spatial:bbox"])
assert stac_bbox == geo_bbox
```

```text
  [-13.606666666666666, 6.759090909090915, -10.193333333333339, 10.14090909090909]
  It is a LIST of boxes: a collection may be scattered over several
  footprints, so even a single-grid dataset nests its box one level deep.
  GeoZarr spatial:bbox = [-13.606666666666666, 6.759090909090915, -10.193333333333339, 10.14090909090909]
  identical to the STAC bbox: True
  That is the point: catalogue search and rendered tile derive the box from
  the same transform, so a hit in the catalogue is really over that ground.
```

An `assert`, not a claim. The two boxes are byte-identical because both come
from `bounding_box(ds)`, which comes from `grid_transform(ds)`. There is one
source of geometry in the whole library and everything else derives from it.

Why that matters operationally: a catalogue search is a spatial query against
the declared bbox. If the declared bbox and the rendered footprint disagree,
searches return datasets that do not cover the area of interest, or miss ones
that do. Deriving both from the same function makes the discrepancy impossible
rather than unlikely.

#### Temporal extent

```python
assert interval == temporal_extent(ds)
assert interval[0].startswith(str(stamps[0].date()))
assert interval[1].startswith(str(stamps[-1].date()))
```

```text
  ['2024-01-01T00:00:00Z', '2024-06-30T00:00:00Z']
  store's real time axis: 2024-01-01 .. 2024-06-30 (182 days)
  temporal_extent(ds)   = ['2024-01-01T00:00:00Z', '2024-06-30T00:00:00Z']
  interval matches the store: True
  Nothing here is declared by hand. The interval is read back off the
  committed time coordinate, so it cannot drift from what the store holds --
  re-publish after the next month lands and the end date moves by itself.
```

182 days is 31 + 29 + 31 + 30 + 31 + 30, January through June of a leap year.

The "moves by itself" property is the whole design. There is no configuration
file with a start and end date. There is no migration to run when the series
grows. Re-running `stac_collection(ds, ...)` after the next ingest produces a
document with the new end date, and no human had to remember.

#### Summaries

```text
  t2m: 2 metre temperature in degC, 23.6641 .. 33.9401
  summaries.proj:code = ['EPSG:4326']
  Units are the half of the contract that data alone cannot carry: 27.4 is
  a plausible temperature in degC and an absurd one in K. The min/max
  let a client build a colour ramp, or notice a source went wrong, before
  downloading a single chunk.
```

The colour ramp point is the practical one. A viewer that has to fetch data
before it can choose a colour scale renders a flash of wrongly coloured tiles
on first paint. With min and max in the metadata it can build the ramp from the
collection document alone.

The "notice a source went wrong" point is the operational one. A temperature
collection whose summary says `-273.15 .. 3.4e38` has a fill-value problem, and
you can see that from the catalogue without opening the store.

#### The asset and the discovery loop

```text
  href  = https://climate.example.org/zarr/temperature-daily-sle
  type  = application/vnd+zarr
  roles = ['data']
  This is the door back to the data. Everything above is metadata a client
  can index and search; this one field says where to actually open the store.
```

```text
-- the discovery loop --
  1. GET /stac/collections            -> which datasets does this instance hold?
  2. GET /stac/collections/temperature-daily-sle
     -> this document: extent, variables, units, asset href
  3. open assets.zarr.href            -> the store, already placed by GeoZarr
  No step required knowing that the store is icechunk, chunked 30 days at a
  time, or ingested one month per commit. That is what a discovery layer buys.
```

Three requests from "I have never seen this service" to "I am reading the
array". Nothing in the loop mentions icechunk, chunk sizes, or the ingest
schedule. That is the abstraction boundary a catalogue provides, and it is the
reason the internals of this project can change without breaking clients.

**Why it matters.** Publishing is the stage that turns a correct store into a
usable one. A catalogue entry is what lets somebody else find your data, and
deriving its contents from the data is what keeps the entry true over time.

**The traps.**

*The double nesting.* `bbox` and `interval` are lists of lists. The single most
common STAC mistake.

*`links` is empty.* A conformant catalogue needs `self`, `root`, and `parent`
links, and those depend on the serving URL, which this project does not have.
A STAC validator would flag it.

*Computing min and max loads the entire array.* Instant at this scale, a full
archive scan at production scale. Discussed under
[`stac_collection`](#stac_collection).

*The interval is first-and-last by position, not min-and-max by value.* On a
store whose time axis went non-monotonic during a backfill, the reported end is
the last *appended* timestamp rather than the latest date. See
[the non-monotonic axis](#why-appends-leave-a-non-monotonic-axis-when-backfilling).

*`stac_version: "1.0.0"` is pinned in the code.* STAC 1.1 exists. Nothing here
tracks the version it emits against the version it claims.

---

## Phase 4 — The whole thing

Two examples that stop taking stages apart and run them together.

### `0401_full_pipeline` — source to product in one pass

Source: [`../../climate-pipeline/examples/0401_full_pipeline.py`](../../climate-pipeline/examples/0401_full_pipeline.py)

**What it teaches.** That the stages compose. Everything before this example
demonstrated one stage in isolation; the point of a service is that a messy
source goes in one end and a discoverable, placed, versioned product comes out
the other, with nothing hand-maintained in between.

```python
DATASET_ID = "temperature-daily-sle"
MONTHS = 12
NY = 16
NX = 16
HOT_THRESHOLD = 30.0
PUBLISHED_AT = datetime(2025, 1, 15, 9, 0, tzinfo=UTC)
```

Twelve months, a 16 x 16 grid, six stages, about a second of wall clock --
**machine-dependent**.

The structure is a banner function and six blocks:

```python
def stage(number: int, title: str) -> None:
    print(f"\n{'=' * 74}")
    print(f"STAGE {number} -- {title}")
    print("=" * 74)
```

The [full walkthrough is the next section](#the-pipeline-end-to-end); what
follows here is why this example exists as its own thing rather than as a
summary of the other nine.

**Why it matters.** Three properties are only visible when the stages run
together, and this example exists to make them visible.

*The stages share no state except the store.* Stage 4 does not receive anything
from stage 2 except a dataset it opened from the repository. Stage 6 does not
receive anything from stage 4 -- it re-reads `ds` and derives the extents from
it. There is no pipeline object, no context, no config threaded through. Each
stage's input is the committed store, which means each stage can be run
separately, later, on a different machine, and it will do the same thing.

*Every published number is derived.* The example closes on this:

```text
- Messy source in, discoverable placed versioned product out
- Every extent published above was read back off the store, never declared
- The store is the record: history, resume, and reproduction all read from it
```

Nothing in the six stages contains a hand-typed extent, a hand-typed date
range, or a hand-typed shape. All of it comes out of `ds`.

*The history is a first-class output.* Stage 3 exists purely to show that the
ingest left an audit trail, and it demonstrates reading an old snapshot back:

```text
  reading the oldest ingest snapshot back: 31 days
  reading the branch tip:                  366 days
  Same store, two points in its history -- useful when a published figure
  has to be reproduced months after the series it came from grew.
```

That is time travel on a data product. A figure published in February can be
regenerated in December from the exact snapshot it was built on, even though
the series has grown by ten months since. Very few storage systems give you that
for free, and it falls straight out of committing per period.

**The traps.**

*The snapshot count is 14, not 13.* The summary line reads:

```text
  snapshots          14 (1 init + 12 ingests + 1 publish)
```

Stage 3 counted 13 (`1 + 12`), and stage 6's GeoZarr attribute write added one
more. The example recounts `repo.ancestry()` at the end rather than reusing the
earlier number. Anything that caches a count across a stage that commits will be
wrong.

*The pyramid is computed and dropped.* Stage 5 builds four levels, prints their
sizes, and never writes them. That is honest about what the library does and it
means the store this example produces is not a complete GeoZarr multiscale
dataset.

*One dataset, one variable.* This example is temperature only. `0402` is the
one that shows the pipeline is not secretly specialized.

---

### `0402_second_dataset` — what generalizes, and what must not

Source: [`../../climate-pipeline/examples/0402_second_dataset.py`](../../climate-pipeline/examples/0402_second_dataset.py)

**What it teaches.** That `normalize`, `ingest`, and `publish` do not care which
variable they are carrying -- and that the *derivations* on top of them very
much do.

The framing is the important bit:

```text
The instance already holds the temperature dataset that example 0401 built.
  temperature-daily-sle: {'time': 366, 'y': 16, 'x': 16} in degC
Now a second source arrives. Nothing in the pipeline is told it is different.
```

#### Normalize: a different unit problem, the same fix

```text
  source : tp('time', 'lat', 'lon') in 'm', max 0.0588
  clean  : tp('time', 'y', 'x') in 'mm', max 58.7643
  Metres of rainfall is what the reanalysis publishes and nobody thinks in.
  The values are scaled by 1000 AND the units attribute is rewritten; doing
  only the first is how a store ends up labelled 'm' while holding mm.
```

0.0588 m is 58.8 mm -- a heavy day's rain. The first number is correct and
unreadable; the second is correct and immediately meaningful.

Then the measurement that explains everything downstream:

```python
wet_zeros = float((np.asarray(clean["tp"].values) == 0.0).mean())
dry_zeros = float((np.asarray(dry_season["tp"].values) == 0.0).mean())
```

```text
  Note the shape of the data: 95% of January's cell-days are exactly zero,
  against 3% of July's. Rainfall is zero-inflated and strongly seasonal;
  temperature is neither, and that difference is what makes the derived
  products further down diverge.
```

Ninety-five percent zeros in January. A mean over that distribution is dominated
by the absence of rain and tells you almost nothing about the rain that did
fall. Temperature has no equivalent -- there is no such thing as a day with zero
temperature -- and that structural difference is the reason the two variables
get different products.

#### Ingest: byte-for-byte the same code path

```text
  ingest(repo, periods, fetch_precipitation) -> {'time': 366, 'y': 16, 'x': 16}
  time span : 2024-01-01 .. 2024-12-31 (366 days)
  snapshots : 13 (1 init + 12 ingests), one commit per month
  units     : mm, mean 6.23 mm/day
  Byte for byte this is the temperature code path. Streaming ingest, resume,
  and versioning are properties of the pipeline, not of the variable.
```

The only difference between the two calls is which `Fetcher` was passed. That
is the payoff of the one-function source protocol.

#### Derive: where it stops generalizing

```text
    month      1      2      3      4      5      6      7      8      9     10     11     12
    total     16     15     80    151    219    261    296    311    292    268    215    156   mm accumulated
    mean     0.5    0.5    2.6    5.0    7.1    8.7    9.5   10.0    9.7    8.6    7.2    5.0   mm per day
```

```text
  Temperature is INTENSIVE: it has a value at every instant, and averaging
  two days gives a temperature. Rainfall is EXTENSIVE: it is an amount that
  accumulates, and the honest monthly figure is the sum of the days.
  The mean is not wrong arithmetic, it is the wrong question. A reservoir
  fills from the 311 mm that fell, not from 10.0 mm/day,
  and a mean also quietly makes a 28-day month look like a 31-day one.
```

"The mean is not wrong arithmetic, it is the wrong question" is the sentence to
take away from this project.

Wet days, the count that a total cannot express:

```text
    month      1      2      3      4      5      6      7      8      9     10     11     12
    days     1.6    1.5    7.9   14.6   21.8   25.7   29.4   30.4   28.5   26.3   20.9   15.4
```

```text
  Two months can share a total and differ completely -- 200 mm over 20 days
  is a growing season, 200 mm over 3 days is a flood -- so the count is its
  own product, not a summary of the total.
```

#### The one-year SPI result

```text
  spi_like over one year -> (12, 16, 16), finite values: 0
  All NaN, and correctly so: with one year, each calendar month has exactly
  one sample, its standard deviation is zero, and 'unusual compared to what?'
  has no answer. The library refuses rather than dividing by zero.
```

Zero finite values out of 3072. That is the `safe_std` guard doing its job, and
it is the right answer rather than a failure. Over three years:

```text
    month      1      2      3      4      5      6      7      8      9     10     11     12
    spi    +1.29  -0.22  +1.31  +1.16  +1.04  +1.39  -1.40  +1.13  +0.38  +0.41  +0.87  -0.87
  Range over the whole cube: -1.41 .. +1.41.
```

The `+/-1.41` ceiling is `sqrt(3 - 1)`, as discussed under [`spi_like`](#spi_like).
Note also how many months score above +1.0: with three samples the index is
extremely coarse, and "wetter than usual" stops being a meaningful statement.

#### Publish: a second collection and a catalogue

```text
  id                       = precipitation-daily-sle
  extent.spatial.bbox      = [-13.6067, 6.7967, -10.1933, 10.1033]
  extent.temporal.interval = ['2024-01-01T00:00:00Z', '2024-12-31T00:00:00Z']
  summaries.variables.tp   = mm, 0.0 .. 72.386
  assets.zarr.href         = https://climate.example.org/zarr/precipitation-daily-sle
  Both collections share a bbox (True) because both grids share a
  transform. They differ in id, units, and value range -- nothing structural.
```

And the catalogue listing, which is what an instance's `/stac/collections`
endpoint would return:

```text
-- GET /stac/collections --
  id                       variable  units  interval                 range
  temperature-daily-sle    t2m       degC   2024-01-01..2024-12-31   19.7 .. 33.7
  precipitation-daily-sle  tp        mm     2024-01-01..2024-12-31   0.0 .. 72.4
  Both intervals were read off their own stores: 2024-01-01 onward.
  That listing is the whole catalogue of this instance: two datasets, one
  pipeline, one document shape, discovered without knowing anything inside.
```

Two rows. That is a climate service.

**Why it matters.** The generalization test is the one that tells you whether
you built a pipeline or a script. `normalize`, `ingest`, `stac_collection` were
called unchanged. `monthly_total` versus `climatological_normal` was a choice
the caller had to make correctly, and nothing in the library could have made it
for them.

That split -- machinery that generalizes, arithmetic that does not -- is a good
way to think about where to put effort. The machinery is worth abstracting
because it is the same everywhere. The arithmetic is worth writing out because
the wrong choice is silent.

**The traps.**

*The min of the rainfall summary is 0.0.* Correct -- there are days everywhere
with no rain -- but it means a colour ramp built from the summary starts at
zero, and with 95 percent of January's cells at exactly zero, a linear ramp
renders January as almost uniformly one colour. Rainfall usually wants a
non-linear scale, which is a thing the metadata cannot express.

*Two collections sharing a bbox is a coincidence of this example.* Both grids
were built with the same `NY`, `NX`, and `BBOX`. Two real sources at different
resolutions would have different transforms and therefore different (though
overlapping) bounding boxes, since the outer edges depend on the cell size.

*The three-year SPI uses `xr.concat` rather than the store.* The multi-year
history is built in memory:

```python
parts = [
    normalize(fetch_precipitation(p, ny=NY, nx=NX))
    for year in (2022, 2023, 2024)
    for p in enumerate_periods(year, MONTHS)
]
history_ds = xr.concat(parts, dim="time")
```

which is fine at this scale and is not what you would do at any real one. A
real service computes indices against the store, lazily, so that dask can read
only the chunks it needs.

---

## The pipeline end to end

This section walks `0401_full_pipeline.py` stage by stage with its real output,
saying at each point which underlying library is doing the work. It is the same
run quoted in pieces above, here in one piece.

```bash
cd climate-pipeline
make run EXAMPLE=0401_full_pipeline
```

About a second on the machine behind this page -- **machine-dependent**.

### Stage 1 — the source

```text
==========================================================================
STAGE 1 -- the source: periods on offer, and what one of them looks like
==========================================================================
A source does not hand over a dataset; it hands over a list of periods it
can supply, and the framework fetches them one at a time.
  enumerate_periods(2024, 12) -> 12 periods
  first: 2024-01 starting 2024-01-01, 31 days
  leap February: 2024-02 has 29 days
  last:  2024-12 starting 2024-12-01, 31 days
  total days on offer: 366

One raw period, in the source's own awkward conventions:
  variable   : t2m('time', 'lat', 'lon')  <- lat/lon, not y/x
  units      : K  (mean 300.95)
  lat[0]=6.900 .. lat[-1]=10.000  <- ASCENDING: south-up
  Ingest normalizes all three at once: renamed to (time, y, x), Kelvin to
  degC, and flipped north-up, so everything downstream sees one convention.
```

**What happens.** `enumerate_periods(2024, 12)` builds twelve `Period` records,
delegating the calendar to `pandas.Timestamp.days_in_month`. Then one period is
fetched with `fetch_temperature` and inspected without normalizing it, so the
three problems are visible.

**Who does the work.** pandas for the calendar and the `DatetimeIndex`; numpy
for the array generation, including the seeded `default_rng` whose seed comes
from `zlib.crc32` rather than `hash`; xarray for wrapping the result into a
`Dataset` with named dims, coordinates, and attributes.

**What to notice.** The leap February. 366 days rather than 365, and 29 in
February rather than 28 or 30. Those numbers are what make stage 2's chunk
alignment necessary.

### Stage 2 — ingest

```text
==========================================================================
STAGE 2 -- ingest: twelve periods, one commit each
==========================================================================
Each period is fetched, normalized, appended, and committed on its own.
A crash therefore leaves a store complete up to the last commit, never
half a month, and resume is a matter of asking the store what it holds.
  ingested : 12 periods -> 2024-01 .. 2024-12
  skipped  : 0   failed: 0
  store    : {'time': 366, 'y': 16, 'x': 16}, variable t2m in degC
  time span: 2024-01-01 .. 2024-12-31 (366 days, contiguous)
  monotonic increasing: True, unique: True
  chunks   : (30, 30, 30)... days per time chunk
```

**What happens.** `ingest(repo, periods, fetch)` loops the twelve periods. For
each: `fetch` produces a raw dataset, `normalize` renames, flips, converts and
sorts it, `chunking_for` picks `{"time": 30, "y": 16, "x": 16}`, `ds.chunk(...)`
applies it, `to_zarr` appends along time with `align_chunks=True`, and
`session.commit(...)` publishes the snapshot. Twelve fetches, twelve
normalizations, twelve appends, twelve commits.

**Who does the work.** xarray for `normalize` and for `to_zarr`; dask for the
chunked array that `ds.chunk()` produces and for the rechunking that
`align_chunks=True` performs; zarr for the array format and the compression
(Zstd level 0 by default); icechunk for the transaction, the manifest, and the
snapshot.

**What to notice.** `monotonic increasing: True, unique: True` -- checked on the
committed store, not asserted about the intent. And the chunk sizes: twelve 30s
and a final 6, since 366 = 12 x 30 + 6:

```python
>>> ds["t2m"].chunksizes
Frozen({'time': (30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 6), 'y': (16,), 'x': (16,)})
```

That trailing 6 is a partial chunk. The next append will land on top of it,
which is exactly the situation `align_chunks=True` exists to handle.

The full encoding, for reference:

```python
>>> ds["t2m"].encoding
{'chunks': (30, 16, 16), 'preferred_chunks': {'time': 30, 'y': 16, 'x': 16},
 'compressors': (ZstdCodec(level=0, checksum=False),), 'filters': (), 'shards': None,
 'serializer': BytesCodec(endian='little'), 'fill_value': np.float64(nan),
 '_FillValue': nan, 'dtype': dtype('<f8')}
```

`chunks` is the *store's* chunk shape and never changes; `chunksizes` is the
dask graph's view and includes the partial final chunk.

### Stage 3 — the history

```text
==========================================================================
STAGE 3 -- history: one snapshot per period, kept forever
==========================================================================
The store is versioned, so the ingest left a readable audit trail. Each
snapshot is the store as it stood after exactly one month landed.
  13 snapshots = 1 repository init + 12 ingests
    1CECHNKREP  Repository initialized
    8GDH6XXBSE  ingest 2024-01
    EGABCJS59K  ingest 2024-02
    EJMDTBG23J  ingest 2024-03
    ...
    VJZAK79SC5  ingest 2024-11
    4MTWG04JSK  ingest 2024-12
  reading the oldest ingest snapshot back: 31 days
  reading the branch tip:                  366 days
  Same store, two points in its history -- useful when a published figure
  has to be reproduced months after the series it came from grew.
```

**What happens.** `repo.ancestry(branch="main")` walks the snapshot chain
backwards from the tip. Then the example opens a *specific historical snapshot*:

```python
first_month = xr.open_zarr(
    repo.readonly_session(snapshot_id=history[-2].id).store, consolidated=False
)
```

`history[-2]` is the second-oldest snapshot -- the oldest is the repository
initialization -- so this is the store as it stood after January landed and
nothing else.

**Who does the work.** icechunk entirely. `readonly_session(snapshot_id=...)`
constructs a store view pinned to that snapshot; xarray then opens it exactly
like any other store and has no idea it is looking at the past.

**What to notice.** 31 days at the old snapshot, 366 at the tip. Same repository
directory, same code path, two answers -- and both are correct, because they are
answers to different questions.

The snapshot ids are ULID-like: sortable, unique, not content hashes. `1CECHNKREP`
is icechunk's fixed initial-snapshot id and is the same in every repository ever
created.

This is the property that makes a published figure reproducible. Record the
snapshot id alongside the figure and you can regenerate it exactly, however much
the series has grown. Without per-period commits there would be nothing to
record.

### Stage 4 — derive

```text
==========================================================================
STAGE 4 -- derive: a climatology and a hot-days index
==========================================================================
Raw temperature is an input, not an answer. The normal says what a month
usually looks like; the index answers a question someone actually asked.
  climatological_normal(ds) -> ('month', 'y', 'x') (12, 16, 16)
  area-mean normal, degC by month:
    month     1     2     3     4     5     6     7     8     9    10    11    12
    degC   27.8  29.1  29.9  29.9  29.1  27.8  26.2  24.9  24.1  24.1  24.9  26.3

  hot_days(ds, threshold=30.0) -> ('time', 'y', 'x') (12, 16, 16)
  units days, days above 30.0 degC
  mean hot days per cell, by month:
    month     1     2     3     4     5     6     7     8     9    10    11    12
    days    1.2   7.1  14.3  13.8   7.6   0.9   0.0   0.0   0.0   0.0   0.0   0.0
  hottest month: 03 at 14.3 days per cell
  range across the whole grid and year: 0 .. 31 days
```

**What happens.** `climatological_normal` groups the 366 daily maps by calendar
month and averages, collapsing `(366, 16, 16)` to `(12, 16, 16)`. `hot_days`
compares every value to 30.0, producing a boolean array, then resamples to month
ends and sums, also producing `(12, 16, 16)` -- but with a `time` dimension
rather than a `month` one, because a count is still a time series.

**Who does the work.** xarray's `groupby` with the virtual `time.month`
accessor, and xarray's `resample` with the `"1ME"` month-end frequency. Both
build dask graphs against the store's chunks; the numbers only materialize when
the example formats them for printing. numpy does the actual arithmetic per
chunk.

**What to notice.** Compare the two rows. The temperature normal ranges from
24.1 to 29.9 -- a 5.8-degree swing. The hot-day count ranges from 0 to 14.3.
July through November record *zero* days above 30, and March records 14.3 out of
31. A 5.8-degree change in the mean produced a change from "never" to "half the
month" in the count. That extreme nonlinearity around a threshold is why
threshold indices communicate risk better than means do.

The `range across the whole grid and year: 0 .. 31 days` line is the per-cell
extreme: somewhere on the grid, in some month, *every single day* exceeded 30
degrees. The area mean of 14.3 for March hides that entirely.

Note also that the two outputs have different dimension names -- `month` for the
normal, `time` for the index -- which is xarray being honest about what each one
is. The normal is not a time series; the index is.

### Stage 5 — the pyramid

```text
==========================================================================
STAGE 5 -- pyramid: coarser levels so a zoomed-out map is cheap
==========================================================================
A viewer showing the whole country does not want every cell. Each level
halves both spatial dims by 2x2 averaging, so a wide view reads a small
array instead of the full grid.
  4 levels:
    level 0: 16 x 16 =  256 cells per timestep (100.0% of level 0)
    level 1:  8 x  8 =   64 cells per timestep (25.0% of level 0)
    level 2:  4 x  4 =   16 cells per timestep ( 6.2% of level 0)
    level 3:  2 x  2 =    4 cells per timestep ( 1.6% of level 0)
  level 0 area mean 27.0026 degC
  level 3 area mean 27.0026 degC
  Coarsening is a mean, so the field's average survives it; what is lost is
  detail, which is exactly what a zoomed-out tile cannot show anyway.
```

**What happens.** `pyramid_levels(ds, levels=4)` calls
`coarsen(y=2, x=2, boundary="trim").mean()` three times in succession, keeping
the input as level 0.

**Who does the work.** xarray's `Coarsen`, which reshapes each spatial dimension
into pairs and reduces; dask underneath, since the input came from a chunked
store, so the coarsening is lazy until the means are printed.

**What to notice.** `27.0026` at level 0 and `27.0026` at level 3. Four
consecutive halvings, 256 cells reduced to 4, and the area mean is identical to
four decimal places. That is the mean-preserving property of equal-size block
averaging, and it is a useful invariant to assert in tests: a pyramid level
whose area mean has drifted has a boundary bug.

It is preserved *here* because 16 is divisible by 2 four times. On a grid whose
dimension is not a power of two, `boundary="trim"` drops cells at each level and
the area mean does drift slightly -- because a different set of cells is being
averaged.

### Stage 6 — publish

```text
==========================================================================
STAGE 6 -- publish: GeoZarr attributes and a STAC collection
==========================================================================
Two documents turn a store into a product: root attributes that place the
grid on Earth, and a collection document a client can discover it through.
  wrote 6 GeoZarr keys to the root and committed them:
    spatial:transform  = [0.2133, 0.0000, -13.6067, 0.0000, -0.2067, 10.1033]
    spatial:dimensions = ['y', 'x'] (array order: y is the slow axis)
    spatial:shape      = [16, 16]
    proj:code          = EPSG:4326

  STAC collection id 'temperature-daily-sle':
    extent.spatial.bbox      = [-13.6067, 6.7967, -10.1933, 10.1033]
    extent.temporal.interval = ['2024-01-01T00:00:00Z', '2024-12-31T00:00:00Z']
    summaries.variables.t2m  = degC, 19.7292 .. 33.6672
    assets.zarr.href         = https://climate.example.org/zarr/temperature-daily-sle
  Both extents are derived from the committed data, so publishing again
  after next month's ingest needs no edits anywhere.
```

**What happens.** `geozarr_attrs(ds)` derives six keys from the coordinate
arrays. A writable session opens the zarr group directly, updates its
attributes, and commits -- so the publish is its own snapshot, the fourteenth.
Then `stac_collection(ds, ...)` builds the document, deriving the bbox from
`bounding_box(ds)`, the interval from `temporal_extent(ds)`, and the value range
from a `nanmin`/`nanmax` over the array.

**Who does the work.** numpy for the transform arithmetic and the min/max;
pandas for the ISO 8601 formatting of the timestamps; the `zarr` API directly
for the attribute write, because xarray's `to_zarr` writes datasets and this
needs to touch only the root group; icechunk for the commit.

**What to notice.** The bbox and the transform share numbers. `originX` is
-13.6067 and `west` is -13.6067; `originY` is 10.1033 and `north` is 10.1033.
They are the same values because `bounding_box` calls `grid_transform`. One
source of geometry.

`stepY` here is -0.2067 rather than the -0.2818 seen in `0301_geozarr`, because
this grid is 16 rows over the same latitude span rather than 12. The step
follows the grid; nothing is hard-coded.

### The summary block

```text
=== Summary ===
  source periods     12 months of 2024
  periods ingested   12 (failed: 0)
  days stored        366
  grid               16 x 16 cells, north-up, EPSG:4326
  snapshots          14 (1 init + 12 ingests + 1 publish)
  climatology shape  ('month', 'y', 'x') (12, 16, 16)
  index shape        ('time', 'y', 'x') (12, 16, 16) of hot days
  pyramid levels     16x16 -> 8x8 -> 4x4 -> 2x2
  bbox               [-13.607, 6.797, -10.193, 10.103]
  time interval      2024-01-01T00:00:00Z .. 2024-12-31T00:00:00Z
  stac collection    temperature-daily-sle (1 asset, 1 variable)

- Messy source in, discoverable placed versioned product out
- Every extent published above was read back off the store, never declared
- The store is the record: history, resume, and reproduction all read from it
```

Fourteen snapshots for twelve months of data: one repository init, twelve
ingests, one publish. Every line of that table was computed from the store.

### What the store looks like on disk afterwards

```text
temperature-daily-sle.icechunk/
  chunks/
  manifests/
  snapshots/
  transactions/
  overwritten/
  repo
```

About 1.1 MB for 753 kB of logical `float64` data -- **machine-dependent**, and
at this scale the overhead is dominated by fourteen snapshots' worth of metadata
rather than by the arrays. At production scale the ratio inverts completely and
the chunks dominate.

The important structural point is that `chunks/` holds immutable objects. An
append writes new chunk objects; it does not modify old ones. That immutability
is what lets a snapshot be a cheap pointer set rather than a copy, and it is
what makes reading an old snapshot free.

---

## Design decisions and their consequences

Six decisions shape this project. Each one has a defensible alternative, each
one has a cost, and each one produces observable behaviour that would be
different if the decision had gone the other way. This section is the argument
for each, and -- more usefully -- the price.

### Why one commit per period

The decision: `ingest_period` opens a writable session, appends exactly one
period, and commits. Twelve months means twelve commits, not one.

The alternative is obvious and tempting: open one session, append all twelve
periods, commit once at the end. Fewer transactions, less metadata, a cleaner
history with one entry per run.

**The argument is crash recovery, and it is decisive.**

Consider what a partially completed run leaves behind under each design.

With one commit at the end, a crash after eight months leaves nothing. The
session is discarded, the branch pointer never moved, and the store is exactly
as it was before the run started. Eight months of fetching, normalizing, and
compressing are thrown away. On a real ingest, where each month is a network
round trip against a rate-limited upstream, that is hours of work and quota.

Worse, the failure mode is *worse the further you get*. A run that dies at
month 11 of 12 loses more than one that dies at month 2. There is no partial
credit, and the probability of completing a long run without interruption falls
as the run gets longer. This is the classic argument for checkpointing, and it
applies exactly.

With one commit per period, a crash after eight months leaves eight months
committed. The ninth is lost -- the session it was being written into is
discarded -- and nothing else is. Resume re-fetches month nine and continues.

The example states it directly:

```text
  A month of daily data is many chunk writes. If the process dies partway
  through one, an ordinary zarr store on disk is left holding some new
  chunks and a time axis that may or may not have been extended -- torn.
  icechunk only publishes a snapshot when commit() succeeds, so a crash
  leaves the store exactly as of the last completed period. Never half a
  month.
```

Note what the transaction is protecting against: not just losing work, but
*torn* state. A plain Zarr store being appended to has no transaction. The
chunk objects go down first, then the array metadata is updated to claim the
longer shape. A crash between the two leaves either orphaned chunks (harmless,
wasteful) or -- if the metadata is written first, or if the writer is doing
something clever -- a store whose declared shape exceeds the chunks that exist.
Reading such a store returns fill values from chunks that were never written,
silently. icechunk's commit boundary makes that state unreachable.

**The costs.**

*Transaction overhead.* A commit involves writing a manifest, writing a snapshot,
and a compare-and-swap on the branch pointer. It is not free. The 48-period
ingest in `0202_indices` takes 4.2 seconds -- **machine-dependent** -- and
essentially all of it is transaction overhead rather than arithmetic. If you
were ingesting hourly periods rather than monthly ones, that overhead would
dominate.

*History volume.* Forty years of monthly ingests is 480 snapshots. Each is
small, but they accumulate, and icechunk's garbage collection has to keep every
chunk any live snapshot references. Expiring old snapshots is a real
operational task at that scale.

**When the decision would flip.** If the period were small enough that the
transaction cost exceeded the fetch cost, batching several periods per commit
would be right, and the recovery granularity would coarsen accordingly. The
knob is explicit: recovery granularity versus transaction overhead. Monthly
periods for daily data put it comfortably on the recovery side.

**The bonus.** The history doubles as provenance for free. `IngestReport.snapshots`
maps each period id to the snapshot in which it landed, and `repo.ancestry()`
gives a readable log:

```text
  75T85KYM1KMC...  2026-08-17 17:48:31  ingest 2024-05
  FS706ZQX64CQ...  2026-08-17 17:48:31  ingest 2024-04
```

You can point at when any given month arrived, and you can read the store as it
stood at that moment. Nobody had to build that; it is a side effect of the
commit boundary.

### Why resume reads the store rather than external bookkeeping

The decision: `committed_periods(repo)` opens the store, reads the time
coordinate, and derives the set of period ids from the timestamps present.
Nothing else is consulted.

```python
stamps = pd.DatetimeIndex(ds["time"].values)
return {f"{ts.year}-{ts.month:02d}" for ts in stamps}
```

The alternatives are all forms of external bookkeeping: a manifest file beside
the store, a row per period in a job database, a progress key in Redis, a
sidecar JSON listing what has been ingested.

**The argument is that anything which can disagree with the data eventually
will.**

Walk through the failure windows for a manifest.

*Manifest written after the commit.* The commit succeeds, then the process dies
before the manifest is updated. The store holds March; the manifest says it does
not. Resume re-ingests March, appending a duplicate month to a store that
already has it. Now the time axis has 62 entries for March, and every downstream
`sel` and `resample` is wrong.

*Manifest written before the commit.* The manifest is updated, then the commit
fails. The manifest says March is done; the store does not have it. Resume skips
March. The gap is permanent and silent -- nothing will ever notice, because the
only thing that would have noticed is the manifest, and it is the thing that is
wrong.

*Manifest and commit in one transaction.* Only possible if the manifest lives
inside the store, at which point it is redundant with the time coordinate.

*Job database on another machine.* Add network partitions to the above.

Every one of these is a real, reachable state. There is no ordering of two
independent writes that eliminates the window; that is what "two-phase commit is
hard" means.

Deriving the answer from the data has no such window, because the question
"what does the store hold" and the question "what should I ingest next" are
answered by the *same read of the same bytes*. There is nothing to keep in
sync, because there is only one thing.

The example puts it in one line:

```text
  Because the last commit is the last thing that succeeded, this answer is
  true even if the process died mid-write on the very next period.
```

**The costs.**

*A full read of the time coordinate on every run.* For forty years of daily
data that is about 14,600 `int64` values, roughly 120 kB, plus the Zarr metadata
read to find it. Negligible, and it is the same read a manifest validator would
have to do anyway.

*It only works for period types the store can express.* `committed_periods`
reconstructs monthly ids from timestamps, which works because "which month is
this timestamp in" is a total function. A period type that is not recoverable
from the data -- "the batch the vendor sent on Tuesday" -- cannot be derived and
would need real bookkeeping. Hence the `period_type` parameter and its explicit
`ValueError` on anything but `"month"`.

*Partial periods are invisible.* If a period were half-written and committed --
which this design makes impossible, but a different one might not -- the derived
set would report it as present. The correctness of the derivation depends on the
atomicity of the commit. The two decisions hold each other up.

**The generalizable rule.** Where you can derive state from the data rather than
tracking it beside the data, derive it. The derived answer cannot drift, cannot
be stale, and cannot need reconciliation. This is the same instinct behind
computing extents from the store instead of declaring them, two decisions
further down.

### Why `align_chunks=True` is mandatory

The decision, and the six-line comment that guards it:

```python
if existing:
    # align_chunks=True is not optional here. Months are 28-31 days and the
    # store's time chunk is 30, so after a few appends the final zarr chunk
    # is partial and the incoming period straddles it. Without alignment
    # xarray refuses the write outright -- "would overlap multiple Dask
    # chunks" -- because a parallel write across a shared chunk can corrupt
    # it. Alignment rechunks the incoming data to the store's boundaries.
    ds.to_zarr(session.store, append_dim="time", consolidated=False, align_chunks=True)
```

**The mechanics.** The store's time chunk is fixed at 30 days, set by
`TIME_CHUNK` and baked into the array's encoding at creation. Months are 28, 29,
30, or 31 days. Every append lands on a boundary that does not divide evenly.

Trace 2024 through:

| Append | Days | Store length after | Final chunk after |
|---|---|---|---|
| January | 31 | 31 | 1 of 30 (partial) |
| February | 29 | 60 | 30 of 30 (full) |
| March | 31 | 91 | 1 of 30 (partial) |
| April | 30 | 121 | 1 of 30 (partial) |
| May | 31 | 152 | 2 of 30 (partial) |

After January the store is 31 long: one full 30-day chunk plus a chunk holding
one day. February's 29 days must fill the remaining 29 slots of that second
chunk. But February arrives as a *single* dask chunk of 29 elements, and it
maps onto a region of the store that is the tail of one Zarr chunk. Sometimes
that works. By the fifth month it does not.

**What actually happens without alignment.** Removing `align_chunks=True` and
running the same ingest:

```text
2024-01: OK (unaligned)
2024-02: OK (unaligned)
2024-03: OK (unaligned)
2024-04: OK (unaligned)
2024-05: ValueError: Specified Zarr chunks encoding['chunks']=(30, 12, 12) for
variable named 't2m' would overlap multiple Dask chunks. Please check the Dask
chunks at position 0 and 0, on axis 0, they are overlapped on the same Zarr
chunk in the region slice(121, None, None). Writing this array in parallel with
Dask could lead to corrupted data. To resolve this issue, consider one of the
following options: - Rechunk the array using `chunk()`. - Modify or delete
`encoding['chunks']`. - Set `safe_chunks=False`. - Enable automatic chunks
alignment with `align_chunks=True`.
```

Four months succeed and the fifth fails. That is the worst possible failure
profile: not immediate, not deterministic-looking, and comfortably past the
point where a smoke test would have caught it.

The message names the exact situation. The store's chunk shape is `(30, 12, 12)`.
The incoming write covers `slice(121, None, None)` -- from day 121 to the end.
Day 121 is inside the fifth Zarr chunk, which spans days 120 to 149. May's 31
days therefore span the tail of chunk 4 and the head of chunk 5, and May arrives
as a single dask chunk. One dask chunk overlapping two Zarr chunks is the thing
xarray refuses.

**Why xarray refuses rather than just doing it.** The message says: "Writing
this array in parallel with Dask could lead to corrupted data."

A Zarr chunk is the unit of atomicity. Writing part of a chunk means
read-modify-write: read the existing chunk, splice in the new values, write it
back. If two dask tasks are doing that to the *same* chunk concurrently -- which
they can be, since dask does not order independent tasks -- the second write
overwrites the first's changes. The chunk ends up with one task's data and not
the other's, silently. No error, no warning, wrong numbers.

xarray cannot tell whether your scheduler will run those tasks concurrently, so
it refuses the whole class of write. That is the correct behaviour and it is
worth appreciating rather than working around.

**What the four options actually do.**

- *Rechunk the array using `chunk()`* -- rechunk the incoming data so its dask
  chunks align with the store's Zarr chunks. Correct, and it is what
  `align_chunks=True` does for you.
- *Modify or delete `encoding['chunks']`* -- let Zarr pick a new chunk shape.
  Wrong for an append: the store's existing chunks do not change, so this just
  moves the conflict.
- *Set `safe_chunks=False`* -- turn off the check. **This is the dangerous
  option.** The write proceeds, the corruption risk is real, and you will not
  find out. Never do this to get past this error.
- *Enable `align_chunks=True`* -- the right answer. xarray rechunks the incoming
  data onto the store's boundaries before writing, so every dask chunk maps to
  exactly one Zarr chunk.

**The cost.** A rechunk is a graph operation: dask reshuffles the incoming
array's blocks. For one month against 30-day chunks that is essentially free.
For a badly mismatched pair -- an incoming array chunked `(1, y, x)` against a
store chunked `(365, y, x)` -- it is an all-to-all shuffle and can be expensive.
The lesson generalizes: choose the incoming chunking to be close to the store's.
`chunking_for` does this by using the same `TIME_CHUNK` constant the store was
created with.

**Why this appears twice in this repository.** The `icechunk` project hit the
identical failure independently, on the identical period -- the fifth month --
and `icechunk/examples/0401_append_periods.py` probes each period and reports
honestly which ones would have succeeded unaligned. Two projects, two authors'
worth of separation, same month. It is not a corner case.

It is also the same family of problem as `_uniform_chunks` in
`dask/examples/0601_zarr_legal_chunks.py`: dask and Zarr have different ideas
about what a legal chunk layout is, and the disagreement surfaces at write time
rather than at chunk time, which is why it is always a surprise.

**The alternative that avoids it entirely.** Chunk the time axis at 1. Then
every append aligns trivially, because every chunk is one day. The cost is
tens of thousands of tiny chunks per variable, which makes metadata enormous and
reads slow. Nobody does this. The 30-day chunk plus alignment is the right
trade, and the alignment flag is the price.

### Why appends leave a non-monotonic axis when backfilling

The decision is really a *consequence*, and `0103_resume` is honest about it
rather than hiding it.

`to_zarr(..., append_dim="time")` appends. It adds to the end. That is the only
thing it does. There is no "insert in the middle", because inserting into the
middle of a chunked array means rewriting every chunk from the insertion point
onward, plus the coordinate array, plus the array metadata.

So when a period fails and is retried later, it lands *after* everything that
succeeded in between:

```text
  report.ingested = ['2024-01', '2024-02', '2024-04', '2024-05']
  report.failed   = {'2024-03': 'RuntimeError: source unavailable: upstream returned HTTP 503'}
```

then the retry:

```text
  retry: ingested=['2024-03'] skipped=['2024-01', '2024-02', '2024-04', '2024-05']
  store after the retry: periods=['2024-01', ..., '2024-05'] steps=152 span=2024-01-01 .. 2024-03-31

  all five periods present: True
  time axis monotonic     : False
```

Read that carefully. **All five periods are present. All 152 days are there.
Every value is correct.** And the time axis reads January, February, April, May,
March, because March was appended after May.

The example draws the conclusion:

```text
  Note the span above ends at 2024-03-31: March was appended AFTER May, so
  the time axis is complete but out of order. Appending only ever adds to
  the end. Backfilling a gap in the middle is a rewrite, not an append --
  which is the argument for ingesting periods in order and retrying early.
```

**What breaks.** A non-monotonic time index is not merely untidy.

- `ds.sel(time=slice("2024-02", "2024-04"))` raises. Pandas slice-based label
  indexing requires a monotonic index, and this one is not.
- `temporal_extent(ds)` reports `2024-01-01 .. 2024-03-31`, because it takes
  first and last by *position*. The store's real coverage ends on 31 May. The
  published STAC interval is therefore wrong.
- Anything that assumed adjacent positions are adjacent in time -- a rolling
  window, a difference along time -- computes garbage across the seam.
- `resample` and `groupby("time.month")` still work, because they group by
  value rather than position. This is a good reason to prefer them.

**Why the code does not just sort.** It could. `ds.sortby("time")` after each
append would fix the axis. It would also be a full rewrite of the array: sorting
along the chunked dimension means every chunk's contents change, so every chunk
is rewritten, so the store doubles in size at every commit and the append
becomes O(n) instead of O(1). That destroys the entire economics of an
append-only store.

The honest options are:

1. **Ingest in order and retry early.** This is what the example recommends.
   Retry the failed period before moving on, or run the retry pass immediately
   after the main pass, before the next scheduled ingest. The gap then closes
   before anything else is appended.
2. **Rewrite the store.** Read everything, sort, write a new store, swap the
   branch. Correct, expensive, and something you schedule rather than something
   that happens automatically.
3. **Accept it and sort on read.** `xr.open_zarr(...).sortby("time")` is cheap
   when it is lazy and the consumer was going to load the data anyway. It pushes
   the cost to every reader instead of paying it once.

This project does none of them -- it surfaces the problem and names the fix.
That is the right call for a teaching project and would be the wrong call for a
production one, which should at minimum detect the condition and refuse to
publish a STAC interval from a non-monotonic axis.

**The deeper point.** Append-only storage is fast because it never touches what
is already there. That property and "arbitrary insertion" are mutually
exclusive. Every append-only system in existence -- log-structured storage,
Kafka, git's object store -- makes the same trade, and every one of them handles
out-of-order arrival with either a compaction pass or a sort on read. Knowing
which one you have chosen is the whole discipline.

### Why extents are read back rather than declared

The decision is visible in the signature:

```python
def stac_collection(
    ds: xr.Dataset,
    dataset_id: str,
    *,
    title: str | None = None,
    description: str = "",
    zarr_href: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
```

There is no `bbox` parameter. There is no `start` or `end` parameter. There is
no `shape`. The function accepts a dataset and derives every extent from it:

```python
bbox = bounding_box(ds)
interval = temporal_extent(ds)
```

**You cannot publish a collection claiming an extent the store does not have,
because the function does not let you say one.**

The alternative is what most catalogues actually do: a configuration file, or a
database row, or a YAML block, saying that dataset `temperature-daily-sle`
covers Sierra Leone from 2024-01-01 to 2024-12-31. Somebody types it once and
it is right.

Then the next month is ingested. Now it is wrong, and nothing knows. The
catalogue says the series ends in December; the store has January. A client
filtering by date will not find the new month. A client that fetches anyway gets
data outside the declared interval. Neither is an error; both are wrong.

Six months later the discrepancy is discovered, somebody edits the config, and
the same thing happens again next month.

Deriving the extent means the update is automatic:

```text
  Nothing here is declared by hand. The interval is read back off the
  committed time coordinate, so it cannot drift from what the store holds --
  re-publish after the next month lands and the end date moves by itself.
```

Re-running `stac_collection(ds, ...)` after any ingest produces a document that
is correct *by construction*. The publish step becomes idempotent and safe to
run on a schedule.

**The same discipline applied twice more.**

`bounding_box` is built on `grid_transform` rather than beside it, so the STAC
bbox and the GeoZarr bbox are the same numbers rather than two computations that
agree today. `0302_stac` asserts it:

```python
assert stac_bbox == geo_bbox
```

```text
  identical to the STAC bbox: True
  That is the point: catalogue search and rendered tile derive the box from
  the same transform, so a hit in the catalogue is really over that ground.
```

And `IngestReport.total` is a property, not a field:

```python
@property
def total(self) -> int:
    return len(self.ingested) + len(self.skipped) + len(self.failed)
```

so a report cannot claim a total that disagrees with its own lists.

Three instances of one rule: **derive rather than declare, wherever the
derivation is cheap.** Every declared value is a value that can go stale. Every
derived value is a value that cannot.

**The costs.**

*You cannot publish an intended extent.* Sometimes you want to. A collection
that will cover 1981 to the present, of which only 2024 has been ingested,
genuinely has a *planned* extent that differs from its *current* one, and STAC
has no way to express the difference anyway. This design simply cannot say it.

*Deriving the value range is expensive at scale.* `np.nanmin(var.values)` loads
the entire array. Instant on 366 x 16 x 16; a full archive scan on forty years
of 4000 x 4000. A production version would maintain running min and max as
attributes updated at ingest, which is a *declared* value -- and would therefore
need care to keep it from drifting, which is exactly the problem this section is
about. There is no free lunch; the right trade is to derive what is cheap and
maintain what is not, deliberately, with the maintenance in the same transaction
as the write.

*A non-monotonic axis produces a wrong-but-derived interval.* Discussed above.
Deriving protects you from staleness, not from every kind of wrongness.

### Why pixel registration and array-order axis naming matter

Two GeoZarr details, both of which produce silent errors, and both of which
`0301_geozarr` verifies rather than assumes.

#### Pixel registration

The origin of an affine transform is the **outer edge** of the first cell, not
its centre:

```python
origin_x = float(x[0]) - step_x / 2.0
origin_y = float(y[0]) - step_y / 2.0
```

The coordinate arrays hold centres. The transform wants edges. The difference is
half a cell.

Verified against the data:

```text
    x[0] (centre of column 0) = -13.500000
    origin_x                  = -13.606667
    x[0] - stepX / 2          = -13.606667  <- matches origin_x
  The offset is exactly half a cell: 0.106667 = stepX/2 = 0.106667
  Walking the full width lands on the far edge: origin_x + stepX*nx = -10.193333
  which is half a cell past the last centre x[-1] = -10.300000.
```

with `assert np.isclose(...)` on each claim, so a wrong transform fails the run.

**The consequence of getting it wrong.** Every rendered tile shifts by half a
pixel. On this grid, `stepX` is 0.2133 degrees, so half a cell is 0.1067 degrees
-- about 11.8 km of longitude at 8 degrees north. The example's phrasing is
exact:

```text
  Treat the origin as a centre instead and every rendered tile shifts by
  half a pixel -- small enough to ship, big enough to misplace a clinic.
```

It will pass review. Nobody eyeballing a national-scale map notices a half-cell
offset. It will be wrong on the ground, and the error is systematic rather than
random, so it will not average out.

There is a second, related error that is easier to make: computing the bbox from
`y.min()` and `y.max()` instead of from the transform. That gives the *centre*
extent, which is `(n-1) * step` wide rather than `n * step` -- one cell short in
each dimension. `bounding_box` avoids it by calling `grid_transform` rather than
touching the coordinates directly.

#### Array-order axis naming

```python
"spatial:dimensions": ["y", "x"],
"spatial:shape": [int(ds.sizes["y"]), int(ds.sizes["x"])],
```

with the comment:

```python
# Array order, y first: read positionally by clients. Naming these
# x-first transposes every raster that reads the store.
```

A client reads `spatial:dimensions` positionally. The first entry names the slow
axis of the buffer. Get the order wrong and the client walks the same bytes with
a different stride.

`0301_geozarr` performs it rather than describing it, on a deliberately
non-square 12 x 16 grid:

```text
  Correct read, shape (12, 16): cell [3, 5] = 27.107 degC
  x-first read, shape (16, 12): cell [3, 5] = 27.400 degC
  The real cell [3, 5] is element 53 of the buffer, which the
  x-first reader places at [4, 5] instead.
  Same bytes, same chunks, different geography: only the stride the client
  walks the buffer with changed, and every pixel landed somewhere else.
```

Element 53 is `[3, 5]` at stride 16 and `[4, 5]` at stride 12. Nothing was
corrupted. Every value is intact. Every value is in the wrong place.

**Why the example uses a non-square grid.** Because on a square grid the bug is
*invisible*:

```text
  On a square grid (12x12) the transposed shape still fits, so the bug
  renders silently -- it survives until someone compares a tile to a map.
```

A 12 x 16 array reshaped to (16, 12) fits -- 192 elements either way -- so there
is no shape error to catch it. On a square grid there is not even a transposed
*shape* to notice. The picture is a transpose of the truth, which for a
roughly-square country is a plausible-looking picture that nobody questions.

This is the same class of bug as the south-up render and the half-cell shift:
**the failure is a picture rather than an exception.** All three produce output
that looks fine. That is why all three get an explicit numerical check in the
example rather than a comment saying "be careful here". A comment cannot fail a
build; an `assert` can.

The transposition trap also explains the `NY = 12, NX = 16` at the top of
`0301_geozarr` and the `ny=12, nx=16` in `0302_stac`. Deliberately non-square
test data is a cheap defence against a whole family of axis bugs, and it costs
nothing to adopt as a habit.

---

## Pitfalls and gotchas

A consolidated list. Several appear above in context; they are gathered here
because when you hit one you will be searching, not reading.

### `hash()` is randomized per process

Python salts string hashing with a per-process random seed. `hash("2024-01")`
is a different number in every interpreter you start:

```text
hash("2024-01") = 6147987242643249052  crc32 = 6962
hash("2024-01") =  814182303813762383  crc32 = 6962
hash("2024-01") = 7707634371377113861  crc32 = 6962
```

`sources.py` uses `zlib.crc32` instead, which is stable across processes and
machines:

```python
def _period_seed(period: Period) -> int:
    return zlib.crc32(period.period_id.encode()) % 10_000
```

Use `hash()` to seed a random generator and your synthetic data changes on every
run. Documented outputs stop matching. Value-asserting tests fail
intermittently. And ingesting the same period twice produces *different data*,
which quietly destroys the idempotence the resume story depends on.

The failure is invisible within a single process -- everything is perfectly
consistent right up until you restart -- which is what makes it so hard to
diagnose.

**Rule: `hash()` is not a checksum.** For a stable identifier derived from a
string, use `zlib.crc32`, `hashlib.md5`, `hashlib.sha256`, or anything with a
defined output. Never `hash()`. (`PYTHONHASHSEED=0` disables the randomization
and is not a fix -- it makes your code depend on an environment variable that
somebody will eventually not set.)

### Attributes are inert

Nothing in xarray reads, validates, or updates the `units` attribute. Whether
attributes even *survive* an operation depends on the operation -- scalar
arithmetic keeps them, a binary op between two arrays keeps only the ones the
operands agreed on, a reduction keeps them -- and none of those outcomes is
*correct*, because none of them reflects what the operation did to the meaning.
Worked through with real output under
[`convert_units`](#convert_units).

Set the attributes yourself, explicitly, every time you change what a variable
means, in the same few lines as the change.

### `align_chunks=True` on every append

Variable-length months against a fixed 30-day chunk. Fails on the fifth month
with `would overlap multiple Dask chunks`. Full treatment
[above](#why-align_chunkstrue-is-mandatory). **Do not reach for
`safe_chunks=False`** -- it silences a real corruption warning.

### `consolidated=False` on every icechunk read and write

Consolidated metadata is a Zarr v2 optimization that icechunk does not need,
because a snapshot already contains the full metadata tree. Asking for it
produces a warning. Every `open_zarr` and `to_zarr` in this project passes
`consolidated=False`.

### A fresh icechunk repository has no group to open

`xr.open_zarr` on a repository that has been created but never written raises.
Both `committed_periods` and `_has_data` catch it and treat it as "no data",
which is the right answer for a first run:

```python
try:
    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
except Exception:
    return set()
```

Broad `except Exception` is doing real work here; the exception type is not part
of icechunk's public contract.

### The non-monotonic axis after a backfill

Retrying a failed period appends it after everything ingested since. All the
data is present and the axis is out of order. `sel` with a slice raises,
`temporal_extent` reports the wrong end date, and rolling operations compute
garbage across the seam. [Above](#why-appends-leave-a-non-monotonic-axis-when-backfilling).

Ingest in order; retry before moving on.

### `is_monotonic_increasing` does not catch duplicates

A sorted axis with repeated timestamps reports `True`. Check `is_unique`
separately. `normalize` handles it, keeping the last of each duplicate, but only
for data passing through `normalize` -- data appended by another path is not
protected.

### Intensive versus extensive

Averaging rainfall or summing temperature runs cleanly and produces a plausible
float with a plausible unit attribute. Nothing objects. 862 degC for a month is
a real output of a real program.

Temperature is intensive: average it. Rainfall is extensive: sum it. Record which
you did, ideally in `cell_methods`.

### `"1ME"`, not `"M"`

Month-end frequency in modern pandas is `"1ME"`. `"M"` is deprecated because it
was ambiguous between month-start and month-end. The resulting labels are the
*last* day of each month, so recovering the month means reading characters 5-6
of the timestamp rather than assuming the first.

### `resample` labels versus `groupby` labels

`resample(time="1ME")` produces a `time` dimension labelled with month-ends.
`groupby("time.month")` produces a `month` dimension labelled 1 to 12. They are
different outputs answering different questions -- one is a time series, one is
a climatology -- and mixing them up produces a shape error at best and a wrong
join at worst.

### `coarsen` needs `boundary=`

The default is `boundary="exact"`, which raises on a dimension that does not
divide evenly. `pyramid_levels` passes `boundary="trim"`. `"pad"` would fill
with NaN and fold a partial block into the mean at wrong weight, breaking the
mean-of-means property. Trim.

### A mean of means is only the mean when the blocks are equal

Which is why trimming is right and padding is wrong, and why the area mean is
preserved exactly through four pyramid levels on a 16 x 16 grid and only
approximately on a grid whose dimension is not a power of two.

### Square grids hide axis-order bugs

A transposed 12 x 12 array still fits in a 12 x 12 shape. Use non-square test
data. `0301_geozarr` uses 12 x 16 for exactly this reason.

### STAC nests bbox and interval one level deep

`"bbox": [[west, south, east, north]]` and
`"interval": [[start, end]]`. Lists of lists, because a collection may span
several footprints or several time ranges. The most common STAC mistake.

### Computing min/max loads the whole array

`np.nanmin(var.values)` in `stac_collection` triggers a full read and compute.
Free at this scale; a full archive scan at production scale.

### A short record caps a standardized index

With `n` samples per group and a population standard deviation, the largest
achievable z-score is `sqrt(n - 1)`. Three years gives 1.41; drought monitoring
flags at about -1.5. An SPI over three years **cannot raise a flag**, ever. This
is not noise, it is a structural ceiling.

### Zero standard deviation

`spi_like` guards with `std.where(std > 0, np.nan)`. Without it, a group with no
variation divides by zero. Over a one-year record *every* group has zero
variation, so the whole result is NaN -- which is the honest answer to "how
unusual was this January" when you have one January.

### icechunk logs are noisy by default

```python
icechunk.set_logs_filter("error")
```

Without it, the Rust core emits `WARN` chatter on every append, including the
genuine "local filesystem storage is not safe for concurrent commits" warning.
Worth reading once; not worth printing fifty times.

### Local filesystem storage is single-writer

`icechunk.local_filesystem_storage` warns about it, and the warning is correct.
A commit is a compare-and-swap on a branch pointer, and POSIX has no portable
conditional write. One committer at a time is correct on a local filesystem;
object storage becomes necessary as soon as compute spans machines or a second
writer appears. See [Storage](../storage.md).

### The library has no missing-data handling

`normalize` does not convert `_FillValue` sentinels to NaN. A source publishing
`-9999` over ocean cells would carry it straight into the store, and every
reduction would silently include it. The synthetic sources here have no missing
data, so the gap never shows -- which is precisely why it is worth flagging.

### The library assumes a regular grid

`grid_transform` computes the step from the first two coordinates and never
checks the rest. A Gaussian or otherwise irregular grid produces a transform
that is right at the origin and progressively wrong across the grid. There is no
validation.

---

## How this maps to open-climate-service

[open-climate-service](https://github.com/dhis2/open-climate-service) is the
real system this project is a model of. Each OCS instance is scoped to one
country, ingests from real sources like CHIRPS and ERA5, stores results as
GeoZarr inside icechunk, and exposes them through STAC, Zarr over HTTP, and
openEO. Every project in this repository was chosen because OCS depends on it;
this one is the whole shape in miniature.

The mapping, stage by stage.

### Stage: source

**OCS.** Source plugins for real archives. CHIRPS rainfall, ERA5 reanalysis
from the Copernicus Climate Data Store, and others. Each plugin authenticates,
handles rate limits and retries, deals with the archive's own pagination and
file layout, and downloads real files in netCDF or GRIB.

**Here.** `sources.py` generates plausible data with numpy from a crc32-derived
seed. No network, no credentials, no rate limits.

**What is preserved.** The *contract*: a source enumerates the periods it can
supply, and the framework fetches one at a time. `enumerate_periods` returns a
list of `Period` records; `Fetcher` is `Callable[[Period], xr.Dataset]`. That
one-function protocol is the real interface, and the fact that an example can
satisfy it with a lambda is the point.

**What is simplified.** Everything about actually getting bytes off the
internet. In OCS that is most of the code in a source plugin and essentially all
of its failure modes. `flaky_temperature` in `0103_resume` -- a function that
raises `RuntimeError("source unavailable: upstream returned HTTP 503")` for one
month -- stands in for the entire subject.

### Stage: normalize

**OCS.** Normalizes every source to `(time, y, x)`, converts Kelvin to Celsius
and precipitation to millimetres, orients north-up, and handles each source's
particular quirks.

**Here.** `normalize.py`, doing the same four things through alias tables.

**What is preserved.** This is the closest correspondence in the project. The
decision to buy uniformity once at ingest rather than handling conventions at
each point of use is exactly OCS's, and it is what makes OCS's API uniform
across sources.

**What is simplified.** No `standard_name` handling and no real unit library.
OCS can and should key conversions on CF standard names; this project keys on
variable name, which is why `convert_units` has an `and name == "tp"` guard on
the metres branch. No missing-data sentinel handling. No calendar conversion. No
regridding -- if two sources arrive on different grids, OCS has to reconcile
them, and this project never has two grids.

### Stage: ingest and store

**OCS.** Streaming ingest, one period at a time, committing each. Resume from
committed time steps. Stores at `{data_dir}/downloads/{dataset_id}.icechunk`.

**Here.** `ingest.py`, doing the same, with `store_path` mirroring the layout
exactly.

**What is preserved.** All of it, structurally. Per-period commits, resume
derived from the store's time coordinate, the report of ingested/skipped/failed,
`align_chunks=True` on append. This is the part of the project that is closest
to being a real implementation rather than a model of one.

Two things in this repository are deliberate re-implementations of OCS code,
kept close to the original: the `_uniform_chunks` fix in
`dask/examples/0601_zarr_legal_chunks.py`, and the open-or-create plus
commit-and-append pattern in `icechunk/src/ocs_stack_icechunk/helpers.py`. The
`ingest_period` function here is the same pattern.

**What is simplified.** Local filesystem storage only, which is also true of OCS
today and is the subject of one of its planned extensions -- see
[Storage](../storage.md) and [Open Climate Service](../open-climate-service.md)
for why that has to change when compute spans machines. No concurrency: one
process, one writer, no fork/merge across dask workers. No garbage collection or
snapshot expiry, which becomes a real operational task at forty years of monthly
commits.

### Stage: derive

**OCS.** Exposes derivations as **openEO** process graphs, executed on dask.
openEO is an API standard for earth observation processing: a client sends a
JSON process graph describing a computation, the server plans and executes it,
and the result comes back without the client ever handling the underlying data.
That is what makes an OCS derivation composable and remotely executable.

**Here.** `indices.py`, plain Python functions.

**What is preserved.** The *shape* of what gets computed: small reductions over
the stored series producing the products people actually ask for. Climatological
normals, anomalies, threshold counts, standardized indices, and the pyramid
downsampling.

**What is deliberately omitted.** openEO entirely. There is no process graph, no
JSON, no remote execution, no composability. The functions are functions.

That is the single largest gap between this project and OCS, and it is
deliberate: openEO is an API standard, and studying an API standard by
reimplementing it teaches you the standard, not the domain. Implementing the
arithmetic directly puts `groupby("time.month").mean()` on the page where you
can see it. The `dask` project's phases 1 through 3 cover the execution model
that openEO process graphs run on.

Also omitted: the real index catalogue. OCS should use, and any real service
must use, [xclim](https://xclim.readthedocs.io/) rather than five hand-rolled
functions.

### Stage: publish

**OCS.** Writes GeoZarr root attributes so clients can place the grid, builds
multiscale pyramids by mean downsampling and **writes them into the store** as
sibling groups, and publishes a STAC collection per dataset at
`/stac/collections/{id}`.

**Here.** `publish.py` builds the GeoZarr attributes and the STAC document.
`0301_geozarr` writes the attributes onto the store root and commits them.
`pyramid_levels` computes the levels.

**What is preserved.** The attribute set, the pixel-registered transform, the
array-order axis naming, the derived extents, and the property that the STAC
bbox and the GeoZarr bbox are the same numbers.

**What is deliberately omitted.**

*Pyramids are not written into the store.* `pyramid_levels` returns a list of
in-memory datasets and nothing persists them as GeoZarr multiscale groups
(`0/`, `1/`, `2/` with a `multiscales` root attribute). `0203_pyramid` describes
the target layout and stops there. So the stores this project produces are valid
GeoZarr at a single scale and are not multiscale datasets.

*No HTTP serving.* OCS serves STAC over HTTP and Zarr over HTTP so a client can
open the store remotely. This project builds the documents and prints them. The
`assets.zarr.href` field points at `https://climate.example.org/...`, which is
a placeholder. The `links` array is empty because there is no serving URL to
build `self`, `root`, and `parent` links against.

*No catalogue endpoint.* `0402_second_dataset` prints what
`GET /stac/collections` would return by formatting two documents into a table.
There is no server.

### Summary table

| OCS does this | Here |
|---|---|
| Source plugins for CHIRPS, ERA5, with auth and retries | `sources.py`, synthetic, seeded with crc32 |
| Enumerate periods, fetch one at a time | Preserved exactly: `enumerate_periods` plus `Fetcher` |
| Normalize to `(time, y, x)`, K to degC, north-up | Preserved: `normalize.py` |
| Key unit conversions on CF `standard_name` | Simplified: keyed on variable name |
| Streaming ingest, one commit per period | Preserved: `ingest_period` |
| Resume from committed time steps | Preserved: `committed_periods` |
| Rechunk to Zarr-legal chunks before writing | Preserved: `align_chunks=True` |
| Store at `{data_dir}/downloads/{id}.icechunk` | Preserved: `store_path` |
| Derivations as openEO process graphs on dask | Omitted: plain functions in `indices.py` |
| xclim for the index catalogue | Omitted: five hand-rolled functions |
| Multiscale pyramids written into the store | Partial: computed, not written |
| GeoZarr root attributes | Preserved: `geozarr_attrs`, committed in `0301` |
| STAC collection per dataset | Preserved: `stac_collection` |
| STAC and Zarr served over HTTP | Omitted entirely |
| Object storage, distributed writes | Omitted: local filesystem, single writer |

### The one trap worth carrying back to OCS

Appending variable-length months to a store chunked at 30 days along time fails
outright once the final chunk is partial:

```text
ValueError: Specified Zarr chunks encoding['chunks']=(30, 12, 12) for variable
named 't2m' would overlap multiple Dask chunks.
```

It is not a corner case. It appeared independently in this project and in the
`icechunk` project, on the same period -- the fifth month -- in both. The fix is
`align_chunks=True` on the append, and both projects demonstrate it rather than
working around it. `icechunk/examples/0401_append_periods.py` probes each period
and reports honestly which ones would have succeeded unaligned.

---

## Where to go next

This project is the capstone, which means it assumes the other four. If a stage
here was opaque, the project that teaches it is:

- **[xarray](xarray.md)** -- the data model everything here is built on.
  `normalize.py` is `rename`, `isel`, `transpose`, and attribute handling;
  `indices.py` is `groupby`, `resample`, and `coarsen`. Start with
  `0303_groupby_climatology` and `0305_rolling_coarsen`, which are the direct
  ancestors of `climatological_normal` and `pyramid_levels`.

- **[dask](dask.md)** -- why chunk shape decides everything and what
  `align_chunks=True` is reconciling. `0601_zarr_legal_chunks` is the same
  family of problem as the alignment failure here; `0602_chunk_sizing` is the
  argument behind `TIME_CHUNK` and `SPATIAL_CHUNK_CAP`.

- **[icechunk](icechunk.md)** -- the transactional store underneath.
  `0401_append_periods` is the per-period commit pattern, `0402_resume` is
  resume-from-store, and `0501_storage_growth` measures what appends versus
  rewrites actually cost.

- **[dask-distributed](dask-distributed.md)** -- what changes when compute spans
  machines, which is the extension this project's single-process,
  single-filesystem design does not attempt.

- **[Open Climate Service](../open-climate-service.md)** -- the real system, the
  stage-by-stage mapping, and the two extensions being planned for it: icechunk
  on S3 and distributed dask.

- **[climate-pipeline API reference](../reference/climate-pipeline.md)** -- every
  public function with its full docstring, generated by mkdocstrings from the
  source.

Also worth reading in this documentation: **[The stack](../stack.md)** for how
xarray, dask, zarr, and icechunk fit together; **[Storage](../storage.md)** for
the local-filesystem-versus-object-store question; **[Scaling](../scaling.md)**
for where the single-machine assumptions break; and
**[Conventions](../conventions.md)** for the project template every directory
here follows.

If you want to extend this project rather than read further, the gaps that would
teach the most are, roughly in order of value:

1. **Write the pyramid into the store** as GeoZarr multiscale groups, with a
   `multiscales` root attribute. That closes the largest structural gap and
   forces you to deal with per-level transforms.
2. **Handle missing data.** Add `_FillValue`-to-NaN conversion to `normalize`
   and confirm that every reduction in `indices.py` does the right thing with it.
3. **Compute indices lazily against the store** rather than loading. Everything
   in `indices.py` already works on dask-backed arrays; the examples just happen
   to materialize immediately when they print. Check that the graphs are what
   you expect.
4. **Maintain min/max at ingest** as store attributes, so `stac_collection` does
   not scan the archive -- and then confront the fact that you have just
   introduced a declared value that can drift.
5. **Replace `indices.py` with xclim** and see how much of the module survives.

---

## Further reading

### The system this models

- **[open-climate-service](https://github.com/dhis2/open-climate-service)** --
  the DHIS2 climate data platform this project is modelled on. Country-scoped
  instances, real sources, GeoZarr in icechunk, STAC and openEO on top.
- **[chapkit](https://github.com/dhis2-chap/chapkit)** -- the project template
  every directory in this repository follows.

### Specifications

- **[CF conventions](https://cfconventions.org/)** -- the metadata agreement:
  `units`, `standard_name`, `long_name`, `axis`, `calendar`, `cell_methods`,
  `_FillValue`. The
  [standard name table](https://cfconventions.org/standard-names.html) is the
  controlled vocabulary that makes a variable self-describing.
- **[GeoZarr spec](https://github.com/zarr-developers/geozarr-spec)** -- the
  root attributes that place a Zarr store on Earth, and the multiscale
  convention for pyramids. Small, readable, and still evolving.
- **[STAC](https://stacspec.org/)** -- the SpatioTemporal Asset Catalog
  specification. The [Collection spec](https://github.com/radiantearth/stac-spec/tree/master/collection-spec)
  is the document `stac_collection` builds.
- **[openEO](https://openeo.org/)** -- the process-graph API standard OCS
  exposes its derivations through, and the largest thing this project omits.
- **[Zarr v3 specification](https://zarr-specs.readthedocs.io/)** -- the array
  format underneath everything, including the chunk grid and codec pipeline.

### Libraries

- **[xclim](https://xclim.readthedocs.io/)** -- the real climate index library.
  Hundreds of standardized indices from ETCCDI, ICCLIM, and the agricultural and
  health literature, with proper unit handling via `pint`, calendar handling via
  `cftime`, and dask-aware implementations. **Use this instead of `indices.py`
  for anything real.** Its
  [indicator list](https://xclim.readthedocs.io/en/stable/indicators.html) is
  also an excellent survey of what a climate service can publish.
- **[xarray](https://docs.xarray.dev/)** -- the labeled array library.
  [Computation](https://docs.xarray.dev/en/stable/user-guide/computation.html)
  and [GroupBy](https://docs.xarray.dev/en/stable/user-guide/groupby.html) are
  the pages behind `indices.py`; the
  [Zarr I/O guide](https://docs.xarray.dev/en/stable/user-guide/io.html#zarr)
  covers `append_dim`, `region`, and `align_chunks`.
- **[icechunk](https://icechunk.io/)** -- transactional versioned storage for
  Zarr. The
  [concepts page](https://icechunk.io/en/latest/concepts/) explains snapshots,
  branches, and the fork/merge model for distributed writes.
- **[dask](https://docs.dask.org/)** -- chunked parallel arrays.
  [Best practices for arrays](https://docs.dask.org/en/stable/array-best-practices.html)
  is the source of the chunk-sizing guidance behind `TIME_CHUNK` and
  `SPATIAL_CHUNK_CAP`.
- **[pint-xarray](https://pint-xarray.readthedocs.io/)** -- real unit handling
  with dimensional analysis, which is what `convert_units` is a hand-rolled
  approximation of.
- **[xesmf](https://xesmf.readthedocs.io/)** -- regridding, including
  conservative remapping, which is what you need when two sources arrive on
  different grids and this project never does.
- **[stackstac](https://stackstac.readthedocs.io/)** and
  **[pystac](https://pystac.readthedocs.io/)** -- reading STAC catalogues into
  xarray, and building STAC documents with validation rather than by hand.

### Data sources this stands in for

- **[ERA5](https://cds.climate.copernicus.eu/)** -- ECMWF reanalysis, and the
  reason `fetch_temperature` publishes Kelvin and `fetch_precipitation`
  publishes metres. Recent CDS downloads call the time axis `valid_time`, which
  is why that alias is in `TIME_ALIASES`.
- **[CHIRPS](https://www.chc.ucsb.edu/data/chirps)** -- Climate Hazards Group
  InfraRed Precipitation with Station data, a satellite-and-gauge rainfall
  product widely used in West Africa and one of the sources OCS ingests.

### Background

- **[WMO guidance on climatological normals](https://library.wmo.int/idurl/4/55797)**
  -- why thirty years, and why the baseline period is part of the contract.
- **[Pangeo](https://pangeo.io/)** -- the community that produced most of this
  stack and most of the thinking about what ARCO means in practice.
- **[Standardized Precipitation Index](https://library.wmo.int/idurl/4/39629)**
  -- the WMO user guide, including the gamma fit that `spi_like` skips.
- **[ETCCDI climate indices](http://etccdi.pacificclimate.org/list_27_indices.shtml)**
  -- the standard 27 indices, which is where `hot_days` and `wet_days` come from
  in their proper forms.
