# dask

**Task graphs and chunks.** This project is a 22-example course in dask, built
from first principles and aimed at one destination: understanding how
[open-climate-service](../open-climate-service.md) actually executes. It starts
with `dask.delayed` and the task graph, works up through `dask.array`,
schedulers, `dask.dataframe` and dask-backed xarray, and ends in the production
chunking patterns that decide whether a climate pipeline finishes in seconds or
never finishes at all. Everything in it runs locally, on synthetic
`(time, y, x)` fields shaped like a real zarr store, and every claim it makes is
backed by a number the example prints.

---

## Introduction to dask

This section assumes you have never used dask. It is long on purpose: almost
every mistake people make with dask comes from an incorrect mental model rather
than from a missing API call, and the model is small enough to explain properly
in one sitting.

### The problem dask solves

Start from the thing dask is not trying to replace. numpy is excellent. It is
fast, it is well understood, and its API is the lingua franca of scientific
Python. It has exactly one structural limitation: a numpy array is a single
contiguous buffer of memory, and every operation on it produces another single
contiguous buffer. If your array does not fit in RAM, numpy has nothing to
offer you. If it fits but only barely, numpy will still allocate a full-size
temporary for every intermediate expression, and `(a * 9 + 20) - mean` becomes
three full copies.

pandas has the same shape of limitation with a different constant factor. A
`DataFrame` lives in memory, and a groupby over a table larger than RAM is not
slow, it is impossible.

There is a second limitation that bites even when memory is fine: neither numpy
nor pandas parallelizes across cores in any general way. Individual BLAS calls
may use multiple threads, and a handful of ufuncs are vectorized, but
`arr.mean(axis=0)` on a 191 MB array is a single-threaded pass on a 12-core
machine. Eleven cores sit idle.

dask addresses both by doing the same thing to both problems: it splits the data
into pieces, runs the operation on each piece, and combines the results. The
pieces are called **chunks** in the array world and **partitions** in the
dataframe world. Each piece is a real numpy array or a real pandas DataFrame.
dask never reimplements the math -- it reimplements the *bookkeeping* around the
math.

That bookkeeping is what makes the two problems go away at once:

- Only a few chunks need to be in memory at any moment, so an array larger than
  RAM can be processed in a streaming fashion, chunk by chunk.
- Chunks are independent, so several can be processed at the same time, on
  different threads, different processes, or different machines.

Concretely, one of this project's examples reduces a 191 MB field with
`((x - 0.5) ** 2).mean()` and reports peak process memory of 281 MB rather than
the roughly 700 MB the naive numpy version would touch (see
[0304](#0304-diagnostics)). Another runs the same graph on three different
schedulers and shows a numpy-heavy reduction dropping from 0.55 s to 0.08 s just
by switching executors (see [0301](#0301-schedulers)). Neither result required
rewriting the math.

The third thing dask solves, which is less advertised but matters most in
practice, is **describing work without doing it**. Because dask records what you
asked for before it executes anything, it can look at the whole request and
rewrite it: drop the chunks a later slice never reads, fuse adjacent elementwise
operations, compute a shared subexpression once instead of twice. In this
project, `arr[0].sum()` on a 52-chunk array executes 9 tasks instead of 117
because dask saw the slice before it saw the data (see
[0202](#0202-blocked-algorithms)).

### The task graph mental model

Everything in dask is one idea wearing different clothes.

A **task** is a Python function plus its arguments. A **task graph** is a
dictionary mapping unique keys to tasks, where an argument may be another key --
that is the edge. A **scheduler** takes a graph and a set of keys you want, and
executes the tasks in dependency order, in parallel where the graph allows.

That is the whole system. Everything else -- `dask.array`, `dask.dataframe`,
`dask.bag`, dask-backed xarray, `dask-geopandas`, `openeo-processes-dask` -- is a
*front end* that builds one of these dictionaries for you.

You can see the dictionary. Every dask collection implements
`__dask_graph__()`, and this project's `task_count` helper is nothing more than
`len(dict(obj.__dask_graph__()))`. Building `a = inc(1); b = inc(2);
total = add(a, b)` with `dask.delayed` produces a three-key graph whose keys are
printed verbatim in [0102](#0102-task-graphs):

```
add-07e1ca93-3558-47c5-83a6-dcb309c70c42
inc-1504c7b0-71e6-4e66-976f-77a09d1638a5
inc-c67c9862-3a6f-42da-b313-ef0c377f6aa9
```

and whose dependency edges print as:

```
add-07e1ca <- inc-1504c7, inc-c67c98
inc-1504c7   (leaf: no dependencies)
inc-c67c98   (leaf: no dependencies)
```

The key format is deliberate. The prefix before the hyphen is the *task name*,
which is almost always the function being applied; the token after it makes the
key unique. Array chunks use tuple keys instead: `(name, i, j, k)` where the
integers are the block index along each dimension. When you stare at a dask
dashboard, or at a profiler dump, or at a traceback from a worker, you are
reading these names. Learning to read them is a large fraction of learning to
debug dask.

Two consequences fall out of the model immediately, and both are load bearing.

**Identical keys are the same task.** If two outputs both depend on key
`load_field-849c...`, that key exists once in the merged graph, and the
scheduler runs it once. This is the entire mechanism behind sharing work. It is
also the entire mechanism behind *failing* to share work: if you build the same
logical step twice, you get two different tokens, two different keys, and two
executions. [0103](#0103-sharing-intermediates) demonstrates both halves with a
call counter.

**The graph is the program.** Work that is not in the graph is never scheduled.
This sounds tautological until you notice its practical form: `arr[:10].mean()`
and `arr.compute()[:10].mean()` produce the same number, but the first prunes
the graph and the second prunes the result after paying for all of it.
[0603](#0603-graph-hygiene) measures the difference at 8.5x on a 191 MB array.

### Lazy versus eager

An **eager** API runs when you call it. Plain Python is eager; numpy is eager;
pandas is eager. A **lazy** API records what you asked for and returns a handle.

dask collections are lazy. `arr * 9 + 20` does not multiply anything -- it
appends tasks. In [0203](#0203-lazy-pipelines), three pipeline steps on a
191 MB array take a combined 3.33 ms (machine-dependent), because the only thing
that happened was 293 dictionary insertions:

```
  step 1  scaled = arr * 9 + 20            0.61 ms, tasks=156
  step 2  anomaly = scaled - time-mean     1.64 ms, tasks=280
  step 3  series = spatial mean            1.07 ms, tasks=345
  three steps of 'work' on a 191 MB array took 3.33 ms -- no data was touched
```

The moment of execution is explicit and is one of a small set of calls:

| Call | What it does | What you get back |
|---|---|---|
| `.compute()` | run the graph, return the concrete result | numpy / pandas object |
| `dask.compute(a, b, ...)` | merge several graphs, run once | tuple of concrete results |
| `.persist()` | run the graph, keep chunks in memory | still a dask collection |
| `.load()` (xarray) | run and replace the data in place | the same object, now eager |
| `.values`, `float()`, `len()`, `print(df)` | implicit compute | varies -- often a surprise |

That last row is where laziness bites beginners. Any operation that needs a
concrete value will silently trigger a full computation. `float(x.mean())` is a
compute. Plotting is a compute. In a notebook, so is repr-ing something that
needs its length.

Laziness buys three things:

1. **Optimization.** dask rewrites the graph before running it. The visible
   forms are *culling* (drop tasks whose results nobody needs) and *fusion*
   (merge chains of one-in-one-out tasks into a single task so intermediates are
   never materialized separately). [0102](#0102-task-graphs) shows culling take
   a 17-task graph down to 9.
2. **Streaming.** Because dask knows the dependency structure, it can release a
   chunk's memory the moment its consumers have run. This is why peak memory
   during a reduction is a small multiple of chunk size rather than the size of
   the array.
3. **Composition.** Lazy things compose without cost. You can build a pipeline
   five layers deep, hand it to a function that adds three more layers, and still
   pay nothing until the end.

Laziness costs one thing, and it is a real cost: **your intermediates do not
exist**. Every `.compute()` re-runs its graph from scratch. In
[0503](#0503-compute-load-persist), the same reduction over a lazy anomaly takes
145.9 ms and then 142.6 ms -- there is no cache. `.persist()` exists precisely to
buy that cache back at the price of RAM, dropping the repeat to 22.3 ms
(machine-dependent).

### dask.array and numpy

A `dask.array.Array` is a grid of numpy arrays plus a graph that knows how to
produce each one. It advertises `.shape`, `.dtype`, `.ndim`, and supports a large
subset of the numpy API: slicing, elementwise arithmetic, ufuncs, reductions,
`matmul`, `stack`/`concatenate`, `where`, most of `linalg` for suitably chunked
inputs. Where numpy has `arr.mean(axis=0)`, dask has `arr.mean(axis=0)`, and the
result is identical:

```
  arr.mean() vs npa.mean():        np.allclose = True
```

The extra vocabulary is all about blocks:

| Attribute | Meaning | Example value from [0201](#0201-chunked-arrays) |
|---|---|---|
| `.chunks` | per-axis tuple of block sizes | `((30,)*12 + (5,), (128, 128), (128, 128))` |
| `.numblocks` | blocks per axis | `(13, 2, 2)` |
| `.npartitions` | total blocks | `52` |
| `.blocks[i, j, k]` | one block, itself a dask array | `shape=(30, 128, 128)` |

Note the first entry of `.chunks`: `(30, 30, ..., 30, 5)`. 365 days do not divide
evenly into 30, so the last chunk is a 5-day remainder. **dask allows any chunk
tuple at all** -- uneven, ragged, whatever the operations happen to produce. That
freedom is convenient right up until you try to write the array to zarr, which
requires uniform chunks except the last. That collision is the subject of
[0601](#0601-zarr-legal-chunks) and of the
[chunking deep section](#chunking-the-deep-section).

What dask.array does *not* give you:

- **Value-dependent shapes.** dask needs to know output shapes before running.
  `arr[arr > 0]` gives an array with `nan` in its shape, and many downstream
  operations refuse to work with it.
- **Efficient elementwise item assignment.** `arr[i, j] = v` exists but is not
  the tool you want; build the array from expressions instead.
- **Fast small-array operations.** Every operation costs graph construction. On
  a 10x10 array, dask is pure loss.
- **Sorting along a chunked axis** without a full shuffle, and several other
  operations that need global order.

The rule for reaching for dask.array: your array is larger than a comfortable
fraction of RAM, *or* it is on disk in a chunked format (zarr, netCDF, TIFF
pyramids) and you only want part of it, *or* the operation is heavy enough that
using all cores is worth the overhead. Otherwise use numpy.

### dask.dataframe and pandas

A `dask.dataframe.DataFrame` is a list of pandas DataFrames -- the
**partitions** -- split along the index, plus a tuple of **divisions** recording
the index boundaries. From [0401](#0401-partitions):

```
  .npartitions = 4
  .divisions   = (0, 25000, 50000, 75000, 99999)
```

`divisions[i] <= index <= divisions[i+1]` for partition `i`, with the final edge
inclusive. `ddf.partitions[0].compute()` returns a `DataFrame` -- an entirely
ordinary pandas object, no wrapper.

Divisions are metadata that lets dask skip work. With known divisions,
`ddf.loc[60_000]` builds a 6-task graph because dask can prove which single
partition owns that index. With `clear_divisions()` applied, the same lookup
becomes 12 tasks because every partition must be checked. On real data with
hundreds of partitions the ratio is far larger.

The dataframe API splits into three cost classes, which
[0402](#0402-groupby-shuffle) measures directly:

1. **Per-partition (embarrassingly parallel).** Column arithmetic, filters,
   `assign`, `map_partitions`. No data crosses a partition boundary. Cost grows
   linearly with partition count and nothing else.
2. **Tree reductions.** `groupby(...).mean()`, `sum()`, `count()`,
   `value_counts()`. Each partition produces a small partial result; the partials
   are combined. Raw rows never move -- only the partials, which are tiny. In the
   example, a groupby over 100k rows in 4 partitions is 13 tasks, *fewer* than the
   24-task column assignment, because the output is small.
3. **Shuffles.** `set_index`, `sort_values`, joins on unaligned keys, `groupby`
   with `apply` over a non-partition-aligned key. Every output partition may need
   rows from every input partition. In the example, `sort_values` is 34 tasks and
   `set_index` is 38, and on a cluster those tasks mean network traffic and
   spill-to-disk.

The single most useful thing to know about dask.dataframe is which class an
operation falls into, because class 3 is where distributed jobs die.

Where dask.dataframe genuinely earns its place is data that starts on disk.
`dd.read_parquet(path, columns=["station_id", "rainfall"])` never reads the other
columns at all -- the projection is pushed into the file reader.
[0403](#0403-pandas-boundary) shows the round trip.

This project touches dataframes lightly (three examples) because the climate
stack is array-shaped. The reason it touches them at all is `dask-geopandas`,
which open-climate-service uses for vector aggregation, and which is partitioned
`dask.dataframe` underneath.

### Collections versus futures

dask offers two genuinely different programming models, and choosing the wrong
one produces awkward code.

**Collections are lazy and declarative.** `delayed`, `array`, `dataframe`, `bag`,
and every xarray object backed by them. You describe the whole computation, hand
it over once, and dask optimizes and executes it as a unit. The scheduler sees
the entire plan, so it can cull, fuse, and order work intelligently. This is the
right model whenever the shape of the computation is known up front -- which,
for a data pipeline, is nearly always.

**Futures are eager and imperative.** `client.submit(fn, *args)` ships the call
to a worker *immediately* and hands back a `Future`. The work is already running
before the next line of your program executes. `client.map` fans one function
over many inputs; `client.gather` collects results; `as_completed` lets you react
to whichever finishes first. From [0303](#0303-futures):

```
  right after submit: fut_a.status='pending', fut_b.status='pending'
  ...main thread now does other work while both tasks run on the workers...
  after result(): fut_a.status='finished', results a=36, b=49
```

and the contrast, run side by side:

```
  future after 0.6 s of doing nothing: status='finished' -- it ran anyway
  delayed after the same wait: Delayed('slow_square-e23906e5-...') -- still just a graph, nothing ran
```

Futures require a `distributed` client; there is no futures interface to the
local threaded scheduler. They are the right model when the computation is
*dynamic*: when what you submit next depends on results you have already
received, when you are serving requests and want work started before you know
what else will arrive, when you need a long-lived background job with a handle
you can cancel.

A service layer typically uses both: futures at the outer edge to manage
concurrency and lifecycle, collections inside each task to describe the actual
data work.

### When dask is the wrong tool

This deserves its own section because the failure mode is common, quiet, and
expensive: people reach for dask because the word "big" appeared in a
requirements document, and end up with a slower program and a harder-to-debug
one.

**If the data fits comfortably in memory, dask is overhead with nothing to
amortize it.** [0403](#0403-pandas-boundary) does the measurement on a 3.2 MB
table:

```
  in-memory size: ~3.2 MB -- pandas territory
  same groupby-mean:  pandas    1.1 ms
                      dask     10.8 ms  (~10x slower)
```

Ten times slower, on identical input, for identical output. The 9.7 ms went to
building a graph, optimizing it, scheduling four partitions, and concatenating
four small results. (Machine-dependent, but the ratio is stable and the sign
never changes.)

Quantifying the overhead more generally: **budget roughly 1 ms of scheduler
overhead per task on the distributed scheduler, and a few hundred microseconds on
the local threaded scheduler.** This is dask's own published rule of thumb and
this project reproduces it. In [0602](#0602-chunk-sizing), the "tiny" layout
runs 19,377 tasks in 1.27 s of wall time on 12 cores -- about 65 microseconds of
wall time per task, which is around 800 microseconds of per-core time per task
once you account for the parallelism. The actual arithmetic in those tasks
(`2 * x + 1` over a 5x32x32 block, then a partial mean) is a few microseconds.
Over 95% of that run was bookkeeping.

The practical thresholds:

- **Under ~1 GB and in memory already**: use numpy or pandas. Full stop.
- **1-10 GB**: numpy/pandas still usually wins if you have the RAM. Consider dask
  if you want the cores, and measure before committing.
- **Larger than RAM, or on disk in a chunked format, or genuinely distributed
  across machines**: dask.
- **Any size, but the pipeline is a long chain of expensive elementwise
  operations over many cores**: dask can win on parallelism alone, but check that
  numpy with a threaded BLAS or `numexpr` is not simpler.

There is a second wrong-tool case that is subtler: **dask is a poor fit for
workloads dominated by pure-Python bytecode**. The default threaded scheduler
cannot help, because the GIL serializes it. [0301](#0301-schedulers) shows a
pure-Python workload taking 8.08 s synchronously and 8.23 s on threads -- threads
were *worse than useless*, having added overhead and delivered nothing. If your
per-task work is Python-level rather than numpy/pandas-level, you need processes
or a distributed cluster, and at that point the serialization costs deserve
scrutiny.

And a third: **dask is not a database.** Random access by key, transactional
updates, and small point queries are not what a task graph is for. If your
access pattern is "fetch one row by id, ten thousand times a second", you want
something else.

The honest summary is the one the dask maintainers themselves write in the best
practices guide: start with the simple thing, and reach for dask when the simple
thing has actually stopped working.

### Reading the official documentation

The upstream docs are good, and this project deliberately does not duplicate
them. Four pages carry most of the value:

- <https://docs.dask.org/> -- the documentation root. The "Deploy" and
  "Diagnostics" trees are the parts you will come back to.
- <https://docs.dask.org/en/stable/10-minutes-to-dask.html> -- the fastest
  correct orientation. Read it before or immediately after this page's core
  concepts section; it covers the same collections in a quarter of the words.
- <https://docs.dask.org/en/stable/best-practices.html> -- short, dense, and
  worth re-reading every few months. Most of the [pitfalls
  section](#pitfalls-and-gotchas) below is this page with measurements attached.
- <https://docs.dask.org/en/stable/array-chunks.html> -- the definitive treatment
  of chunk selection. The [chunking deep
  section](#chunking-the-deep-section) below assumes you have read it.

---

## Setup

Everything lives in the `dask/` directory of this repository and is managed with
`uv`.

```bash
cd dask
make install
```

`make install` is `uv sync`: it creates `.venv/` and installs `dask[complete]`,
`distributed`, `xarray`, and `zarr` pinned by `uv.lock`. Nothing else on your
system is touched.

Run one example:

```bash
make run EXAMPLE=0101_delayed_basics
```

The target refuses to run without `EXAMPLE`, and the name is the file stem, no
`.py` and no `examples/` prefix. Under the hood it is
`uv run python examples/0101_delayed_basics.py`, which is also perfectly fine to
type directly when you want to tweak an example and re-run it quickly:

```bash
cd dask
uv run python examples/0602_chunk_sizing.py
```

Run everything, in order, stopping at the first failure:

```bash
make run-all
```

This takes a couple of minutes. Two examples ([0302](#0302-local-cluster) and
[0303](#0303-futures)) start a `LocalCluster` and bind a dashboard port; both
fall back gracefully to no dashboard if the port cannot be bound, so `run-all`
works on a locked-down machine too.

Tests and checks:

```bash
make test        # pytest over tests/
make lint        # ruff format, ruff check --fix, mypy, pyright
make ci          # lint + test
make coverage    # pytest under coverage, with a report
```

`make test` covers `src/`, not the examples -- the examples are the
documentation, and their job is to print evidence rather than to assert. The
`tests/test_helpers.py` suite pins the helper behaviour the examples depend on:
that `random_field` is deterministic for a seed, that its chunk tuple comes out
as `(30, 30, ..., 5)` for 365 days, that `task_count` grows when you add
operations.

### The shared helpers

Every example imports from `src/climate_stack_dask/helpers.py`, so all 22 lessons
start from the same shape of data. There are three functions.

**`random_field(days=365, ny=256, nx=256, time_chunk=30, spatial_chunk=128,
seed=0) -> da.Array`** returns a lazy `(time, y, x)` float64 array of uniform
random values, chunked the way a climate zarr store would be. It is the stand-in
for "a year of daily gridded data" throughout the project. It validates its
inputs (`days`, `ny`, `nx` must be at least 1) and it is seeded, so two calls
with the same seed produce identical values -- which is what lets examples
compare a dask result against a numpy reference.

```python
rng = da.random.default_rng(np.random.default_rng(seed))
chunks: Any = (time_chunk, spatial_chunk, spatial_chunk)
arr: da.Array = rng.random((days, ny, nx), chunks=chunks)
```

The defaults give a 191 MB array in 52 chunks of about 3.9 MB each -- small
enough to run anywhere, structured enough that every axis has interior chunk
boundaries.

**`chunk_report(arr) -> str`** is the one-line layout summary that appears
throughout the examples:

```python
first = tuple(c[0] for c in arr.chunks)
n_chunks = int(np.prod([len(c) for c in arr.chunks]))
mb = float(np.prod(first)) * arr.dtype.itemsize / 1e6
return f"shape={arr.shape}, chunks={first}, n_chunks={n_chunks}, ~{mb:.1f} MB/chunk"
```

producing, for the default field:

```
shape=(365, 256, 256), chunks=(30, 128, 128), n_chunks=52, ~3.9 MB/chunk
```

Note the honest limitation baked into the docstring: the MB figure uses the
*first* chunk. For ragged layouts the last chunk is smaller, and for a layout
where the first chunk is the odd one out the number is misleading. It is a
reporting aid, not a metric.

**`task_count(obj) -> int`** is the graph-size probe:

```python
graph = obj.__dask_graph__()
return len(dict(graph))
```

It works on anything implementing the dask collection protocol -- `Delayed`,
`dask.array.Array`, `dask.dataframe.DataFrame`, and xarray objects backed by
dask. It counts keys in the *unoptimized* graph, which is exactly what you want
for watching a graph grow as you chain operations, and exactly what you do not
want for predicting execution cost. Several examples pair it with
`dask.base.optimize` to get the post-culling count:

```python
def optimized_task_count(obj: Any) -> int:
    (optimized,) = optimize(obj)
    return task_count(optimized)
```

### Environment used for the outputs on this page

Every output block quoted below was produced by actually running the example. For
reference, and because the timing numbers depend on it:

| Component | Version |
|---|---|
| Python | 3.13.14 |
| dask | 2026.7.1 |
| distributed | 2026.7.1 |
| xarray | 2026.7.0 |
| zarr | 3.3.0 |
| numpy | 2.5.2 |
| pandas | 3.0.5 |
| Machine | macOS, 12 cores |

!!! note "Timings vary"
    Every wall-clock number, ratio, and speedup on this page is
    machine-dependent and varies between runs -- sometimes substantially, since
    these are single measurements rather than benchmarks. Task counts, chunk
    layouts, shapes, and error messages are stable. Trust the structure of the
    numbers, not their third digit.

---

## Core concepts

Six ideas carry the whole library. Each one below has runnable code and real
output; the examples that follow expand on them.

### Task graphs

A dask graph is a mapping from keys to tasks. You can build one by hand and hand
it to a scheduler, though you almost never will:

```python
from dask.threaded import get

def inc(x: int) -> int:
    return x + 1

def add(x: int, y: int) -> int:
    return x + y

dsk = {
    "a": 1,
    "b": (inc, "a"),
    "c": (inc, "b"),
    "d": (add, "b", "c"),
}
print(get(dsk, "d"))
```

```
5
```

The tuple form `(function, arg, arg, ...)` is the classic dask task
specification: the first element is callable, the rest are arguments, and a
string that happens to be a key in the graph is substituted with that key's
result. Modern dask uses richer internal representations, but the semantics are
the same and the mental model is unchanged.

The important properties:

- **Keys are unique identifiers, and identity is by key.** Two tasks with the
  same key are one task.
- **The graph is a DAG.** Cycles are an error, not a loop.
- **Order is not specified.** Anything the dependency structure permits to run in
  parallel may run in parallel, in any order.
- **Tasks should be pure.** dask may run a task more than once (on a distributed
  cluster it certainly will, after a worker loss), may run it on a different
  machine, and may run several at once. Side effects, shared mutable state, and
  order dependence are all bugs waiting for a bad day.

You get graphs from collections, not by hand. `obj.__dask_graph__()` returns a
`HighLevelGraph`, a layered representation that keeps operations grouped rather
than fully expanded; `dict(graph)` materializes it into the flat key-to-task
mapping.

**Pitfall.** The unoptimized graph is not what runs. `dask.base.optimize` culls
unreachable tasks and fuses linear chains, and `compute()` calls it for you.
Reading a raw task count as an execution cost will mislead you, sometimes by an
order of magnitude -- see the 61-versus-9 gap in
[0202](#0202-blocked-algorithms).

### Delayed

`dask.delayed` is the general-purpose front end: it turns any Python function
into a graph builder.

```python
from dask.delayed import delayed

@delayed
def load(path: str) -> dict[str, float]:
    return {"value": 1.0}

@delayed
def transform(record: dict[str, float], factor: float) -> float:
    return record["value"] * factor

results = [transform(load(f"file-{i}"), 2.0) for i in range(3)]
```

Nothing has run. `results` is a list of `Delayed` objects, each carrying a
two-task graph. `dask.compute(*results)` runs all three in one merged graph.

Three rules make `delayed` behave:

1. **Wrap functions, not results.** `delayed(load)(path)` is right;
   `delayed(load(path))` calls `load` eagerly and then wraps a value.
2. **Reuse the object to reuse the work.** Assign the intermediate to a variable
   and pass that variable to every consumer. Calling `delayed(load)(path)` twice
   produces two different keys and two executions, no matter that the arguments
   match.
3. **Keep tasks meaningfully sized.** A delayed call that does 50 microseconds of
   work is a task whose overhead exceeds its content. Aim for tasks in the
   100 ms range; batch smaller units together.

`delayed` is what you use for code that is not array-shaped or table-shaped: a
per-file processing pipeline, a fan-out over parameter combinations, wrapping an
existing library that only speaks in whole objects.

### Chunks

A chunk is one block of a dask array; it is a real numpy array in memory when it
exists at all. The chunk layout is a property of the array, is chosen when the
array is created, and propagates through every operation until something changes
it.

```python
from climate_stack_dask import chunk_report, random_field, task_count

arr = random_field()  # (365, 256, 256), chunks (30, 128, 128)
print(chunk_report(arr))
print("chunks[0] =", arr.chunks[0])
print("numblocks =", arr.numblocks)
print("tasks     =", task_count(arr))
```

```
shape=(365, 256, 256), chunks=(30, 128, 128), n_chunks=52, ~3.9 MB/chunk
chunks[0] = (30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 5)
numblocks = (13, 2, 2)
tasks     = 52
```

Three facts that are easy to get wrong:

- **`.chunks` is a tuple of tuples, one per axis**, giving the size of every
  block along that axis. It is not a single shape. `chunks[0]` above has 13
  entries because time is split into 13 blocks.
- **Chunks need not be uniform.** 365 does not divide by 30, so the last time
  chunk is 5. dask does not care. zarr does -- see
  [0601](#0601-zarr-legal-chunks).
- **Chunk count sets task count.** Every chunk is at least one task for every
  operation applied to it. Doubling the number of chunks doubles the scheduler's
  workload for the entire downstream pipeline.

The single most consequential decision in a dask array pipeline is the chunk
shape, which is why this page has [an entire section on
it](#chunking-the-deep-section).

### Blocked algorithms

A blocked algorithm computes a global result from per-block results without ever
materializing the whole input. Reductions are the canonical case.
`arr.sum()` becomes: sum each chunk (52 tasks), combine the partials in a tree
(a dozen tasks), aggregate to a scalar (one task).

```python
arr = random_field()                     # 52 chunks
print(task_count(arr))                   # 52
print(task_count(arr.sum()))             # 118
print(task_count(arr.sum(axis=0)))       # 124
print(task_count(arr.sum(axis=(1, 2))))  # 117
```

```
52
118
124
117
```

The per-chunk partials are the same in all three cases; only the combine tree
differs, because the tree's shape follows which axes survive. Reducing over time
leaves a `(256, 256)` map that is still chunked into 4 blocks; reducing over
space leaves a `(365,)` series.

The tree matters. Combining 52 partials in a single task would make that task a
bottleneck and a memory spike; combining a few at a time keeps every task small
and lets the combines themselves run in parallel. `split_every` controls the
branching factor if you ever need to tune it.

Not every operation blocks cleanly. `map_blocks` gives you an escape hatch to run
arbitrary numpy code per block, but it is *block-local*: your function never sees
a value from a neighbouring chunk. Anything with a spatial or temporal stencil
(smoothing, gradients, rolling windows) needs `map_overlap`, which copies a halo
from each neighbour first. Skipping that step produces results that are wrong
exactly at chunk boundaries and nowhere else, which is a spectacularly annoying
class of bug. [0205](#0205-map_blocks-and-map_overlap) reproduces it and counts
the wrong cells.

### Schedulers

The graph and the executor are separate. The same graph runs on any scheduler.

| Scheduler | `scheduler=` | Parallelism | Data movement | Use for |
|---|---|---|---|---|
| Synchronous | `"synchronous"` / `"sync"` | none | none | debugging, profiling with `pdb` |
| Threads | `"threads"` (default for arrays) | threads in one process | shared memory, zero copy | numpy / pandas work that releases the GIL |
| Processes | `"processes"` | separate processes | pickle everything | pure-Python task bodies |
| Distributed | a live `Client` | processes, possibly many machines | serialize over the network | clusters, dashboards, futures, resilience |

Switching is one keyword:

```python
from dask.base import compute

(result,) = compute(graph, scheduler="synchronous")
(result,) = compute(graph, scheduler="threads")
(result,) = compute(graph, scheduler="processes")
```

or, for distributed, simply creating a `Client` -- which installs itself as the
process-wide default so that plain `.compute()` starts using the cluster with no
further changes.

The synchronous scheduler deserves more use than it gets. When a computation
fails with a confusing traceback, re-running it with `scheduler="synchronous"`
puts the failure on your own stack, in your own process, where a debugger works.

### The GIL

Python's global interpreter lock allows exactly one thread to execute Python
bytecode at a time. C extensions may release it while doing work that does not
touch Python objects, and numpy, pandas, and most of the scientific stack do
exactly that for their heavy loops.

This single fact explains the threaded scheduler's entire performance profile.
[0301](#0301-schedulers) puts the two cases side by side, on a 12-core machine:

```
  workload                 synchronous       threads     processes
  A numpy-heavy                  1.00x         0.14x         1.26x
  B pure-Python                  1.00x         1.02x         0.57x
```

Workload A is `random_field(...).sum()` -- generation and summation both happen
inside numpy's C code, the GIL is released, and 12 threads get a 7x speedup.
Workload B is a delayed `for` loop accumulating an integer -- pure bytecode, the
GIL is held throughout, and threads deliver a 2% *slowdown* over running
serially. Processes cut B roughly in half despite paying spawn and pickle costs.

The practical rule: **threads for arrays, processes or distributed for Python**.
If you cannot tell which you have, run both and look.

---
## Phase 1 — Delayed and task graphs

Three examples establish the core model: build a graph lazily, execute it later,
and let identical keys share work. Everything after this phase is a front end
that builds these same graphs for you.

### 0101 — Delayed basics

Source: [`../../dask/examples/0101_delayed_basics.py`](../../dask/examples/0101_delayed_basics.py)

**What it teaches.** That `dask.delayed` wraps an ordinary Python function so
that calling it *records* a task instead of running one, and that `.compute()` is
the moment execution happens. The proof is not an assertion -- it is a print
statement inside the function body that does not appear, plus timings that
separate graph construction from execution by three orders of magnitude.

The example uses a deliberately slow function with a loud side effect:

```python
WORK_SECONDS = 0.3

def slow_square(x: int) -> int:
    print(f"  [side effect] slow_square({x}) is RUNNING now")
    time.sleep(WORK_SECONDS)
    return x * x
```

Called normally, it prints and blocks. Wrapped, it does neither:

```python
lazy: Delayed = delayed(slow_square)(4)
print(f"  type={type(lazy).__name__}, repr={lazy!r}")
```

```
Plain Python is eager: calling slow_square(3) runs it on the spot.
  [side effect] slow_square(3) is RUNNING now
  result=9, and the call blocked for the full 0.3 s of work

delayed(slow_square)(4) builds a task instead of running one:
  type=Delayed, repr=Delayed('slow_square-2f7ef0ca-88cc-4db8-9973-a44497f60542')
  no '[side effect]' line printed above -- the function body never ran
  building was ~1461x faster than the eager call (it does no work)

.compute() hands the graph to a scheduler, which finally runs it:
  [side effect] slow_square(4) is RUNNING now
  result=16
  compute took ~1886x longer than building -- the work happens here
```

(The 1461x and 1886x ratios are machine-dependent; the meaningful part is the
order of magnitude, and that the side-effect line moves from the construction
step to the compute step.)

Look at the repr: `Delayed('slow_square-2f7ef0ca-...')`. That string is the graph
key. The prefix is the function name, the UUID makes it unique. Everything dask
prints about your computation -- profiler output, dashboard task names, worker
tracebacks -- is built from prefixes like this one, which is a good argument for
giving your functions descriptive names.

**Composition.** `Delayed` objects can be passed to other delayed calls, and the
graph grows:

```python
a: Delayed = delayed(inc)(1)          # inc(1) -> 2
b: Delayed = delayed(inc)(2)          # inc(2) -> 3
total: Delayed = delayed(add)(a, b)   # add(a, b) -> 5
```

```
  task_count(a)=1, task_count(total)=3 (total contains a and b)
  total.compute() = 5
```

`total` carries the whole subgraph, not a reference to something computed
elsewhere. This is why passing delayed objects around between functions works so
naturally: each one is self-contained.

**Multiple outputs, one pass.** `dask.compute` takes several collections and
merges their graphs before executing:

```python
doubled: Delayed = delayed(add)(total, total)
results: tuple[Any, ...] = compute(a, b, total, doubled)
```

```
  dask.compute(a, b, total, doubled) = (2, 3, 5, 10)
  one call, one merged graph -- shared work is not repeated (see 0103)
```

**Why it matters.** open-climate-service assembles an openEO process graph from
a JSON request and hands it to dask. That is exactly what happens here in
miniature: a description is built up front, then executed by a scheduler that
sees all of it at once. If you understand `delayed`, the rest of dask is
front-ends for producing the same structure with less typing.

**Traps.**

- `delayed(fn(x))` calls `fn` immediately and then wraps its return value. The
  correct form is `delayed(fn)(x)`. This mistake is silent -- your program still
  works, it just is not lazy.
- Side effects in delayed functions are unreliable. dask may run a task more
  than once, in another thread, or in another process. The `print` in this
  example is pedagogy, not a pattern.
- Control flow on a `Delayed` does not work as written. `if lazy > 3:` forces a
  computation to evaluate the condition, and `for x in lazy:` usually fails
  outright. Keep branching outside the graph, or put it inside a delayed
  function.
- One task per `delayed` call means graph size scales with how you decompose the
  problem. Ten thousand `delayed` calls of 1 ms each is a bad trade; batch them.

### 0102 — Task graphs

Source: [`../../dask/examples/0102_task_graphs.py`](../../dask/examples/0102_task_graphs.py)

**What it teaches.** How to open the box. The graph is a dictionary, its keys are
readable, its edges are recoverable, and both `Delayed` and `dask.array` carry
the same structure. The example builds a tiny delayed pipeline, prints its keys
and adjacency, then does the same for a chunked array, and finishes by showing
what the optimizer does before anything runs.

The graph itself:

```python
total: Delayed = delayed(add)(a, b)
graph = total.__dask_graph__()
print(f"  total.__dask_graph__() -> {type(graph).__name__}")
```

```
  total.__dask_graph__() -> HighLevelGraph
  task_count(total) = 3 (one task per delayed call: inc, inc, add)
```

`HighLevelGraph` is a layered structure -- it stores "an elementwise operation
over 52 chunks" as one layer rather than 52 entries, and only expands to the flat
dictionary when asked. `dict(graph)` does the expansion, which is what
`task_count` relies on.

Reading the dependency structure is a five-line function using
`dask.core.get_dependencies`:

```python
def print_adjacency(dsk: Mapping[Key, Any]) -> None:
    for key in sorted(dsk, key=str):
        deps = sorted(get_dependencies(dsk, key), key=str)
        arrow = " <- " + ", ".join(short(d) for d in deps) if deps else "   (leaf: no dependencies)"
        print(f"    {short(key)}{arrow}")
```

```
Every task has a key: '<function name>-<hash token>' for delayed calls.
  total.key = 'add-07e1ca93-3558-47c5-83a6-dcb309c70c42'
  all keys in the graph:
    add-07e1ca93-3558-47c5-83a6-dcb309c70c42
    inc-1504c7b0-71e6-4e66-976f-77a09d1638a5
    inc-c67c9862-3a6f-42da-b313-ef0c377f6aa9
  the token makes keys unique; the prefix tells you which function runs

The graph is tasks plus dependencies -- printed as an adjacency listing:
    add-07e1ca <- inc-1504c7, inc-c67c98
    inc-1504c7   (leaf: no dependencies)
    inc-c67c98   (leaf: no dependencies)
```

**Arrays are the same thing at scale.** Switching to a chunked field, the keys
become tuples:

```
A dask array is the same thing at scale -- one task per chunk operation:
  shape=(60, 256, 256), chunks=(30, 128, 128), n_chunks=8, ~3.9 MB/chunk
  task_count(field) = 8 (one random-block task per chunk)
  task_count(field.mean()) = 17 (per-chunk partials + combine steps)
  array keys are tuples: (random-676cdd, 0, 0, 0) = (name, block index per dim)
```

`(random-676cdd, 0, 0, 0)` is "block (0, 0, 0) of the array named
`random-676cdd`". Every array key in dask has this shape, which is why you can
look at a dashboard and immediately tell which corner of your array is slow.

**What optimization buys.** The payoff section computes `field[0].mean()` and
compares the graph before and after `dask.base.optimize`:

```python
day0 = field[0].mean()          # reads only the 4 spatial blocks of the first time-chunk
optimized: Any = optimize(day0)[0]
```

```
  field[0].mean() graph: 17 raw tasks -> 9 after dask.optimize
  the optimizer dropped chunks the slice never reads -- only possible because
  the whole graph exists before anything runs
  same answer either way: day-0 mean = 0.4994, whole mean = 0.5000
```

Eight of seventeen tasks vanished because the slice makes them unreachable. On a
52-chunk array the ratio is much starker -- [0202](#0202-blocked-algorithms)
measures 61 down to 9.

**Why it matters.** An openEO process graph and a dask task graph are the same
kind of object: nodes with dependencies, evaluated by a planner. OCS translates
one into the other. Being able to print, count, and reason about a dask graph is
the difference between "the pipeline is slow" and "the pipeline schedules 40,000
tasks because the store's chunks are too small".

**Traps.**

- `task_count` on an unoptimized graph is not a cost estimate. Culling and
  fusion can remove most of it. Use `optimize` first when the number is meant to
  predict work.
- `dict(HighLevelGraph)` fully materializes the graph. On a very large graph that
  is itself expensive and memory-hungry -- fine for a 52-chunk demo, not something
  to do in a hot loop.
- Keys are stable within a session but the tokens are not reproducible across
  processes for `delayed` (they are UUIDs). Do not build logic on key strings.

### 0103 — Sharing intermediates

Source: [`../../dask/examples/0103_sharing_intermediates.py`](../../dask/examples/0103_sharing_intermediates.py)

**What it teaches.** That dask deduplicates by key *within a single compute*, and
not at all across separate computes. This is the highest-value habit in the whole
project: it costs nothing to adopt and it silently doubles or triples your
runtime when ignored.

The setup is one expensive shared step feeding two cheap outputs, with a counter
recording real executions:

```python
CALLS: Counter[str] = Counter()

def load_field() -> float:
    CALLS["load_field"] += 1
    time.sleep(EXPENSIVE_SECONDS)   # 0.3
    return 42.0

base: Delayed = delayed(load_field)()
x: Delayed = delayed(scale)(base)
y: Delayed = delayed(offset)(base)
```

The merged graph already tells you what should happen:

```
  task_count(x) = 2, task_count(y) = 2 (each: load + its own step)
  merged graphs hold 3 keys, not 4 -- base has ONE key, load_field-849c4d93-...
```

Two two-task graphs merge into three keys, not four, because `base` appears in
both with the same key.

Computing separately ignores that:

```python
rx: Any = x.compute(scheduler="threads")
ry: Any = y.compute(scheduler="threads")
```

```
  results: x=420.0, y=43.0
  call counts: {'load_field': 2, 'scale': 1, 'offset': 1}
  load_field ran TWICE -- each compute() only sees its own graph
```

Computing together does not:

```python
rx, ry = compute(x, y, scheduler="threads")
```

```
  results: x=420.0, y=43.0
  call counts: {'load_field': 1, 'scale': 1, 'offset': 1}
  load_field ran ONCE -- identical keys are deduplicated within a single compute
```

```
Timings (shared step costs a fixed 0.3 s):
  separate computes were ~2.3x slower than one dask.compute (expected ~2x)
```

(The 2.3x is machine-dependent; the call counter, 2 versus 1, is not.)

**The caveat that makes it work.** Sharing is by key, and keys come from
identity, not from equality of arguments:

```python
base2: Delayed = delayed(load_field)()
```

```
  a second delayed(load_field)() gets a fresh key: 2f74f0 vs 61f0b3
  build once, reuse the variable -- do not re-wrap the call per output
```

This is the trap. Two calls that look identical in the source produce different
tokens and therefore different tasks. dask will happily load your dataset twice
because a helper function called `open_store()` internally instead of accepting
the already-open handle.

For `dask.array` and `dask.dataframe` the situation is friendlier -- their tokens
are content-derived hashes, so `da.ones(10, chunks=5)` twice does produce the
same key. But the moment randomness, file handles, or `delayed` enter the
picture, identity is what you get, and the rule "build once, pass the variable
around" is the only reliable one.

**Why it matters.** An openEO process graph fans out: one loaded datacube feeds a
mean, a maximum, a percentile, and three derived indices. If OCS computed those
separately, it would read the store six times. It computes them together, and
reads once.

**Traps.**

- Deduplication is per compute call. A `for` loop of `.compute()` calls shares
  nothing, which is exactly the anti-pattern
  [0603](#0603-graph-hygiene) measures.
- Deduplication does not survive process boundaries or client restarts.
- If the outputs are needed at genuinely different times, `dask.compute` is not
  available to you -- that is what `.persist()` is for
  ([0503](#0503-compute-load-persist)).
- Beware of "helpfully" re-deriving an intermediate inside a function. Pass it
  in.

---

## Phase 2 — dask.array

Five examples on blocked numpy: the block structure itself, how reductions
decompose, how pipelines stay lazy, what rechunking costs, and how to run your
own numpy code per block.

### 0201 — Chunked arrays

Source: [`../../dask/examples/0201_chunked_arrays.py`](../../dask/examples/0201_chunked_arrays.py)

**What it teaches.** The anatomy of a dask array: what `.chunks`, `.numblocks`,
`.npartitions`, and `.blocks` mean, that the values are ordinary numpy, and that
the chunk shape you pick at creation determines the task count of everything
downstream.

```python
arr = random_field()  # (365, 256, 256), chunks (30, 128, 128), seed 0
print(f"  {chunk_report(arr)}")
print(f"  arr.chunks[0] (per-chunk sizes along time) = {arr.chunks[0]}")
print(f"  arr.numblocks = {arr.numblocks}")
```

```
A dask array is one logical array split into a grid of numpy blocks (chunks).
random_field() mimics an OCS climate store: daily data, (time, y, x), zarr-style chunks.
  shape=(365, 256, 256), chunks=(30, 128, 128), n_chunks=52, ~3.9 MB/chunk
  arr.chunks[0] (per-chunk sizes along time) = (30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 5)
    -> 12 full 30-day chunks plus a 5-day remainder; chunks need not be uniform
  arr.numblocks = (13, 2, 2)  (blocks per axis: 13 x 2 x 2)
  arr.npartitions = 52  (total blocks, the dataframe world calls these partitions)
  arr.blocks.shape = (13, 2, 2)  (.blocks indexes the grid of blocks)
  arr.blocks[0, 0, 0] is itself a dask array: shape=(30, 128, 128), chunks=(30, 128, 128), n_chunks=1, ~3.9 MB/chunk
  task_count(arr) = 52  -- one task per chunk just to create the data
```

Several things to extract from that block.

`.chunks[0]` has thirteen entries, ending in 5. This is dask being relaxed about
uneven division, and it is the exact situation that later collides with zarr.

`.blocks` is an indexer, not data. `arr.blocks[0, 0, 0]` returns a new dask array
containing only that block -- useful for testing a per-block function against a
numpy reference, which is precisely how [0205](#0205-map_blocks-and-map_overlap)
verifies `map_blocks`.

`task_count(arr) = 52` before you have done anything. Creating the array is
already 52 tasks, one random-generation call per block. Reading a zarr store is
the same: one read task per stored chunk.

**Values are numpy values.** The example checks three ways:

```python
sliced = arr[10:20, :64, :64].compute()
scaled = (arr * 9.0 + 20.0)[:5].compute()
```

```
  arr.compute() -> ndarray with shape (365, 256, 256), dtype float64
  slice matches numpy slice:       np.allclose = True
  elementwise math matches numpy:  np.allclose = True
  arr.mean() vs npa.mean():        np.allclose = True
```

dask changes *when* and *where* work happens. It does not change the answer.
(Floating-point reductions can differ in the last bits because the summation
order changes -- `allclose`, not `array_equal`, is the right comparison for
reductions.)

**Layout is a creation-time decision.** The example builds the same logical data
three ways:

```python
balanced = random_field(time_chunk=30, spatial_chunk=128)
tiny = random_field(time_chunk=1, spatial_chunk=32)
single = random_field(time_chunk=365, spatial_chunk=256)
```

```
  balanced: shape=(365, 256, 256), chunks=(30, 128, 128), n_chunks=52, ~3.9 MB/chunk, tasks=52
  tiny    : shape=(365, 256, 256), chunks=(1, 32, 32), n_chunks=23360, ~0.0 MB/chunk, tasks=23360
  single  : shape=(365, 256, 256), chunks=(365, 256, 256), n_chunks=1, ~191.4 MB/chunk, tasks=1
```

23,360 tasks to create an array that fits in 191 MB. At roughly a millisecond of
scheduler overhead each, that layout has budgeted twenty-plus seconds of pure
bookkeeping before doing any arithmetic. At the other extreme, one chunk means
one task, no parallelism, and 191 MB that must be resident.

**Why it matters.** OCS stores every dataset as a `(time, y, x)` zarr store, and
the chunk shape written into that store is inherited by every later read. There
is no fixing it at query time without paying for a rechunk. The decision is made
once and lived with.

**Traps.**

- `.chunks` (plural, a tuple of tuples) is not `.chunksize` and is not the shape
  you passed in. After operations the layout drifts -- `groupby` in xarray can
  fragment time into one chunk per label, as [0504](#0504-lazy-zarr-pipeline)
  shows.
- `chunk_report` reports the *first* chunk's size. For ragged layouts that
  overstates the average.
- `arr.compute()` on a 191 MB array is fine; on a 191 GB array it is a crash.
  There is nothing in the API to stop you.
- `.blocks[...]` uses block indices, not element indices. `arr.blocks[0]` is the
  first 30 days, not the first day.

### 0202 — Blocked algorithms

Source: [`../../dask/examples/0202_blocked_algorithms.py`](../../dask/examples/0202_blocked_algorithms.py)

**What it teaches.** How a reduction turns into per-chunk partials plus a combine
tree, how the reduced axes shape that tree, and how slicing before reducing lets
the optimizer throw most of the graph away.

```python
arr = random_field()          # 52 chunks
total = arr.sum()
```

```
Baseline: creating the chunked field is already a graph -- one task per chunk.
  shape=(365, 256, 256), chunks=(30, 128, 128), n_chunks=52, ~3.9 MB/chunk
  task_count(arr) = 52  (52 chunks -> 52 creation tasks)

arr.sum() does not sum 24.5M elements in one task; it sums each chunk, then combines:
  task_count(arr.sum()) = 118  (added 66 tasks)
  added tasks = 52 per-chunk partial sums + a tree of combine steps + 1 final aggregate
  the tree matters: combining the 52 partials a few at a time keeps every task small and parallel
  result matches numpy: np.allclose(arr.sum(), npa.sum()) = True
```

66 added tasks for a sum over 52 chunks: 52 partials, 13 or so combines, one
aggregate, plus finalization. Note that no task in this graph ever holds more
than one chunk plus a handful of scalars. That is the entire trick to reducing an
array bigger than memory.

**The axes shape the tree.**

```python
over_time = arr.sum(axis=0)        # (256, 256) map
over_space = arr.sum(axis=(1, 2))  # (365,) series
```

```
  arr.sum()           -> shape (),  tasks=118
  arr.sum(axis=0)     -> shape (256, 256),  tasks=124
  arr.sum(axis=(1,2)) -> shape (365,),  tasks=117
  every variant starts with the same per-chunk partials; only the combine pattern differs
  output stays chunked too: sum(axis=0) has 4 blocks, one per spatial column
```

The per-chunk stage is identical across all three. What changes is how partials
are grouped: summing over time combines the 13 time-blocks within each spatial
column, leaving a 2x2 grid of output blocks. The output of a reduction is still a
dask array with its own chunk layout -- reductions do not automatically bring you
back to numpy.

**Culling is where the money is.**

```python
day0 = arr[0].sum()   # touches only the 4 spatial blocks of the first time-chunk
```

```
  raw graphs still reference every creation task (nothing is culled until optimize/compute):
    task_count(arr.sum())    = 118
    task_count(arr[0].sum()) = 61
  after dask.optimize (what compute() runs):
    optimized arr.sum()    = 117 tasks  (all 52 chunks needed)
    optimized arr[0].sum() = 9 tasks  (4 chunks + slice + combine)
  value check: np.allclose(arr[0].sum(), npa[0].sum()) = True
```

61 raw tasks, 9 executed. The slice `arr[0]` selects day 0, which lives in the
first time-chunk, which spans 4 spatial blocks -- so 4 creation tasks, 4 slice
tasks, and a combine. The other 48 creation tasks were in the graph and are
simply never run.

This is the mechanism behind every "dask read only what I needed" claim. It
requires nothing from you except *expressing the subset lazily*. Slice the dask
array; do not compute and then slice.

**Why it matters.** OCS answers requests for a bounding box and a date range over
a store that may hold decades of global data. It does that by slicing the lazy
view before the graph executes, so the read touches a handful of chunks. The
entire performance story of the service is this one behaviour.

**Traps.**

- Culling only works on what the graph can prove is unused. A boolean mask
  (`arr[arr > 0.5]`) cannot be resolved without running, so nothing is culled.
- Slicing across chunk boundaries pulls in whole chunks. `arr[0:1]` on a
  30-day chunk still reads 30 days from disk. The chunk is the unit of I/O.
- The optimized count is not proportional to time -- one 15 MB chunk read may
  dominate nine tasks' worth of scheduling.
- `optimize` returns a tuple even for a single input: `(optimized,) =
  optimize(obj)`.

### 0203 — Lazy pipelines

Source: [`../../dask/examples/0203_lazy_pipelines.py`](../../dask/examples/0203_lazy_pipelines.py)

**What it teaches.** That an entire multi-step analysis pipeline can be built in
milliseconds and executed in one pass, that each step only appends tasks, and
that the intermediates never exist in full.

The pipeline is a realistic climate one: scale to physical units, subtract the
time mean to get an anomaly, reduce space away to get a daily series.

```python
scaled = arr * 9.0 + 20.0                  # elementwise
anomaly = scaled - scaled.mean(axis=0)     # reduction feeding an elementwise op
series = anomaly.mean(axis=(1, 2))         # reduce space away
```

```
Every step below is graph construction -- watch the wall time stay near zero.
  source: shape=(365, 256, 256), chunks=(30, 128, 128), n_chunks=52, ~3.9 MB/chunk, tasks=52

Pipeline: scale to degC -> daily anomaly vs time-mean -> spatial-mean time series
  step 1  scaled = arr * 9 + 20            0.61 ms, tasks=156
  step 2  anomaly = scaled - time-mean     1.64 ms, tasks=280
  step 3  series = spatial mean            1.07 ms, tasks=345
  three steps of 'work' on a 191 MB array took 3.33 ms -- no data was touched
  series is still lazy: shape=(365,), dtype=float64, tasks=345
```

(Timings machine-dependent; task counts are not.)

Notice step 2. `scaled - scaled.mean(axis=0)` is a *broadcast against a
reduction*: dask must compute the `(256, 256)` time-mean map, then subtract it
from every one of the 13 time-blocks. That is why the step adds 124 tasks rather
than 52 -- it is a reduction and a broadcast in one line. The array knows its own
shape and dtype the whole time, so `series.shape` is `(365,)` without computing
anything.

**One compute at the end.**

```
One .compute() at the end executes the whole graph -- this is where the time goes:
  series.compute() -> numpy shape (365,) in 0.13 s
  first 3 daily anomalies: [-0.004848  0.007993 -0.002739]
  scheduler note: chunks stream through the pipeline; the full scaled/anomaly
  intermediates never exist in memory at once
```

That last line is the important one. `scaled` and `anomaly` are both 191 MB
arrays *logically*. Neither is ever materialized. The scheduler runs the chain
chunk by chunk and releases each intermediate as soon as its consumers have
finished, so peak memory is a few chunks, not a few hundred megabytes. Fusion
helps here too: a chain of elementwise operations on the same block collapses
into a single task, so `2 * x + 1` never allocates a separate `2 * x`.

**Agreement with numpy.**

```
Same math with plain numpy agrees:
  np.allclose(dask pipeline, numpy pipeline) = True
```

**The teaser for persist.** The example ends by computing `argmax` and `argmin`
over the same pipeline in two separate calls:

```
  argmax/argmin over the same pipeline: day 293 hottest, day 221 coldest
  (two separate computes took 0.12 s -- 0103 shows how one compute shares work)
```

Two computes, two full runs of the 345-task graph.
[0603](#0603-graph-hygiene) turns this into a measured habit.

**Why it matters.** An openEO process graph is a chain exactly like this one. The
laziness is what allows OCS to accept "scale, mask, aggregate, reproject, write"
as a single request and execute it without ever holding a full datacube.

**Traps.**

- Task counts grow superlinearly with pipeline depth if each step touches every
  chunk. 52 chunks became 345 tasks in three steps. Ten steps on 5,000 chunks is
  a graph large enough that *building and optimizing* it becomes the bottleneck.
- Anything that forces a value ends laziness. `if series.max() > 5:` computes.
  `float(x)`, `bool(x)`, `len(x)` on an unknown-length axis, `plt.plot(x)`, and
  printing a dataframe all compute.
- Fusion only helps chains that are one-in-one-out. A step whose result feeds two
  consumers is a fusion barrier, which is the reason `anomaly` costs more than
  its line count suggests.

### 0204 — Rechunking

Source: [`../../dask/examples/0204_rechunking.py`](../../dask/examples/0204_rechunking.py)

**What it teaches.** That `rechunk` has three distinct cost regimes, that
creating the right layout is far cheaper than converting into it, and that no
single layout serves every access pattern. This example is the empirical
foundation for the [chunking deep section](#chunking-the-deep-section).

**Regime 1: crossing boundaries is all-to-all.**

```python
ts_layout: Any = (365, 32, 32)
reshaped = base.rechunk(ts_layout)
```

```
Rechunk to a time-series layout (full time, 32x32 tiles) -- chunk boundaries cross:
  result: shape=(365, 256, 256), chunks=(365, 32, 32), n_chunks=64, ~3.0 MB/chunk
  tasks: 52 -> 184  (+132)
  every new chunk needs pieces of many old chunks: split tasks + concat tasks,
  an all-to-all data movement -- and at scale, memory pressure while both layouts coexist
```

132 extra tasks to produce 64 output chunks. Every new `(365, 32, 32)` column
needs a slice out of all 13 time-blocks and one of the 2x2 spatial blocks, so
dask emits split tasks to carve pieces out and concat tasks to glue them
together. On a cluster this is network traffic between every pair of workers.

**Regime 2: aligned boundaries are cheap merges.**

```python
merged_layout: Any = (60, 128, 128)   # old 30-day boundaries line up
merged = base.rechunk(merged_layout)
```

```
Rechunk to (60, 128, 128) -- old 30-day boundaries line up with new 60-day ones:
  result: shape=(365, 256, 256), chunks=(60, 128, 128), n_chunks=28, ~7.9 MB/chunk
  tasks: 52 -> 80  (+28)
  aligned boundaries just concatenate neighbours: one merge task per new chunk
```

28 added tasks for 28 output chunks: exactly one concatenation each, no splits.
Merging chunks by an integer factor is nearly free. Splitting them by an integer
factor is likewise cheap. It is the *unaligned* case that hurts.

**Regime 3: the no-op is free.**

```
  rechunk to the current layout is free: returns the same array (True), tasks=52
```

`same is base` is `True` -- dask returns the identical object rather than
building anything. This is why a defensive `arr.rechunk(target)` before a write
is safe when the layout already matches, and why the `uniform_chunks` helper in
[0601](#0601-zarr-legal-chunks) can afford to check before acting.

**Creation beats conversion.**

```
Choosing the layout at creation skips the shuffle entirely:
  created as (365, 32, 32) directly:   tasks=64
  created balanced, then rechunked:    tasks=184
  same final layout, ~3x the tasks plus all-to-all data movement
```

**No layout wins both queries.** The centrepiece of the example is a table
comparing two OCS-shaped queries -- a point time series and a single-day map --
across three layouts, using *optimized* task counts so the numbers reflect chunks
actually touched:

```python
for name, arr in (("balanced (30,128,128)", balanced),
                  ("time-opt (365,32,32)", time_opt),
                  ("map-opt  (1,256,256)", map_opt)):
    ts_tasks = optimized_task_count(arr[:, 7, 7])
    map_tasks = optimized_task_count(arr[100])
```

```
  layout                   time series arr[:, 7, 7]   day map arr[100]
  balanced (30,128,128)                    26 tasks            8 tasks
  time-opt (365,32,32)                      2 tasks          128 tasks
  map-opt  (1,256,256)                    730 tasks            2 tasks
```

Read the diagonal. The time-optimized layout answers a time-series query in 2
tasks and a map query in 128. The map-optimized layout does the reverse, and its
time-series cost is catastrophic -- 730 tasks, because every one of 365 daily
chunks must be opened to extract a single pixel. The balanced layout is worse
than each specialist at its own game and far better than each at the other's:
26 and 8.

That asymmetry -- specialists are 64x and 365x worse at the wrong query, the
generalist is 4x to 13x worse at the right one -- is the whole argument for
picking a balanced layout when you cannot predict the query mix.

**Why it matters.** OCS freezes one layout into each zarr store and serves both
map tiles and time series from it. Its choice (spatial chunks capped at 512 per
side, time chunks derived from the dataset's temporal resolution) is the balanced
row of this table, tuned so neither query shape degenerates.

**Traps.**

- Rechunking a lazy array does not "fix" a bad store. The read still happens at
  the store's chunk granularity; the rechunk is extra work on top.
- Peak memory during an all-to-all rechunk can approach holding both layouts.
  This is a classic worker-killer on a cluster.
- `rechunk("auto")` picks sizes from `dask.config` (`array.chunk-size`, default
  128 MiB) and is a reasonable default when you have no opinion, but it optimizes
  for chunk size, not for your access pattern.
- The chunk tuple after `rechunk` may still be ragged. If zarr is the
  destination, check it -- see [0601](#0601-zarr-legal-chunks).

### 0205 — map_blocks and map_overlap

Source: [`../../dask/examples/0205_map_blocks_overlap.py`](../../dask/examples/0205_map_blocks_overlap.py)

**What it teaches.** The escape hatch for running arbitrary numpy code over a
chunked array, why it is block-local, and what that breaks. This is the example
that produces a genuinely wrong answer on purpose, then fixes it.

The field is small and deliberately chunked so every axis has interior
boundaries:

```
A small OCS-shaped field, chunked so every axis has interior chunk boundaries:
  shape=(6, 64, 64), chunks=(3, 32, 32), n_chunks=8, ~0.0 MB/chunk, tasks=8
```

**`map_blocks` runs your function once per block.**

```python
def normalize_block(block: np.ndarray) -> np.ndarray:
    result: np.ndarray = (block - block.mean()) / block.std()
    return result

normalized: da.Array = arr.map_blocks(normalize_block)
```

```
map_blocks(normalize_block): each of the 8 blocks is standardized independently.
  lazy result: shape=(6, 64, 64), chunks=(3, 32, 32), n_chunks=8, ~0.0 MB/chunk, tasks=16 (one task per block)
  block (0,0,0) equals normalizing that numpy sub-array alone: True
  ...but differs from a global normalization: allclose = False
  map_blocks is block-LOCAL: the function never sees values outside its own chunk
```

Both checks matter. The block *does* equal what you get by normalizing that numpy
sub-array alone -- so `map_blocks` is doing exactly what it says. And the result
is *not* a global normalization, because each block used its own mean and
standard deviation. If you wanted a global normalization you needed
`(arr - arr.mean()) / arr.std()`, which is a different graph entirely.

**Stencils break at the seams.** A 3x3 mean filter needs each cell's eight
neighbours. Applied per block, cells on a chunk edge get padded with their own
block's values instead:

```python
def mean3x3(field: np.ndarray) -> np.ndarray:
    padded = np.pad(field, ((0, 0), (1, 1), (1, 1)), mode="edge")
    windows = sliding_window_view(padded, (3, 3), axis=(1, 2))
    result: np.ndarray = windows.mean(axis=(-2, -1))
    return result

seamed: da.Array = arr.map_blocks(mean3x3)
```

```
A 3x3 mean filter needs each cell's neighbours -- some live in the next chunk over.
  map_blocks(mean3x3) matches the full-array filter: False
  mismatched cells: 1512 of 24576 -- every one sits on an interior chunk edge
  e.g. along the x=31|32 seam alone: 384 wrong cells (blocks edge-padded with their own values)
```

1512 wrong cells out of 24576. That is 6% of the array, and every one of them
sits on a chunk boundary. Nothing raises. Nothing warns. The array has the right
shape, the right dtype, plausible values, and a grid of subtle artefacts that a
plot will render as faint lines. This is the single nastiest silent bug in the
dask array world.

**`map_overlap` adds the halo.**

```python
filtered = arr.map_overlap(mean3x3, depth={0: 0, 1: 1, 2: 1}, boundary="none")
```

```
map_overlap(depth={y:1, x:1}) copies a 1-cell halo from each neighbour before the
function runs, then trims it from the output -- so every 3x3 window sees real data:
  lazy result: shape=(6, 64, 64), chunks=(3, 32, 32), n_chunks=8, ~0.0 MB/chunk, tasks=64
  (vs 16 for map_blocks: the extra tasks slice, exchange, and trim the halos)
  matches the full-array filter everywhere: True
```

64 tasks versus 16 -- the halo exchange costs 4x the tasks on this tiny array,
because each block must slice pieces off its neighbours, concatenate them, run
the function, and trim. The output chunk layout is unchanged.

`depth` is per-axis and given by axis index: `{0: 0, 1: 1, 2: 1}` means no halo
along time and one cell along y and x, matching a purely spatial 3x3 kernel.
`boundary="none"` leaves the outer edges of the whole array to the function's own
handling (here, `np.pad(mode="edge")`); the alternatives include a constant,
`"reflect"`, `"periodic"`, and `"nearest"`.

**Why it matters.** Real climate processing is full of neighbourhood operations:
smoothing, gradients, regridding kernels, morphological masks. All of them run
chunk by chunk over a zarr store, and all of them need a halo whose depth matches
the kernel radius.

**Traps.**

- The halo depth must be at least the kernel radius. A 5x5 filter needs
  `depth=2`; a rolling 7-day window along time needs `depth={0: 3}` (or 6, if the
  window is trailing rather than centred).
- Depth must be smaller than the chunk size along that axis, or dask will
  complain -- and if it is even close, rechunk to larger blocks first, because the
  halo overhead grows as `depth / chunk_size`.
- `map_blocks` assumes the output block has the same shape as the input block.
  If it does not, pass `chunks=` (and `drop_axis` / `new_axis`) explicitly, or
  dask will build a graph with wrong metadata and fail confusingly at compute
  time.
- `map_blocks` calls your function once with a tiny fake array to infer dtype and
  shape (`meta`). A function that crashes on a zero-length input will fail at
  graph-construction time for no visible reason. Pass `meta=` or `dtype=` to skip
  the inference.
- Anything reduction-like inside a `map_blocks` function is block-local by
  definition. If your function contains `.mean()`, stop and check whether you
  meant a global mean.

---
## Phase 3 — Schedulers and distributed

The graph is fixed; the executor is a choice. Four examples cover that choice,
the local cluster that turns dask into a system, the eager futures API, and the
tooling for finding out where time and memory went.

### 0301 — Schedulers

Source: [`../../dask/examples/0301_schedulers.py`](../../dask/examples/0301_schedulers.py)

**What it teaches.** That the same graph runs on three different local executors,
and that which one wins depends entirely on whether the task bodies release the
GIL. The example constructs two workloads chosen to sit on opposite sides of that
line and runs both on all three schedulers.

Workload A is numpy-heavy -- random generation and a sum, both C code:

```python
field = random_field(days=365, ny=512, nx=512, time_chunk=30, spatial_chunk=256)
numpy_graph = field.sum()
```

Workload B is pure Python -- a delayed loop accumulating an integer:

```python
def py_loop(n: int) -> int:
    total = 0
    for i in range(n):
        total += i
    return total

parts = [delayed(py_loop)(LOOP_SIZE + i) for i in range(N_LOOP_TASKS)]  # 12 tasks
python_graph = delayed(sum)(parts)
```

Both go through the same timing harness, which changes exactly one thing:

```python
def timed_compute(graph: Any, scheduler: str) -> tuple[float, float]:
    start = time.perf_counter()
    (result,) = compute(graph, scheduler=scheduler)
    return float(result), time.perf_counter() - start
```

```
One task graph can run on different executors; this machine has 12 cores.

  A: shape=(365, 512, 512), chunks=(30, 256, 256), n_chunks=52, ~15.7 MB/chunk
     graph for sum(): 118 tasks
  B: 12 delayed py_loop(30,000,000) tasks + 1 combining sum
     graph: 13 tasks

--- Workload A: numpy-heavy (GIL released) ---
A: same graph, three executors
  scheduler=synchronous    0.55 s  (1.00x of sync)  result=4.784e+07
  scheduler=threads        0.08 s  (0.14x of sync)  result=4.784e+07
  scheduler=processes      0.70 s  (1.26x of sync)  result=4.784e+07

--- Workload B: pure Python (GIL held) ---
B: same graph, three executors
  scheduler=synchronous    8.08 s  (1.00x of sync)  result=5.4e+15
  scheduler=threads        8.23 s  (1.02x of sync)  result=5.4e+15
  scheduler=processes      4.58 s  (0.57x of sync)  result=5.4e+15
```

(All timings machine-dependent. The *signs* of the effects are not.)

The scorecard:

```
  workload                 synchronous       threads     processes
  A numpy-heavy                  1.00x         0.14x         1.26x
  B pure-Python                  1.00x         1.02x         0.57x
```

Four separate lessons live in that table.

**Threads win numpy work by a lot.** 0.14x, a 7x speedup on 12 cores. Chunks are
shared in memory with zero copying, and numpy releases the GIL for the duration
of each block operation.

**Processes lose numpy work here.** 1.26x -- *slower than running serially*.
Every one of those 52 chunks is a 15.7 MB numpy array that must be pickled and
shipped to a worker process, and the result shipped back. Plus a one-time cost to
spawn the pool. Serialization ate the parallelism.

**Threads do nothing for pure Python.** 1.02x. Twelve threads, one GIL, one
thread running bytecode at a time. The 2% is overhead.

**Processes are the answer for pure Python.** 0.57x. Twelve separate
interpreters, twelve separate GILs. The speedup is far short of 12x because the
graph is small and the pool startup is a visible share of 4.58 s -- which the
example says outright:

```
note: 'processes' pays a one-time cost here to spawn worker processes and
pickle tasks/results; on graphs this small that startup is a visible share
of its wall time, so its ratios above understate its win on longer jobs.
```

**Why it matters.** A slow OCS pipeline is frequently a scheduler mismatch rather
than a graph problem. If the heavy work is inside a Python-level function --
a custom openEO process implemented in pure Python, say -- the default threaded
scheduler will refuse to help no matter how many cores the machine has.

**Traps.**

- `scheduler="processes"` requires everything to pickle: the function, its
  closure, the arguments, and the results. Lambdas, local functions, open file
  handles, and database connections all fail. `cloudpickle` covers more than
  `pickle` but not everything.
- The default scheduler depends on the collection: threads for `array` and
  `dataframe`, processes for `bag`, synchronous for `delayed` in some paths. Do
  not assume.
- A live `distributed.Client` overrides all of this process-wide. If you create a
  client and then pass `scheduler="threads"`, you get threads and confusion.
- Measuring with a graph this small overstates fixed costs. Benchmark with a
  workload the size of the real one.
- `scheduler="synchronous"` is the debugging setting. Reach for it first when a
  traceback makes no sense.

### 0302 — Local cluster

Source: [`../../dask/examples/0302_local_cluster.py`](../../dask/examples/0302_local_cluster.py)

**What it teaches.** What a real dask cluster is made of, even when the cluster is
two threads in your own process, and what the client exposes about it.

```python
def start_cluster() -> LocalCluster:
    settings: dict[str, Any] = {"processes": False, "n_workers": 2, "threads_per_worker": 1}
    try:
        return LocalCluster(dashboard_address="127.0.0.1:0", **settings)
    except Exception:
        print("note: could not bind a dashboard port; continuing with dashboard_address=None")
        no_dashboard: Any = None
        return LocalCluster(dashboard_address=no_dashboard, **settings)
```

Two settings deserve comment. `dashboard_address="127.0.0.1:0"` means "bind the
dashboard on localhost, on any free port" -- the fallback path exists because
sandboxed environments sometimes refuse to bind at all, and an example that
crashes on a locked-down machine teaches nothing. `processes=False` runs the
workers as threads inside the current interpreter: no pickling, no separate
processes, breakpoints work, and the whole thing tears down cleanly.

```python
with start_cluster() as entered, Client(entered) as client:
    cluster = cast(LocalCluster, entered)
    print(f"  cluster: {cluster.scheduler_address} with {len(cluster.workers)} workers")
```

```
LocalCluster starts a scheduler plus workers on this machine; Client connects to it.
processes=False keeps workers as threads in this process: no pickling, easy debugging.
  cluster: inproc://192.168.1.8/59258/1 with 2 workers

The dashboard is a live bokeh web app served by the scheduler:
  dashboard link: http://192.168.1.8:54902/status
```

The `inproc://` scheme confirms the in-process arrangement; a normal cluster
would show `tcp://`.

**The client becomes the default scheduler.** This is the part people trip over:

```python
scheduler_fn = get_scheduler()
owner = getattr(scheduler_fn, "__self__", None)
```

```
Creating a Client makes it dask's default scheduler -- no scheduler= needed:
  dask.base.get_scheduler() is bound to: 'Client'
  data:  shape=(120, 256, 256), chunks=(30, 128, 128), n_chunks=16, ~3.9 MB/chunk
  graph: mean() = 35 tasks, sent to the scheduler by plain .compute()
  result: mean = 0.499971 (uniform [0, 1) draws, so ~0.5)
```

Merely constructing a `Client` installs it globally. Nothing about the
computation changed -- `graph.compute()` is the same call it was before -- but the
work now goes to the cluster. This is convenient and occasionally surprising: a
client created in a notebook cell twenty minutes ago is still capturing every
compute you run.

**What the scheduler knows.**

```python
info: dict[str, Any] = client.scheduler_info()
workers: dict[str, Any] = info["workers"]
for address, meta in sorted(workers.items()):
    memory_gb = meta["memory_limit"] / 1e9
    print(f"    {meta['name']!s:>3} at {address}: nthreads={meta['nthreads']}, memory_limit={memory_gb:.1f} GB")
```

```
client.scheduler_info() -- the scheduler's live view of its workers:
  workers: 2
      0 at inproc://192.168.1.8/59258/4: nthreads=1, memory_limit=34.4 GB
      1 at inproc://192.168.1.8/59258/6: nthreads=1, memory_limit=34.4 GB
```

`scheduler_info()` is the programmatic version of the dashboard's worker table:
addresses, thread counts, memory limits, and (on a live cluster) current memory
use. It is what a health check should read.

**The dashboard.** The example prints the link and describes the panels rather
than screenshotting them:

```
  it shows the task stream (which worker ran what, when), progress bars per
  task name, per-worker memory/CPU, and the task graph -- open it during a
  long compute to watch tasks flow; port 0 above means 'pick any free port'.
```

The task stream is the panel worth learning. Each horizontal bar is one task on
one worker, coloured by task name, with transfer time shown in red. A stream full
of red is a data-locality problem. A stream with gaps is a dependency or
scheduling problem. A stream of very short bars is a too-many-tiny-chunks
problem. The [dask-distributed](dask-distributed.md) project reads all of this
programmatically.

**Why it matters.** OCS talks to dask through a distributed `Client`. Worker
count, threads per worker, and memory limit are the three knobs that decide
whether a request finishes or kills a worker, and `scheduler_info()` is how you
find out what they currently are.

**Traps.**

- Always close the client and cluster. Without the context managers, the process
  will not exit -- worker threads and the tornado event loop keep it alive.
- `processes=False` is great for tests and terrible as a production default: one
  GIL for all workers means the arrangement has the threaded scheduler's
  weaknesses without its simplicity.
- `memory_limit` defaults to a share of system RAM per worker. On a container
  with a cgroup limit, dask may see the host's memory and set a limit that gets
  the container OOM-killed. Set it explicitly in containers.
- The dashboard binds a port. In CI, in containers, and behind restrictive
  firewalls it may fail; handle it, as the example does.
- More workers is not automatically better. Total throughput tracks
  `n_workers * threads_per_worker`, and more workers means more data movement
  between them. See [scaling](../scaling.md).

### 0303 — Futures

Source: [`../../dask/examples/0303_futures.py`](../../dask/examples/0303_futures.py)

**What it teaches.** The eager, imperative half of dask. `client.submit` starts
work *now*; `Future.result()` collects it; `client.map` fans out; `client.gather`
collects in bulk. And, directly contrasted, how differently a `Delayed` behaves.

```python
fut_a = client.submit(slow_square, 6)
fut_b = client.submit(slow_square, 7)
print(f"  right after submit: fut_a.status={fut_a.status!r}, fut_b.status={fut_b.status!r}")
other_work = sum(i * i for i in range(200_000))
a, b = fut_a.result(), fut_b.result()
```

```
Connected to a LocalCluster with 2 workers, 1 thread each (settings from 0302).

client.submit ships the call to a worker IMMEDIATELY and returns a Future:
  right after submit: fut_a.status='pending', fut_b.status='pending'
  ...main thread now does other work while both tasks run on the workers...
  other work done (checksum 2666646666700000), then result() blocked only for the remainder
  after result(): fut_a.status='finished', results a=36, b=49
  wall time 0.32 s for two 0.3 s tasks -> they overlapped on 2 workers
```

Two 0.3 s tasks in 0.32 s of wall time, with a chunk of unrelated Python work
sandwiched in between. That is the whole point of the futures model: the work
started at `submit`, ran on the workers while the main thread was busy, and
`result()` only blocked for what was left.

**Fan-out.**

```python
futures = client.map(slow_square, inputs)
results = cast(list[int], client.gather(futures))
```

```
client.map submits one task per input; client.gather collects them in one round trip:
  inputs:  [0, 1, 2, 3, 4, 5, 6, 7]
  results: [0, 1, 4, 9, 16, 25, 36, 49]
  wall time 1.22 s vs 2.4 s serial -> ~2.0x with 2 workers
```

Exactly 2.0x on 2 workers, which is what you would predict for eight identical
0.3 s tasks. `gather` is one round trip for all eight futures; calling
`.result()` in a loop would be eight.

**The contrast that defines the two models.**

```python
eager = client.submit(slow_square, 9)
lazy = delayed(slow_square)(9)
time.sleep(TASK_DELAY + 0.3)
```

```
Contrast: futures are eager and imperative; collections are lazy and declarative.
  future after 0.6 s of doing nothing: status='finished' -- it ran anyway
  delayed after the same wait: Delayed('slow_square-e23906e5-650d-48f4-8383-d0ebb73a5100') -- still just a graph, nothing ran
  the delayed only executes on request: compute() = 81
  futures: 'run THIS now, I will decide what next based on results' (imperative)
  collections: 'here is the WHOLE recipe, optimize and run it once' (declarative)
```

Same function, same argument, same cluster. One ran while the program slept; the
other is still a string in a repr.

**Why it matters.** A service that runs dask work has two layers. The outer layer
is imperative -- a request arrives, work is submitted, the handler returns, the
result is collected later or streamed. That layer wants futures. The inner layer
is a data pipeline whose shape is known, and that wants collections. OCS is
built exactly this way.

**Traps.**

- Futures require a `distributed` client. There is no `submit` on the local
  threaded scheduler.
- A `Future` holds its result in worker memory until it is garbage collected.
  Keeping a list of thousands of futures around is a memory leak with extra
  steps. Drop references once gathered.
- `client.map` over a large list creates one task per element. If each element is
  a millisecond of work, batch first.
- `client.submit(fn, big_array)` serializes `big_array` once per call. If several
  tasks need the same large input, `client.scatter` it once and pass the handle.
  See [dask-distributed](dask-distributed.md) `0202_scatter_gather`.
- Exceptions surface at `result()`/`gather()`, not at `submit()`. A fire-and-
  forget future whose result nobody collects can fail silently.
- Futures and collections mix: `client.compute(collection)` returns a future for
  a lazy graph, and `dask.delayed(future)` goes the other way.

### 0304 — Diagnostics

Source: [`../../dask/examples/0304_diagnostics.py`](../../dask/examples/0304_diagnostics.py)

**What it teaches.** The three local-scheduler diagnostic tools -- `ProgressBar`,
`Profiler`, `ResourceProfiler` -- and how to read their raw records rather than
their plots.

The workload is a chunked reduction on a 365x512x512 field:

```python
field = random_field(days=365, ny=512, nx=512, time_chunk=30, spatial_chunk=256)
graph = ((field - 0.5) ** 2).mean()
```

```
  data:  shape=(365, 512, 512), chunks=(30, 256, 256), n_chunks=52, ~15.7 MB/chunk
  graph: ((x - 0.5)**2).mean() = 222 tasks
```

**ProgressBar** is a context manager that hooks the scheduler's callbacks and
draws a live bar:

```python
with ProgressBar():
    result = float(graph.compute())
```

```
[                                        ] | 0% Completed | 71.58 us[#####################                   ] | 52% Completed | 105.16 ms[########################################] | 100% Completed | 212.36 ms
  result: 0.083334 (variance of uniform [0, 1) is 1/12 = 0.083333)
```

(The bar overwrites itself in a terminal; captured to a file it concatenates,
which is what you see above.) The result is a correctness check in disguise --
the variance of a uniform distribution on [0, 1) is exactly 1/12 = 0.083333, and
the computed 0.083334 confirms the pipeline is doing what it claims.

**Profiler** records one entry per task with start time, end time, and thread:

```python
with Profiler() as prof, ResourceProfiler(dt=sample_dt) as rprof:
    result = float(graph.compute())
rprof.close()
```

The example then aggregates the records by task-name prefix:

```python
for record in records:
    prefix = key_prefix(record.key)
    busy[prefix] += record.end_time - record.start_time
    counts[prefix] += 1
```

```
What the Profiler captured (prof.results, one record per task):
  tasks executed: 118 in 0.15 s wall time
  busy time across threads: 1.74 s -> parallelism paid off
  time by task name (top 4):
    random-mean_chunk             52 tasks    1.61 s busy
    mean_chunk                    52 tasks    0.12 s busy
    mean_combine-partial          12 tasks    0.02 s busy
    mean_agg-aggregate-finalize-hlgfinalizecompute   1 tasks    0.00 s busy
```

This table is worth dwelling on. 118 tasks *executed* out of a 222-task graph --
optimization removed nearly half before anything ran. 1.74 s of busy time
compressed into 0.15 s of wall time is a parallel efficiency of about 11.6x on 12
cores, which is close to ideal. And the name breakdown says where the time went:
`random-mean_chunk` is 92% of it, meaning the cost is dominated by *generating*
the random data, not by the reduction.

Note the name `random-mean_chunk` itself: dask fused the random generation and
the per-chunk partial mean into one task, and named the fused task after both. If
you see a hyphenated compound name in a profile, that is fusion at work.

**ResourceProfiler** samples process memory and CPU on an interval:

```
What the ResourceProfiler captured (rprof.results, one sample per 0.05 s):
  samples: 1, peak memory: 281 MB, peak CPU: 0%
  (>100% CPU means multiple threads were computing at once)
  peak memory stays far below the ~700 MB of total data: chunks stream through
```

Only one sample, because the whole computation finished in about 150 ms and the
sampler runs every 50 ms in a separate process with startup latency of its own.
The example handles the zero-sample case explicitly. The useful number that did
survive: 281 MB peak against roughly 700 MB of logical data -- chunks streamed
through and were released.

!!! note "These tools are local-only"
    `dask.diagnostics` hooks the *local* schedulers. On a distributed cluster
    they capture nothing. There you use the live dashboard or
    `distributed.performance_report(filename=...)`, which writes the same
    task-stream and profile views to a standalone HTML file you can attach to a
    ticket. The [dask-distributed](dask-distributed.md) project covers both.

**Why it matters.** "The pipeline is slow" is not actionable. "92% of busy time
is in one task name, and that name is the store read" is. These tools convert
one into the other in about four lines of code.

**Traps.**

- Wrong tool for distributed. Silently returns nothing rather than erroring.
- `ResourceProfiler` spawns a sampling process; call `.close()` or leave the
  context manager, or it lingers.
- The sampling interval sets the floor on what you can observe. Short computes
  need a small `dt`; very long ones will fill memory with samples if `dt` is
  tiny.
- Profiling adds overhead. The numbers are indicative, not a benchmark.
- `.visualize()` on either profiler renders a bokeh plot, which needs bokeh
  installed and a browser. The raw `.results` are plain namedtuples and are often
  more useful.

---

## Phase 4 — dask.dataframe

Three examples on the tabular sibling. The climate stack is array-shaped, so this
phase is deliberately short -- but `dask-geopandas`, which OCS uses for vector
aggregation, is partitioned `dask.dataframe` underneath, and shuffles are a class
of cost that has no array equivalent.

All three examples share one synthetic table: 100,000 weather observations from
25 stations across 2024, with columns `station_id`, `date`, `temperature`, and
`rainfall`.

### 0401 — Partitions

Source: [`../../dask/examples/0401_partitions.py`](../../dask/examples/0401_partitions.py)

**What it teaches.** That a dask DataFrame is a list of pandas DataFrames plus
divisions metadata, and that the metadata is what makes index-based operations
cheap.

```python
ddf: dd.DataFrame = dd.from_pandas(df, npartitions=4)
```

```
One pandas DataFrame: 100k weather observations from 25 stations.
  rows=100000, columns=['station_id', 'date', 'temperature', 'rainfall']
  index: RangeIndex 0..99999 (sorted, unique)

dd.from_pandas(df, npartitions=4) cuts the table along its index:
  .npartitions = 4
  .divisions   = (0, 25000, 50000, 75000, 99999)
  divisions are the index boundaries: partition i holds rows with
  divisions[i] <= index <= divisions[i+1] (last edge inclusive).
```

Note the asymmetry in the last edge: `divisions` has `npartitions + 1` entries,
and the final one is the last index value rather than one past it.

**A partition is a pandas DataFrame, full stop.**

```python
part0 = ddf.partitions[0].compute()
lengths = ddf.map_partitions(len).compute()
```

```
Each partition is nothing exotic -- it is a pandas DataFrame:
  type(ddf.partitions[0].compute()) = DataFrame
  partition 0: rows 0..24999, len=25000
  map_partitions(len) -> one number per partition: [25000, 25000, 25000, 25000]
  sum of partition lengths = 100000 (the full table)
```

`map_partitions` is the dataframe equivalent of `map_blocks`: your function
receives a real pandas DataFrame and returns whatever you like. It is the escape
hatch for anything the dask API does not cover, and, like `map_blocks`, it is
partition-local.

**Divisions prune work.**

```python
lookup_known = ddf.loc[60_000]
unknown = ddf.clear_divisions()
lookup_unknown = unknown.loc[60_000]
```

```
Known divisions let dask route index lookups to ONE partition:
  ddf.known_divisions = True
  ddf.loc[60_000] task count = 6  (only the owning partition is read)

Without divisions, dask cannot know which partition holds an index:
  .divisions = (None, None, None, None, None)
  .known_divisions = False
  same .loc[60_000] task count = 12  (every partition must be checked)
```

6 versus 12 on four partitions. On 400 partitions it would be 6 versus roughly
1200. Divisions are the dataframe analogue of chunk-based culling in arrays: they
let the planner prove which pieces are irrelevant.

**Sorting is the prerequisite.**

```python
by_date = ddf.set_index("date")
march = by_date.loc["2024-03-01":"2024-03-31"]
```

```
Divisions only make sense over a SORTED index -- set_index sorts:
  set_index('date') -> known_divisions=True
  date divisions: ['2024-01-01', '2024-03-30', '2024-06-16', '2024-09-18', '2024-12-31']
  .loc['2024-03-01':'2024-03-31'] computes 8409 rows,
  touching only partitions whose division range overlaps March.
```

Sorted-by-date partitions turn a date-range query into a partition-range scan.
This is the one case where paying for a shuffle up front is clearly worth it: if
every query is a date range and the table is written once, `set_index("date")`
before writing to parquet buys cheap queries forever.

**Why it matters.** Vector aggregation in OCS joins region geometries against
gridded statistics, and the join key determines whether the operation is
per-partition or a shuffle. Divisions are what make the difference visible before
you run it.

**Traps.**

- `dd.from_pandas` requires the data to already be in memory, so it is not a
  scaling tool -- it is a testing convenience. Real pipelines start with
  `read_parquet` / `read_csv`.
- Many operations silently discard divisions (`reset_index`, most `apply`,
  arbitrary `map_partitions`). Check `known_divisions` after anything that
  reshapes the index.
- `set_index` is a full shuffle. Do it once, not per query, and preferably before
  writing to disk.
- `npartitions` is a target, not a guarantee -- dask may produce fewer.
- Partition sizes should land in the 100 MB range, the same rule as chunks. Too
  many tiny partitions has the same failure mode as too many tiny chunks.

### 0402 — Groupby shuffle

Source: [`../../dask/examples/0402_groupby_shuffle.py`](../../dask/examples/0402_groupby_shuffle.py)

**What it teaches.** The three cost classes of dataframe operations, measured
against each other on the same data.

```
100k observations from 25 stations, split into 4 partitions.
  npartitions=4, baseline task count = 4 (one task per partition)
```

**Class 1: per-partition operations move nothing.**

```python
with_f = ddf.assign(temp_f=ddf["temperature"] * 9 / 5 + 32)
warm = ddf[ddf["temperature"] > 25.0]
```

```
  column arithmetic (assign temp_f): 24 tasks
  row filter (temperature > 25):     16 tasks
  task count grows only linearly with npartitions -- the cheap class.
```

Every partition is transformed alone. Doubling the partitions doubles the tasks
and nothing else. On a cluster this class never touches the network.

**Why groupby cannot be class 1.** The example first demonstrates the problem:

```python
stations_per_part = ddf.map_partitions(lambda p: p["station_id"].nunique()).compute()
```

```
Every partition contains rows from (almost) every station:
  distinct stations per partition: [25, 25, 25, 25] (of 25)
  so a per-station mean needs data from ALL partitions combined.
```

Each partition holds rows from all 25 stations, so no partition can compute any
station's mean by itself.

**Class 2: tree reduction moves only partials.**

```python
means = ddf.groupby("station_id")[["temperature", "rainfall"]].mean()
```

```
Class 2 -- groupby aggregations: partial result per partition,
then combine. Only tiny partials move, never the raw rows:
  groupby('station_id').mean(): 13 tasks
  result is small: 25 rows (one per station), e.g.:
                temperature  rainfall
    station_id
    0              9.980134  2.422048
    1             10.156041  2.487058
```

13 tasks -- *fewer* than the 24 for a column assignment. Each partition computes a
25-row table of sums and counts; the four small tables are combined; the result
is 25 rows. The 100,000 raw rows never move.

This is why `groupby(...).mean()` is not something to fear. It is
`groupby(...).apply(arbitrary_function)` that is dangerous, because an arbitrary
function cannot be decomposed into partial-and-combine and dask has to gather
each group's rows into one place -- a shuffle.

**Class 3: shuffles move everything.**

```python
by_temp = ddf.sort_values("temperature")
by_station = ddf.set_index("station_id")
```

```
Class 3 -- shuffles: every output partition may need rows from every
input partition, so ALL raw data is repartitioned and moved:
  sort_values('temperature'): 34 tasks
  set_index('station_id'):    38 tasks
  (split/transfer/merge tasks between every pair of partitions)
```

The cost ladder in one table:

```
Task counts side by side (4 partitions, same data):
  baseline (just the partitions)       4 tasks
  assign column arithmetic            24 tasks
  filter rows                         16 tasks
  groupby mean (tree reduce)          13 tasks
  sort_values (shuffle)               34 tasks
  set_index (shuffle)                 38 tasks
  On a real cluster the shuffle rows also mean network + disk traffic.
```

On four partitions the shuffle rows look merely 2-3x worse. That understates
things badly, because shuffle task count grows with the *square* of the partition
count in the naive case: `p` input partitions each split into `p` pieces. At 400
partitions that is 160,000 transfer tasks. Modern dask uses a smarter
peer-to-peer shuffle that avoids the quadratic explosion, but the data still all
moves, and on a cluster it moves over the network and often through disk.

**Why it matters.** "Aggregate early, sort never" is the design rule for any
dataframe pipeline meant to run on a cluster. A pipeline that filters and
aggregates before it joins will finish; one that sorts a large table first may
not.

**Traps.**

- `groupby(...).apply(fn)` is a shuffle, not a tree reduce. So is
  `groupby(...).transform`. Use the built-in aggregations when you can, and
  `Aggregation` for custom partial/combine pairs when you cannot.
- Merges are cheap when one side is small enough to broadcast, or when both are
  already indexed on the join key. Otherwise they shuffle.
- Task count is a proxy, not a measure. A shuffle's cost is bytes moved, and
  task count only correlates with it.
- Skew ruins tree reductions too. If one group holds 90% of the rows, one task
  does 90% of the work, and the batch cannot finish faster than that task.

### 0403 — Pandas boundary

Source: [`../../dask/examples/0403_pandas_boundary.py`](../../dask/examples/0403_pandas_boundary.py)

**What it teaches.** Where to stop using dask. The example measures the
small-data anti-pattern directly, then shows the correct division of labour, then
shows the case where dask genuinely wins.

**The anti-pattern, measured.**

```python
t_pandas = best_of(lambda: df.groupby("station_id")[["temperature", "rainfall"]].mean())
t_dask = best_of(lambda: ddf.groupby("station_id")[["temperature", "rainfall"]].mean().compute())
```

```
Anti-pattern: 100k rows fit comfortably in memory, but we wrap them anyway.
  in-memory size: ~3.2 MB -- pandas territory
  same groupby-mean:  pandas    1.1 ms
                      dask     10.8 ms  (~10x slower)
  Graph construction, scheduling, and result concatenation are pure
  overhead here -- there is no out-of-core or parallel win to buy.
```

(Machine-dependent; `best_of` takes the minimum of five runs to strip noise, and
the ratio has been stable across runs.)

Ten times slower for identical output on 3.2 MB. There is no configuration that
fixes this, because the overhead is not a bug -- it is the cost of the machinery
that makes out-of-core work possible, being paid in a situation where there is
nothing out-of-core about the problem.

**The right pattern: cross the boundary once, when the data is small.**

```python
monthly = ddf.assign(month=ddf["date"].dt.month).groupby(["station_id", "month"])["rainfall"].sum()
summary = monthly.compute()          # the boundary crossing: dask -> pandas
wettest = summary.groupby("station_id").max().nlargest(3)
```

```
Right pattern: keep the HEAVY reduction lazy, then .compute() once
the result is small, and finish the analysis in plain pandas:
  lazy: dask.Series -- nothing has run yet
  .compute() -> pandas.Series with 300 rows (100000 rows reduced to 25 stations x 12 months)
  final analysis is trivial pandas -- wettest station-months (mm):
    station_id
    6    987.23
    8    977.54
    0    950.57
```

100,000 rows in, 300 rows out. Everything after that line is pandas, and pandas
is better at it: nicer API, better error messages, instant results, no scheduler.
The skill is recognizing the moment the data becomes small.

**Where dask earns its keep.**

```python
ddf.to_parquet(path)
subset: dd.DataFrame = dd.read_parquet(path, columns=["station_id", "rainfall"])
totals = subset.groupby("station_id")["rainfall"].sum().compute()
```

```
Where dask.dataframe earns its keep is on-disk data -- and parquet
lets it read only the columns a query needs:
  to_parquet wrote one file per partition: ['part.0.parquet', 'part.1.parquet', 'part.2.parquet', 'part.3.parquet']
  read_parquet(columns=['station_id', 'rainfall']) -> columns ['station_id', 'rainfall']
  (date and temperature are never read from disk at all)
  lazy reduction over the subset -> 25 station totals,
  e.g. station 0: 9789.9 mm across 100000 source rows
```

One file per partition, and column projection pushed into the reader. `date` and
`temperature` are not read, not decompressed, not allocated. This is the
tabular analogue of chunk culling in arrays, and it is the reason
`read_parquet` + a lazy reduction is the canonical dask.dataframe entry point.

**Why it matters.** OCS produces small per-region statistics from large gridded
inputs. The heavy end is dask; the reporting end is pandas; the boundary is a
single `.compute()`. Getting the boundary in the wrong place -- either computing
too early and blowing up memory, or staying in dask for the final 300 rows -- is
the common failure.

**Traps.**

- The most common form of "computing too early" is `df = ddf.compute()` near the
  top of a script, followed by pandas code. That is not a dask pipeline; it is a
  slow pandas pipeline.
- The second most common is calling `.compute()` inside a loop over groups. Batch
  it.
- `to_parquet` writes one file per partition. 10,000 partitions means 10,000 tiny
  files, which object stores hate. Repartition before writing.
- `len(ddf)` and `ddf.shape[0]` compute. So does printing the dataframe.
- CSV cannot do column projection or predicate pushdown, and cannot be split
  reliably without scanning. Use parquet.

---
## Phase 5 — Dask-backed xarray

This phase is the open-climate-service execution model. xarray provides names and
labels; dask provides chunks and a scheduler; zarr provides the bytes. Four
examples walk the whole path, ending with a complete write-open-slice-derive-write
round trip.

If you have not met xarray, read [that project](xarray.md) first -- this phase
assumes `DataArray`, `Dataset`, dims, coords, and `sel`.

### 0501 — Chunked xarray

Source: [`../../dask/examples/0501_chunked_xarray.py`](../../dask/examples/0501_chunked_xarray.py)

**What it teaches.** That xarray is agnostic about its backing array, that giving
it a dask array makes every operation lazy, and how to tell at a glance which
kind you have.

```python
def make_field(days: int = 365) -> xr.DataArray:
    arr = random_field(days=days)
    return xr.DataArray(
        arr,
        dims=("time", "y", "x"),
        coords={
            "time": pd.date_range("2023-01-01", periods=days, freq="D"),
            "y": np.arange(256, dtype=np.float64),
            "x": np.arange(256, dtype=np.float64),
        },
        name="t2m",
        attrs={"units": "degC", "long_name": "2 metre temperature"},
    )
```

```
A DataArray built from a dask array keeps the dask array as its .data:
  underlying array: shape=(365, 256, 256), chunks=(30, 128, 128), n_chunks=52, ~3.9 MB/chunk
  type(da_lazy.data) = Array  (dask, not numpy)
  is a dask Array?  True
  graph size: 52 tasks -- one per chunk, no values exist yet
```

`.data` is the backing array; `.values` is always numpy and therefore always
computes. That distinction is the single most useful thing to internalize about
dask-backed xarray. `type(obj.data).__name__` returning `Array` means dask;
`ndarray` means numpy and eager.

**The repr advertises it.**

```
  | <xarray.DataArray 't2m' (time: 365, y: 256, x: 256)> Size: 191MB
  | dask.array<random, shape=(365, 256, 256), dtype=float64, chunksize=(30, 128, 128), chunktype=numpy.ndarray>
  | Coordinates:
```

A numpy-backed DataArray prints its values there. A dask-backed one prints
`dask.array<...>` with the chunk size. In a notebook the HTML repr goes further
and draws the chunk grid, which is worth clicking on.

**Dataset-level chunk view.**

```python
ds = da_lazy.to_dataset()
for dim, sizes in dict(ds.chunks).items():
    print(f"  {dim}: {sizes}")
```

```
As a Dataset, ds.chunks maps each dim to its chunk sizes:
  time: (30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 5)
  y: (128, 128)
  x: (128, 128)
```

`ds.chunks` is a dict keyed by dimension *name* rather than by axis position,
which is the whole reason to use xarray. It raises if two variables disagree
about a dimension's chunking -- a useful early warning before a zarr write.

**Operations stay lazy.**

```python
monthly_map = da_lazy.mean(dim="time")
anomaly = da_lazy - monthly_map
```

```
  da.mean(dim='time')  -> 124 tasks, .data is Array
  da - mean            -> 180 tasks, .data is Array
  building both graphs took 2.0 ms -- graph construction, not number crunching
```

Note `dim="time"` rather than `axis=0`. Same dask graph underneath, better code.

**The eager twin.**

```python
values = da_lazy.values                     # materializes every chunk
da_eager = xr.DataArray(values, dims=da_lazy.dims, coords=da_lazy.coords, name="t2m")
```

```
  da_lazy.values ran the whole graph in 0.13 s -> ndarray, 191 MB
  type(da_eager.data) = ndarray  (numpy: values already in memory)
  da_eager.mean(dim='time') executed immediately (61.3 ms) -> ndarray
  same xarray API, opposite execution model: numpy runs now, dask runs later
```

Identical API, identical results, opposite timing. Which one you have is a
property of the object, not of the code operating on it -- which is precisely why
a function that works fine in a test with a small numpy fixture can blow up
memory in production against a dask-backed store.

**Why it matters.** OCS never holds a full `(time, y, x)` cube in memory. Every
variable is a dask array wearing xarray labels, and nothing touches bytes until
something forces it. This example is the definition of that arrangement.

**Traps.**

- `.values` computes, always, silently. So does `float(da)`, `int(da)`,
  `np.asarray(da)`, `bool(da)`, and passing the object to matplotlib.
- Coordinates are usually numpy even when data is dask. That is normally right --
  a time index should be in memory -- but it means `ds.time.values` is free while
  `ds.t2m.values` is a full read.
- Constructing a DataArray around a dask array does not copy or compute; it wraps.
- `ds.chunks` on a Dataset whose variables disagree raises `ValueError`. Use
  `ds.chunksizes` (per-variable) when that is expected.
- Attributes do not survive every operation. `keep_attrs=True` or
  `xr.set_options(keep_attrs=True)` if the metadata matters downstream.

### 0502 — Graphs through xarray

Source: [`../../dask/examples/0502_graphs_through_xarray.py`](../../dask/examples/0502_graphs_through_xarray.py)

**What it teaches.** That xarray's high-level verbs -- `groupby`, `resample`,
`coarsen` -- are graph compilers. Each one is a few milliseconds of Python that
emits a few hundred dask tasks.

```
Start from a lazy daily field; its graph is one task per chunk:
  shape=(365, 256, 256), chunks(time)=(30, 30, ..., 5)
  base graph: 52 tasks
```

**groupby.**

```python
climatology = da.groupby("time.month").mean()
```

```
groupby('time.month').mean() -- a monthly climatology, still lazy:
  result: dims=('month', 'y', 'x'), shape=(12, 256, 256)
  tasks: 52 -> 338  (built in 10.6 ms, zero chunks computed)
```

52 to 338. The jump is large because grouping by month cuts across the 30-day
chunk boundaries: January is days 0-30, which straddles chunk 0 and chunk 1, so
dask must slice and regroup. The `time.month` virtual coordinate is xarray's
`.dt` accessor applied to the index at graph-build time -- it is computed on the
coordinate, which is in memory, not on the data.

**resample.**

```python
monthly = da.resample(time="MS").mean()
```

```
resample(time='MS').mean() -- monthly means along the real calendar:
  result: dims=('time', 'y', 'x'), shape=(12, 256, 256)
  tasks: 52 -> 324  (built in 8.1 ms)
```

`resample` differs from `groupby("time.month")` in a way that matters for climate
work: `groupby` bins all Januaries together across all years (a climatology),
while `resample` produces one value per calendar month in sequence (a time
series). Same task-count neighbourhood, entirely different meaning. Over a single
year the shapes coincide, which is exactly the situation in which people confuse
them.

**coarsen -- the pyramid operation.**

```python
coarsened: Any = da.coarsen(y=2, x=2)
level_up: xr.DataArray = coarsened.mean()
```

```
coarsen(y=2, x=2).mean() -- one zoom level up; this is the open-climate-service
pyramid-level operation, and it too just adds tasks to the dask graph:
  result: shape (365, 256, 256) -> (365, 128, 128)  (each output cell averages a 2x2 block)
  tasks: 52 -> 208  (built in 2.1 ms)
```

`coarsen` is the cheapest of the three -- 52 to 208, built in 2.1 ms -- because
2x2 blocks nest perfectly inside 128x128 chunks. No chunk boundary is crossed;
every output block comes from exactly one input block. This is the reason tile
pyramids are built with `coarsen` and factors of 2 rather than with
interpolation to arbitrary resolutions.

**Graphs compose.**

```python
clim_coarsened: Any = climatology.coarsen(y=2, x=2)
pyramid_clim: xr.DataArray = clim_coarsened.mean()
```

```
  climatology 338 tasks -> coarsened climatology 482 tasks
  three API calls deep and still not one chunk of data has been touched
```

**And then compute.**

```
.compute() is the only step that executes:
  compute took 0.17 s -- far above the ms-scale graph builds, because chunks finally ran
  result: ndarray-backed, shape=(12, 128, 128), mean=0.4999
```

Milliseconds to build, hundreds of milliseconds to run. The ratio only grows with
real data: on a store where each chunk is a 100 MB read from object storage, the
build stays at milliseconds and the run becomes minutes.

**Why it matters.** When OCS builds monthly aggregates or downsamples a pyramid
level, it calls these exact methods. The task counts here are what a request's
graph looks like before the scheduler ever sees it.

**Traps.**

- `groupby` over a label that cuts across chunks fragments the layout. After
  `groupby("time.month")` on 30-day chunks you may end up with one chunk per
  group, or worse -- [0504](#0504-lazy-zarr-pipeline) hits exactly this and has
  to rechunk before writing.
- `groupby(...).map(fn)` with an arbitrary function is the xarray equivalent of a
  dataframe shuffle: no partial/combine decomposition, so groups get gathered.
- `coarsen` requires the dimension to divide evenly unless you pass
  `boundary="trim"` or `"pad"`. The default raises.
- `resample` with an offset that does not align with chunk boundaries costs the
  same fragmentation as `groupby`.
- Building an enormous graph is itself work. Ten chained verbs over 10,000 chunks
  can take seconds of pure graph construction and a lot of memory before anything
  is scheduled.

### 0503 — compute, load, persist

Source: [`../../dask/examples/0503_compute_persist.py`](../../dask/examples/0503_compute_persist.py)

**What it teaches.** Three ways to turn a lazy graph into real numbers, what each
does to the object you called it on, and what re-computation costs when you
choose wrong.

The pipeline is a standardized anomaly:

```python
anomaly = ((da - da.mean(dim="time")) / da.std(dim="time")).rename("t2m_anomaly")
```

```
  anomaly: 312 tasks, .data is Array, 191 MB when real
```

**`.compute()` returns a new object.**

```
.compute() -- returns a NEW in-memory object; the original stays lazy:
  compute took 0.25 s
  computed.data is ndarray  (numpy, 191 MB now in RAM)
  anomaly.data is still Array with 312 tasks -- untouched
```

This is the trap in the trio. `anomaly.compute()` does *not* make `anomaly`
eager. If you write `ds.compute()` and then keep using `ds`, you paid for the
computation and threw the result away.

**`.load()` fills in place.**

```python
loaded = (make_field() - 0.5).rename("t2m_shifted")
loaded.load()
```

```
.load() -- same materialization, but it REPLACES the data on the object itself:
  before: loaded.data is Array
  after:  loaded.data is ndarray  (no new object; the graph is gone for good)
```

`.load()` is what you want when the object is the thing you will keep using. It
is irreversible -- the graph is discarded.

**`.persist()` keeps dask.**

```python
persisted = anomaly.persist()
```

```
.persist() -- runs the graph, keeps results as concrete chunks STILL wrapped in dask:
  persisted.data is Array  (still dask: chunked API intact)
  tasks: 312 -> 52  (collapsed to one task per finished chunk)
  chunk layout preserved: time chunks = (30, 30, ..., 5)
```

312 tasks collapse to 52 -- one per finished chunk. Each remaining "task" is just
"here is a concrete numpy block". The object still behaves as a dask array:
chunked, lazily sliceable, ready to feed further lazy operations that will start
from the persisted chunks instead of from the source.

On a distributed cluster the distinction sharpens: `compute` pulls the result
across the wire into the client process, while `persist` leaves the chunks
distributed across worker memory. For anything large, `persist` is the one you
want, because the client usually cannot hold the result at all.

**The three-way summary:**

| | Runs the graph | Result lives | Original object | Still chunked |
|---|---|---|---|---|
| `.compute()` | yes | client, numpy | unchanged, still lazy | no |
| `.load()` | yes | client, numpy | replaced in place | no |
| `.persist()` | yes | workers, dask chunks | unchanged, still lazy | yes |

**What re-computation costs.**

```python
def time_reduction(da: xr.DataArray) -> float:
    t0 = time.perf_counter()
    da.mean().compute()
    return time.perf_counter() - t0
```

```
Memory consequences -- laziness is free, materialization is not:
  lazy anomaly:  ~0 MB held (only a 312-task recipe)
  persisted:     191 MB of real chunks pinned in RAM until it is dropped

Re-computation cost -- the same reduction, twice, off lazy vs persisted:
  lazy:      1st   145.9 ms, 2nd   142.6 ms  (graph re-runs each time)
  persisted: 1st    23.8 ms, 2nd    22.3 ms  (already real)
  persisted repeat is ~6x faster: you paid once in RAM to stop paying in CPU
```

(Machine-dependent.) The lazy second run is not faster than the first -- there is
no cache anywhere in dask. Persisting bought a 6x repeat speedup for 191 MB of
pinned memory.

**The decision rule.** Persist when *all three* hold: the intermediate is used by
several later computations, it fits comfortably in memory (worker memory, on a
cluster), and it is expensive to recompute. If any one fails, stay lazy.

**Why it matters.** Choosing when to materialize is the memory/CPU dial of the
whole OCS execution model. Persisting too eagerly is the most common way to kill
a worker; persisting not at all is the most common way to compute the same
datacube five times.

**Traps.**

- `.compute()` not mutating the original is a genuine and frequent bug.
- `persist` is asynchronous on a distributed cluster: it returns immediately and
  the chunks fill in behind you. Use `distributed.wait(x)` if you need the fill
  to be finished before proceeding.
- Persisted data occupies memory until every reference is dropped. In a notebook,
  the `Out[n]` history counts as a reference.
- Persisting an intermediate that is then used once is strictly worse than not
  persisting it.
- Persisting a whole dataset "just in case" at the top of a pipeline is the
  cluster-killer pattern.

### 0504 — Lazy zarr pipeline

Source: [`../../dask/examples/0504_lazy_zarr_pipeline.py`](../../dask/examples/0504_lazy_zarr_pipeline.py)

**What it teaches.** The complete OCS request path in one file: write a chunked
store, reopen it lazily, slice to a window, derive a product, write the result --
with numbers at every step proving that the small request never paid for the
whole store.

**Step 1, write.**

```python
ds.to_zarr(source_store, consolidated=False)
```

```
Step 1 -- write a chunked year of daily data to zarr:
  wrote (365, 256, 256) in chunks of (30, 128, 128) in 0.26 s
```

`consolidated=False` appears on every zarr call in this example, because
consolidated metadata is not part of the zarr v3 spec yet and writing it emits a
`ZarrUserWarning`. Each dask chunk becomes one zarr chunk, one object on disk.

**Step 2, open lazily.**

```python
opened = xr.open_zarr(source_store, consolidated=False)
t2m = opened.t2m
```

```
Step 2 -- open_zarr is lazy: metadata only, one task per stored chunk:
  chunks on disk -> chunks in dask: time=(30, 30, ..., 5)
  graph: 53 tasks, .data is Array, nothing read yet
```

53 tasks: 52 chunk reads plus one assembly. Opening read only the JSON metadata.
The store's chunk layout became dask's chunk layout automatically -- which is why
the write-time decision is so consequential.

**Step 3, slice.**

```python
window = t2m.sel(y=slice(0.0, 127.0), x=slice(0.0, 127.0), time=slice("2023-06-01", "2023-08-31"))
```

```
Step 3 -- select a spatial window and a time slice (still lazy):
  window: shape=(92, 128, 128)  (92 of 365 days, 1 of 4 spatial blocks)
  time chunks inside the window: (29, 30, 30, 3)
```

Look at the resulting time chunks: `(29, 30, 30, 3)`. The window starts mid-chunk
and ends mid-chunk, so the first and last chunks are partial. This is completely
normal, completely legal for dask, and -- note for later -- *not* legal for zarr,
since the first chunk (29) is smaller than the second (30).

**Step 4, derive.**

```python
def monthly_anomaly(da: xr.DataArray) -> xr.DataArray:
    climatology = da.groupby("time.month").mean()
    anom = da.groupby("time.month") - climatology
    return anom.rename("t2m_anomaly")
```

```
Step 4 -- derive the monthly anomaly; compare the graph against the full domain:
  windowed anomaly graph: 460 tasks
  full-domain equivalent: 6179 tasks
  compute windowed: 0.04 s   compute full domain: 0.49 s  (~12x)
  the lazy pipeline reads only the stored chunks the window overlaps -- the
  rest of the store is never touched (full domain shown only for contrast)
```

460 tasks versus 6179, 0.04 s versus 0.49 s. The 13x task ratio and 12x time
ratio come from the same source: the window overlaps 4 time chunks and 1 spatial
block out of 52 total chunks. The slice happened before the graph was built, so
the graph never contained the rest.

(Note the absolute size of the full-domain graph: 6179 tasks to compute a monthly
anomaly over 52 chunks. `groupby`-based anomalies are expensive in graph terms
because each of 12 groups must be assembled from pieces of several chunks. This
is a good example of a cheap-looking two-line function that produces a lot of
scheduling.)

**Step 5, write the result -- with a rechunk.**

```python
window_anom.chunk({"time": 92}).to_dataset().to_zarr(anomaly_store, consolidated=False)
```

```
Step 5 -- write the derived result to a second zarr store:
  (groupby arithmetic fragmented time into 92 single-day chunks; rechunk to one
   clean block so the output store is not 92 tiny objects)
  wrote (92, 128, 128) in 0.06 s
```

This is the fragmentation trap in the flesh. `groupby("time.month") - climatology`
left time chunked into 92 blocks of one day each. Written straight to zarr that
becomes 92 tiny objects, each about 128 KB -- terrible for object storage, where
per-object latency dominates. One `.chunk({"time": 92})` before the write fixes
it.

**The evidence on disk.**

```
Evidence on disk -- the result store holds only the derived window:
  source store:    179.6 MB  (365, 256, 256)
  anomaly store:     11.5 MB  (92, 128, 128)
  sanity check: window anomaly mean = -0.000000 (deviations cancel by construction)
```

The anomaly mean being zero to six decimals is the correctness check: subtracting
each month's mean from its own days must cancel.

**Why it matters.** This is the OCS request path, end to end. Open a normalized
`(time, y, x)` store, slice to the requested window, run the process graph, write
or stream the result. Every performance property of the service falls out of the
fact that step 3 happens before step 4.

**Traps.**

- Always slice before deriving. Reversing steps 3 and 4 gives the 6179-task graph
  and the same final answer.
- Check the chunk layout before every `to_zarr`. Derived layouts are frequently
  fragmented, frequently ragged, and occasionally illegal.
- `open_zarr(..., chunks=...)` can override the stored layout, but doing so adds
  a rechunk on top of the read. Prefer matching the store.
- Writing to a store you also have open for reading is a good way to get
  confusing results. Write to a new store.
- `consolidated=True` speeds up opening stores with many variables by reading one
  metadata document instead of many. It is a zarr v2 feature whose v3 status is
  in flux; know which you are on.

---

## Phase 6 — Patterns and pitfalls

Three examples encoding production wisdom: the zarr chunk-legality rule that OCS
hit in its downloader, the chunk-size trade-off measured rather than asserted,
and a checklist of graph-level habits with timings attached.

### 0601 — Zarr-legal chunks

Source: [`../../dask/examples/0601_zarr_legal_chunks.py`](../../dask/examples/0601_zarr_legal_chunks.py)

**What it teaches.** That dask and zarr disagree about what a legal chunk layout
is, how an ordinary operation produces an illegal one, and the OCS helper that
fixes it minimally.

**The rule.** dask accepts any chunk tuple whatsoever. zarr stores a single chunk
shape per array, so every chunk along a dimension must be that size, except the
last, which may be smaller. `(30, 30, 5)` is legal. `(5, 30, 30)` is not.

**How you get there.** The example builds a store the way a downloader does, by
appending pieces along time:

```python
pieces = [
    _piece(30, "2024-01-01", seed=1),
    _piece(30, "2024-01-31", seed=2),
    _piece(5, "2024-03-01", seed=3),
]
field = xr.concat(pieces, dim="time")
```

```
A store grows by appending downloads along time: 30 + 30 + 5 days.
  time chunks after concat: (30, 30, 5)
  Legal for zarr: uniform 30s with the remainder (5) LAST.
```

Then it flips the axis, which is what `ensure_north_up` does to a south-up
source:

```python
reversed_ds = ds.isel(time=slice(None, None, -1))
```

```
A south-up source gets flipped north-up: isel(time=slice(None, None, -1)).
(OCS does the same flip on y; the chunk mechanics are identical.)
  time chunks after reversal: (5, 30, 30)
  Reversing the axis reverses the chunk TUPLE too: the remainder now leads.
  Dask is fine with (5, 30, 30). Zarr is not.
```

That is the whole bug. Reversing an axis reverses its chunk tuple. A perfectly
legal trailing remainder becomes an illegal leading one. Nothing warns, because
from dask's point of view nothing happened.

**What zarr says.** The example attempts the write and catches the failure, so
here is the real error text:

```
Attempting to_zarr with the illegal layout:
  ValueError: Zarr requires uniform chunk sizes except for final chunk. Variable named 't2m' has incompatible dask chunks: ((5, 30, 30), (64,), (64,)). Consider rechunking using `chunk()`.
```

Good error. It names the constraint, the variable, the exact chunk tuple, and the
fix. It also fails *before* writing anything, so you do not get a half-written
store.

**The fix.** OCS's `_uniform_chunks`, re-implemented:

```python
def uniform_chunks(ds: xr.Dataset) -> xr.Dataset:
    targets: dict[Hashable, int] = {}
    for var in ds.data_vars.values():
        for dim, chunks in zip(var.dims, var.chunks or (), strict=False):
            if len(set(chunks[:-1])) <= 1 and chunks[-1] <= chunks[0]:
                continue  # already legal: uniform except a smaller final chunk
            targets[dim] = max(targets.get(dim, 0), max(chunks))
    if not targets:
        return ds
    return ds.chunk(targets)
```

Four design decisions worth reading closely.

1. **The legality test is exactly zarr's rule.** `len(set(chunks[:-1])) <= 1`
   means every chunk but the last is the same size; `chunks[-1] <= chunks[0]`
   means the last is no larger. Note `<= 1` rather than `== 1`, which handles the
   single-chunk case where `chunks[:-1]` is empty.
2. **Only offending dims are rechunked.** A blanket `ds.chunk(...)` would rewrite
   the graph for dimensions that were already fine, which costs tasks for
   nothing.
3. **The target is `max(chunks)`.** Rechunking to the largest existing chunk
   restores the pre-reversal layout, because dask fills chunks uniformly from the
   start and leaves the remainder last.
4. **It returns the input unchanged when nothing is wrong.** Combined with
   `rechunk`'s free no-op ([0204](#0204-rechunking)), calling this before every
   write is cheap enough to be unconditional.

```
Fix: uniform_chunks(ds) -- detect dims that are non-uniform except the
final chunk, rechunk them to the max existing chunk (here 30).
Pattern credit: open-climate-service's _uniform_chunks in its downloader.
  time chunks after fix: (30, 30, 5)
  to_zarr succeeded.

Reading the store back:
  shape=(65, 64, 64), time chunks=(30, 30, 5)
  first time label: 2024-03-05 (reversed order preserved)
```

The reversal is preserved in the data -- the first time label is the last day of
the last appended piece. Only the *chunk boundaries* moved.

**Why it matters.** This is a real bug from a real production downloader,
reproduced in 60 lines. Anything that appends along a dimension and then
transforms it -- flips, reorders, concatenates unevenly sized pieces -- can
produce a layout zarr will not accept, at the last step of a long pipeline.

**Traps.**

- The error arrives at write time, after all the expensive work. Validate the
  layout before you compute, not after.
- Other operations produce illegal layouts too: `concat` of unevenly sized
  pieces, `sel` with a slice starting mid-chunk (see the `(29, 30, 30, 3)` in
  [0504](#0504-lazy-zarr-pipeline)), `where`/`isel` with fancy indexing.
- You can pass `encoding={"var": {"chunks": (...)}}` to `to_zarr` to set the
  stored chunk shape, but that does not repair a dask layout that cannot be
  written -- you still need the rechunk.
- Rechunking to `max(chunks)` is right for the reversal case. For a layout that
  is ragged in the middle it may not be what you want; check the result.
- `region=` writes have their own alignment rules on top of this one: the region
  must start and end on stored chunk boundaries.

### 0602 — Chunk sizing

Source: [`../../dask/examples/0602_chunk_sizing.py`](../../dask/examples/0602_chunk_sizing.py)

**What it teaches.** The chunk-size trade-off with numbers on both ends. Same
data, same computation, three layouts.

```python
layouts = [
    ("tiny    (5, 32, 32)", 5, 32),
    ("sensible (90, 128, 128)", 90, 128),
    ("giant   (365, 256, 256)", 365, 256),
]
for label, time_chunk, spatial_chunk in layouts:
    arr = random_field(days=365, ny=256, nx=256, time_chunk=time_chunk, spatial_chunk=spatial_chunk, seed=0)
    expr = (2.0 * arr + 1.0).mean()
    tasks = task_count(expr)
    seconds = timed_compute(expr)
```

```
Same data, same computation -- (2*x + 1).mean() over (365, 256, 256), ~191 MB.
Only the chunk layout changes:

  tiny    (5, 32, 32)
    shape=(365, 256, 256), chunks=(5, 32, 32), n_chunks=4672, ~0.0 MB/chunk
    tasks=19377, wall=1.27s
  sensible (90, 128, 128)
    shape=(365, 256, 256), chunks=(90, 128, 128), n_chunks=20, ~11.8 MB/chunk
    tasks=86, wall=0.04s
  giant   (365, 256, 256)
    shape=(365, 256, 256), chunks=(365, 256, 256), n_chunks=1, ~191.4 MB/chunk
    tasks=5, wall=0.17s

Ratios against the sensible layout:
  tiny    (5, 32, 32):  225.3x tasks,  33.2x wall time
  sensible (90, 128, 128):    1.0x tasks,   1.0x wall time
  giant   (365, 256, 256):    0.1x tasks,   4.5x wall time
```

(Machine-dependent, and the giant row in particular is sensitive to how many
cores are available. The shape of the curve is not.)

**Reading the U.** Both extremes lose, for opposite reasons.

*Tiny:* 4672 chunks, 19,377 tasks, 33x slower. Each task multiplies a 5x32x32
block -- 5120 elements, a few microseconds of arithmetic -- and pays hundreds of
microseconds of scheduling for the privilege. Over 95% of that 1.27 s was
bookkeeping. Note also `~0.0 MB/chunk` in the report: each chunk is 41 KB, which
on object storage would mean 4672 HTTP requests to read one array.

*Giant:* one chunk, 5 tasks, still 4.5x slower than sensible. There is nothing to
parallelize -- eleven of twelve cores idle -- and the whole 191 MB must be
resident, along with the intermediate from `2 * x + 1`. On an array larger than
memory, this layout does not run slowly; it does not run.

*Sensible:* 20 chunks of about 12 MB, 86 tasks, and the fastest of the three. It
has enough chunks to occupy the cores with a bit of slack for load balancing, and
each task does enough real work that the fixed overhead disappears into it.

**The rule of thumb.**

```
Rule of thumb: aim for chunks around ~100 MB (tens of MB to a few
hundred), and enough chunks to keep every core busy -- but not many more.
```

The ~100 MB figure is dask's own recommendation and is discussed at length in the
[upstream chunks page](https://docs.dask.org/en/stable/array-chunks.html). The
reasoning behind it is in the [chunking deep section](#chunking-the-deep-section)
below.

**How OCS applies it.**

```
open-climate-service derives chunk sizes from the data itself:
  - time chunks from the temporal resolution (time_chunk_for_iso_step):
    ~a week of sub-daily steps, ~a month of daily steps, ~a year of weekly+
  - spatial chunks capped at 512 pixels per side (its pyramid tile size),
    so a map client never fetches a huge chunk to fill a small screen tile
```

**Why it matters.** Chunk size is set once, at write time, and inherited by every
read for the life of the store. A store written with 4672 tiny chunks is slow
forever, for everyone, and the only remedy is rewriting it.

**Traps.**

- The example's "sensible" chunks are 12 MB, not 100 MB, because the whole array
  is only 191 MB. The rule is a target for large arrays, not a floor -- if the
  array is 191 MB, chunks of 100 MB would leave you with two of them.
- Chunk size is `prod(chunk_shape) * itemsize`. Changing dtype from float64 to
  float32 halves it; a store of int16 with the same shape is a quarter.
- A worker needs room for several chunks at once, roughly
  `memory_limit / threads_per_worker` divided among concurrent tasks. 100 MB
  chunks on a 2 GB worker with 4 threads is already tight.
- Compressed chunks on disk are smaller than in memory, sometimes by 5x. Size the
  chunk by its in-memory footprint.
- The task count that matters is post-optimization, and it scales with chunks
  times pipeline depth, not with chunks alone.

### 0603 — Graph hygiene

Source: [`../../dask/examples/0603_graph_hygiene.py`](../../dask/examples/0603_graph_hygiene.py)

**What it teaches.** Three habits, each measured. This is the example to re-read
before optimizing anything.

```python
arr = random_field(seed=0)
heavy = da.sin(arr) * da.cos(arr) + da.sqrt(arr)
da.zeros(1).sum().compute()   # warm the thread pool so timings compare graphs, not startup
```

**Habit 1: slice before computing.**

```python
lazy_sliced = heavy[:10].mean()
t_good = timed(lambda: lazy_sliced.compute())
t_bad = timed(lambda: heavy.compute()[:10].mean())
```

```
Habit 1 -- slice BEFORE computing. Goal: mean of the first 10 days of heavy.
  task_count(heavy[:10].mean()) =  321  (only chunks touching days 0-9)
  task_count(heavy.mean())      =  378  (every chunk)
  compute sliced graph:            0.01s
  compute FULL array, then slice:  0.11s  (8.5x, and ~191 MB materialized)
  Slicing a lazy array prunes the graph; slicing a computed result prunes nothing.
```

Same answer, 8.5x apart, and the slow version also materialized 191 MB it did not
need. The raw task counts barely differ (321 versus 378) because culling has not
happened yet -- this is a good reminder that raw `task_count` is a graph-size
metric, not a cost metric.

**Habit 2: one grouped compute, not a loop of computes.**

```python
def monthly_loop() -> None:
    loop_results.clear()
    for month in range(1, 13):
        sel = field.sel(time=slice(f"2023-{month:02d}", f"2023-{month:02d}"))
        loop_results.append(sel.mean("time").compute().values)

grouped = field.groupby("time.month").mean("time")
```

```
Habit 2 -- one grouped compute beats a Python loop of computes.
Goal: 12 monthly mean maps from a year of daily data.
  loop of 12 .compute() calls: 0.10s  (12 graph builds; boundary chunks regenerated)
  one groupby('time.month'):   0.07s  (one graph; each chunk generated once)
  ratio: 1.5x -- results identical: True
```

Only 1.5x here, and the example is honest about why: with 30-day chunks and
monthly selections, the overlap between iterations is small, so the loop wastes
relatively little. The gap widens sharply when iterations overlap more, when
there are more of them, and when each `.compute()` round-trips to a distributed
scheduler. The structural point stands regardless: twelve graph builds, twelve
scheduler submissions, twelve chances to regenerate a shared boundary chunk.

**Habit 3: share or persist intermediates.**

```python
norm = (arr - arr.mean()) / arr.std()
r_min, r_max = norm.min(), norm.max()
t_twice = timed(lambda: (r_min.compute(), r_max.compute()))
t_shared = timed(lambda: dask_compute(r_min, r_max))
kept = norm.persist()
t_persist_use = timed(lambda: (kept.min().compute(), kept.max().compute()))
```

```
Habit 3 -- share or persist intermediates. Goal: min and max of a
normalized field norm = (arr - mean) / std, an expensive shared subgraph.
  two separate .compute() calls:    0.13s  (norm computed TWICE)
  one dask.compute(r_min, r_max):   0.07s  (norm computed once)
  persist norm, then both computes: 0.08s + 0.02s per extra pass
  Persist trades memory (~191 MB held) for cheap reuse across many later computes;
  a single dask.compute(...) is the lighter fix when the results are needed together.
```

Three tiers, cleanly separated. Separate computes: 0.13 s, the shared subgraph
run twice. One `dask.compute`: 0.07 s, run once, no memory held. Persist: 0.08 s
up front, then 0.02 s per subsequent pass, at the cost of 191 MB pinned.

The decision follows from how many passes you expect. Two results needed
together: `dask.compute`. Many results needed over time: `persist`. One result:
neither.

**The checklist**, quoted as printed:

```
Graph hygiene checklist:
  1. Subset lazily (slice/sel) before compute -- prune the graph, not the result
  2. Never call .compute() in a loop -- batch with groupby/resample or dask.compute(*many)
  3. Compute related results together so shared subgraphs run once
  4. Persist an intermediate only when it is reused across several later computes
  5. Keep pipelines as chained lazy ops -- fuse-friendly and schedulable as one graph
```

**Why it matters.** OCS runs openEO process graphs, where the graph *is* the
program. Work you never put in the graph is never scheduled; a loop of computes
rebuilds and re-runs overlapping graphs; an unshared intermediate is silently
computed twice. These five lines are the review checklist for any dask pipeline.

**Traps.**

- The warm-up call (`da.zeros(1).sum().compute()`) matters when timing. Without
  it the first measurement absorbs thread-pool startup and the comparison is
  meaningless.
- "Never compute in a loop" has an exception: when results are genuinely needed
  one at a time and the whole batch would not fit in memory. Even then,
  `dask.compute` in batches beats one at a time.
- Persisting inside a loop is the worst of both worlds -- memory grows and nothing
  is shared.
- Habit 1's benefit depends on the slice landing inside few chunks. A slice that
  takes one pixel from every chunk prunes nothing.

---
## Chunking: the deep section

If you take one thing from this page, take this section. Chunk layout is the
single most consequential decision in a dask array pipeline, it is made early,
it is inherited by everything downstream, and it is expensive to change. A store
written with the wrong chunks is slow for every consumer, forever, and the only
real fix is rewriting it.

The upstream reference is
<https://docs.dask.org/en/stable/array-chunks.html>. What follows is that
material with this project's measurements attached, plus the two constraints that
page does not cover: zarr's legality rule and access-pattern-driven layout.

### What a chunk actually costs

Three costs scale with chunk *count*, and three scale with chunk *size*. Getting
the layout right means balancing them.

Costs that grow as chunks get smaller (more of them):

- **Scheduler overhead.** Roughly 1 ms per task on the distributed scheduler,
  a few hundred microseconds locally. Multiply by chunks times pipeline depth.
- **Graph size.** The graph itself is a Python data structure that must be built,
  optimized, serialized, and sent. A 100,000-task graph is seconds of overhead
  before anything runs, and hundreds of megabytes on the scheduler.
- **I/O request count.** One chunk is one object in a zarr store. On object
  storage, per-request latency (tens of milliseconds) dominates unless the
  object is large enough to amortize it.

Costs that grow as chunks get larger (fewer of them):

- **Memory per task.** A worker must hold the input chunk, the output chunk, and
  any intermediates simultaneously. Several tasks run concurrently per worker.
- **Lost parallelism.** You cannot use more cores than you have chunks. One chunk
  means one core.
- **Read amplification.** The chunk is the unit of I/O. Wanting one day out of a
  365-day chunk means reading all 365 days.

### Why ~100 MB

The ~100 MB rule of thumb falls out of putting numbers on both sides.

From below: a chunk should do enough work that scheduling it is noise. If
scheduling costs 1 ms and you want overhead under 1%, the task must do at least
100 ms of work. Numpy processes on the order of 1 GB/s per core for simple
elementwise work, so 100 ms of work is roughly 100 MB of data. That is the
derivation, and it is why the number moves with your workload: heavy per-element
work (a transcendental, a fit, a Python-level callback) justifies smaller chunks;
trivial work justifies larger ones.

From above: a worker with a memory limit `L` and `T` threads runs up to `T` tasks
at once, each needing room for input, output, and scratch -- call it 3 chunks per
task. So chunk size should be well under `L / (3 * T)`. A 4 GB worker with 4
threads wants chunks under about 300 MB, and comfortably under, because dask's
own bookkeeping and any unmanaged memory count too.

100 MB sits in the middle of that window for typical hardware. The workable range
is roughly 10 MB to 500 MB; below 1 MB you are paying pure overhead, above 1 GB
you are asking for worker deaths.

[0602](#0602-chunk-sizing) measures the U-shape on a small array:

| Layout | Chunk size | Chunks | Tasks | Wall | vs sensible |
|---|---|---|---|---|---|
| `(5, 32, 32)` | 41 KB | 4672 | 19377 | 1.27 s | 33.2x |
| `(90, 128, 128)` | 11.8 MB | 20 | 86 | 0.04 s | 1.0x |
| `(365, 256, 256)` | 191.4 MB | 1 | 5 | 0.17 s | 4.5x |

Note that "sensible" here is 11.8 MB, not 100 MB, because the whole array is only
191 MB. **The rule is a target for large arrays, not a floor.** You want enough
chunks to fill your cores with a little slack for load balancing -- something like
2-4 chunks per available thread -- and then as large as that allows within the
memory budget.

### The sizing recipe

For a `(time, y, x)` climate store, work through it in this order.

1. **Pick the spatial tile first, from the consumer.** If clients fetch map tiles,
   the tile size is the natural spatial chunk. OCS uses 512 as a cap, matching its
   pyramid tile size, so a map client fetching one screen tile fetches at most one
   chunk. If the consumer is analysis rather than display, use the largest tile
   that keeps chunks in budget.
2. **Derive the time chunk from what is left.** With `float32` and 512x512
   spatial, one time step is `512 * 512 * 4 = 1.05 MB`. A 100 MB budget gives
   about 95 time steps -- so roughly three months of daily data, or a week of
   hourly data. This is the same reasoning as OCS's `time_chunk_for_iso_step`:
   about a week of sub-daily steps, about a month of daily steps, about a year of
   weekly-or-coarser steps.
3. **Round to something that divides the data reasonably.** A time chunk of 30
   over 365 days gives `(30,) * 12 + (5,)` -- fine, and legal for zarr. A time
   chunk of 100 gives `(100, 100, 100, 65)` -- also fine. Avoid layouts whose
   remainder is a tiny sliver.
4. **Check the resulting chunk count.** Chunks times pipeline depth is your task
   count. If that lands above roughly 100,000 for a single request, revisit.
5. **Verify against the real query mix**, using optimized task counts -- see the
   access-pattern table below.

Two adjustments worth making. If your dtype is `float64`, halve the time chunk or
accept 2x the chunk size. And remember that chunks are compressed on disk: a
100 MB in-memory chunk may be 20 MB on disk, so disk sizes are not a reliable
guide to the memory footprint.

### Chunks and partitions are the same idea

`dask.array` says "chunk"; `dask.dataframe` says "partition". Both mean "one
piece, held entirely in memory by one task, of the underlying in-memory library's
native type".

| | dask.array | dask.dataframe |
|---|---|---|
| Piece is a | numpy array | pandas DataFrame |
| Layout attribute | `.chunks` (tuple per axis) | `.divisions` (index boundaries) |
| Count | `.numblocks`, `.npartitions` | `.npartitions` |
| Indexer | `.blocks[i, j, k]` | `.partitions[i]` |
| Per-piece escape hatch | `map_blocks` | `map_partitions` |
| Change the layout | `rechunk` | `repartition`, `set_index` |
| Target size | ~100 MB | ~100 MB |
| Dimensionality | pieces along every axis | pieces along the index only |

The differences that matter: an array is chunked along *every* axis, so the
layout is a grid and you can trade one axis against another; a dataframe is split
along the index only, so the only knobs are how many pieces and where the
boundaries fall. And a dataframe's boundaries carry semantic weight -- divisions
enable index-based pruning ([0401](#0401-partitions)) -- while array chunk
boundaries are purely mechanical.

### Rechunking cost, precisely

[0204](#0204-rechunking) separates three regimes, and the distinction is worth
holding onto because the word "rechunk" hides an enormous cost range.

| Regime | Example | Added tasks (52-chunk array) | Data movement |
|---|---|---|---|
| Identical layout | `(30,128,128)` to `(30,128,128)` | 0, returns the same object | none |
| Aligned merge or split | `(30,128,128)` to `(60,128,128)` | +28, one concat per output chunk | local |
| Crossing boundaries | `(30,128,128)` to `(365,32,32)` | +132, split then concat | all-to-all |

The all-to-all case is the one that kills clusters. Every output chunk needs a
piece of many input chunks, so on a distributed cluster the data crosses the
network between every pair of workers, and peak memory approaches holding both
layouts at once. If a rechunk is unavoidable at scale, do it as its own step,
write the result, and start a fresh computation from the new store.

The cheapest rechunk is the one you do not do:

```
Choosing the layout at creation skips the shuffle entirely:
  created as (365, 32, 32) directly:   tasks=64
  created balanced, then rechunked:    tasks=184
```

Three times the tasks, plus the shuffle, for the identical result.

### Zarr's uniformity rule versus dask's flexibility

This is the constraint that surprises people, because it appears only at the very
end of a pipeline.

**dask** stores a chunk *tuple* per axis. Any tuple of positive integers summing
to the axis length is legal: `(30, 30, 5)`, `(5, 30, 30)`, `(1, 47, 3, 200)`.
dask genuinely does not care.

**zarr** stores a single chunk *shape* per array in its metadata. Every chunk
along a dimension is that size, except the last, which may be smaller. There is
no representation for anything else.

So `(30, 30, 5)` maps onto a zarr chunk shape of 30. `(5, 30, 30)` maps onto
nothing at all, and `to_zarr` refuses. This is the real error, caught and printed
by [0601](#0601-zarr-legal-chunks):

```
  ValueError: Zarr requires uniform chunk sizes except for final chunk. Variable named 't2m' has incompatible dask chunks: ((5, 30, 30), (64,), (64,)). Consider rechunking using `chunk()`.
```

The ways an ordinary pipeline produces an illegal layout:

- **Reversing an axis.** `isel(time=slice(None, None, -1))` reverses the chunk
  tuple. `(30, 30, 5)` becomes `(5, 30, 30)`. This is what OCS's
  `ensure_north_up` does to south-up sources, on `y` rather than `time`, and it
  is the exact bug `_uniform_chunks` exists to fix.
- **Slicing that starts mid-chunk.** [0504](#0504-lazy-zarr-pipeline) produces
  `(29, 30, 30, 3)` from a `sel` on a date range -- the leading 29 is illegal.
- **Concatenating unevenly sized pieces.** Appending a 17-day month to 30-day
  chunks leaves a ragged interior.
- **groupby / resample arithmetic**, which can fragment an axis into one chunk per
  label.

The fix is the OCS helper, re-implemented in the example:

```python
def uniform_chunks(ds: xr.Dataset) -> xr.Dataset:
    targets: dict[Hashable, int] = {}
    for var in ds.data_vars.values():
        for dim, chunks in zip(var.dims, var.chunks or (), strict=False):
            if len(set(chunks[:-1])) <= 1 and chunks[-1] <= chunks[0]:
                continue  # already legal: uniform except a smaller final chunk
            targets[dim] = max(targets.get(dim, 0), max(chunks))
    if not targets:
        return ds
    return ds.chunk(targets)
```

It is worth restating why this is the right shape for the fix rather than a
blanket `ds.chunk({...})`:

- It **detects** rather than assumes, so legal dimensions are left completely
  alone and pay nothing.
- It **rechunks to `max(chunks)`**, which restores the pre-reversal layout,
  because dask fills chunks uniformly from the start and leaves the remainder
  last. Rechunking `(5, 30, 30)` to 30 gives back `(30, 30, 5)`.
- It **returns the input unchanged** when nothing is wrong, and a same-layout
  `rechunk` is free anyway, so calling it before every write costs nothing.

There is a closely related failure in the same family, documented on the
[open-climate-service page](../open-climate-service.md): appending
variable-length months to a store chunked at 30 days fails with
`ValueError: Specified Zarr chunks encoding['chunks']=(30, 32, 32) for variable
named 't2m' would overlap multiple Dask chunks`. Different message, same root
cause -- dask and zarr disagree about legal layouts, and the disagreement surfaces
at write time. The fix there is `align_chunks=True` on the append.

**The habit to adopt: validate the chunk layout before you compute, not after.**
The write is the last step of the longest pipeline, and finding out there that
the layout is illegal means paying for all of it again.

### Access-pattern-driven layout

Chunk *size* is a resource-budget question with a defensible default. Chunk
*shape* is a workload question with no default at all, because the same total
chunk size can be distributed across axes in wildly different ways, and the right
distribution depends entirely on how the data will be read.

[0204](#0204-rechunking) measures this with optimized task counts, which count
the chunks actually touched:

```
  layout                   time series arr[:, 7, 7]   day map arr[100]
  balanced (30,128,128)                    26 tasks            8 tasks
  time-opt (365,32,32)                      2 tasks          128 tasks
  map-opt  (1,256,256)                    730 tasks            2 tasks
```

Two canonical climate queries, three layouts, and a 365x spread.

**Time-series read** (`arr[:, y, x]`: one pixel, all time). Wants the whole time
axis in one chunk and small spatial tiles. The time-optimized layout answers in 2
tasks. The map-optimized layout answers in 730, because every one of 365 daily
chunks must be opened and a single value extracted from each -- and on object
storage those are 365 separate HTTP requests to fetch 365 float64 values.

**Map read** (`arr[t]`: one time step, all space). Wants the whole spatial extent
in few chunks and a small time chunk. The map-optimized layout answers in 2
tasks; the time-optimized layout needs 128, because every spatial tile must be
opened and one time slice extracted.

**The balanced layout is worse than each specialist at its own query and vastly
better than each at the other's.** 26 versus 2 for time series (13x worse than
the specialist), 8 versus 2 for maps (4x worse). But the specialists are 64x and
365x worse at the query they were not designed for.

That asymmetry is the whole argument. If you know the access pattern with
certainty and it will not change, specialize. If you serve mixed queries -- which
any general-purpose store does -- take the balanced layout, because the penalty
for guessing wrong with a specialist is an order of magnitude larger than the
penalty for the generalist being merely good.

A concrete way to make the decision: write down your two or three most common
queries as slices, and evaluate

```python
def optimized_task_count(obj: Any) -> int:
    (optimized,) = optimize(obj)
    return task_count(optimized)
```

for each candidate layout, exactly as [0204](#0204-rechunking) does. It takes
five minutes and it is the only way to be sure, since the interaction between
slice boundaries and chunk boundaries is not something anyone reliably predicts
in their head.

### Diagnosing an existing store

Given a store you did not write, four checks tell you nearly everything:

```python
ds = xr.open_zarr(store, consolidated=False)
print(ds.chunks)                              # per-dimension chunk sizes
print(chunk_report(ds["t2m"].data))           # shape, first chunk, count, MB
print(task_count(ds["t2m"]))                  # tasks just to read it
print(optimized_task_count(ds["t2m"][:, 7, 7]))  # cost of your real query
```

The red flags, in order of how often they occur:

- **Chunk size under 1 MB.** Almost always a mistake; often thousands of times
  more objects than necessary.
- **Chunk count in the tens of thousands** for a single variable.
- **A time chunk of 1** on a store that anyone queries as a time series.
- **Chunk size over 1 GB**, which will kill workers.
- **A ragged chunk tuple** that is not simply "uniform with a smaller last" --
  a sign that something has been sliced, reversed, or concatenated on the way in.

---

## Pitfalls and gotchas

Consolidated from all six phases, in rough order of how much damage they do.

### Too many tiny chunks

The most common and most expensive mistake. Every chunk is at least one task per
operation, and every task costs fixed scheduler overhead regardless of how little
work it does.

The evidence, from [0602](#0602-chunk-sizing): 4672 chunks of 41 KB produced
19,377 tasks and ran 33.2x slower than 20 chunks of 11.8 MB, computing exactly
the same thing. And from [0201](#0201-chunked-arrays): a `(1, 32, 32)` layout on
a 191 MB array is 23,360 chunks before you have done anything at all.

Symptoms: the dashboard task stream shows thousands of tiny bars with gaps; CPU
usage is low while the scheduler process is pinned; the computation takes longer
than the same thing in numpy.

Fix: rechunk to something in the 10-500 MB range, or, better, write the store
with sensible chunks in the first place. If the store is the problem, the tiny
chunks are baked in and reading is expensive no matter what you do afterwards.

### Rebuilding graphs in loops

```python
# Anti-pattern
for month in range(1, 13):
    sel = field.sel(time=slice(f"2023-{month:02d}", f"2023-{month:02d}"))
    results.append(sel.mean("time").compute())

# Better
grouped = field.groupby("time.month").mean("time").compute()
```

Twelve graph builds, twelve scheduler submissions, twelve chances to regenerate
a chunk that spans a month boundary, and zero sharing between iterations.
[0603](#0603-graph-hygiene) measures 1.5x on a favourable case; the gap grows
with iteration count, with overlap between iterations, and with the cost of
round-tripping to a distributed scheduler.

If a loop is genuinely unavoidable -- results needed one at a time, or the full
batch would not fit -- build all the lazy objects in the loop and compute them
together afterwards:

```python
lazies = [field.sel(time=...).mean("time") for month in range(1, 13)]
results = dask.compute(*lazies)
```

### Forgetting to persist shared intermediates

[0103](#0103-sharing-intermediates) shows the mechanism with a call counter:
separate `.compute()` calls ran the shared step twice; one `dask.compute(x, y)`
ran it once. [0603](#0603-graph-hygiene) puts wall times on the same choice:
0.13 s for two separate computes, 0.07 s for one shared compute, and 0.02 s per
pass off a persisted intermediate.

The decision tree:

- Results needed **together**: `dask.compute(a, b, c)`. No memory cost.
- Results needed **repeatedly over time**, and the intermediate fits:
  `.persist()`. Pay in RAM once.
- Result needed **once**: neither. Just compute it.

And the caveat from [0103](#0103-sharing-intermediates) that makes sharing
possible at all: sharing is by graph key. Two calls that look identical in source
produce different keys for `delayed` and for anything involving randomness or
file handles. **Build the intermediate once and pass the variable around.**

### Persisting too much

The mirror-image mistake, and the usual cause of dead workers. `.persist()` pins
data in memory until every reference is dropped. Persisting a full datacube "just
in case" at the top of a pipeline turns a streaming computation into an
out-of-memory error.

[0503](#0503-compute-load-persist) quantifies the trade: 191 MB pinned bought a
6x repeat speedup. That is a good trade for a reused intermediate and a terrible
one for something used once.

Also remember `.compute()` returns a *new* object and leaves the original lazy.
`ds.compute()` followed by continued use of `ds` computes twice and discards the
result of the first.

### Scattering small data (and its opposite)

On a distributed cluster, arguments to `client.submit` are serialized per call. A
100 MB lookup table sent to 500 tasks is 50 GB across the wire. `client.scatter`
publishes it once and hands back a reference.

The opposite error is scattering things that are small. Scatter has its own
round-trip cost and its own bookkeeping, and scattering a 10 KB config object is
pure loss. The threshold is roughly "bigger than a megabyte, used by more than a
few tasks".

Related: `client.submit(fn, big_dask_array)` is almost always wrong -- it will
compute and serialize the array. Pass the lazy collection to `client.compute` or
use collections directly.

### Shuffles in dataframes

[0402](#0402-groupby-shuffle) lays out the cost ladder: per-partition ops are
cheap and linear, groupby aggregations tree-reduce and move only tiny partials,
and `set_index` / `sort_values` / unaligned joins move every row.

The design rule is **aggregate early, sort never**. If a sort or re-index is
genuinely required, do it once, before writing to disk, so every later query
inherits sorted partitions with known divisions.

Watch for shuffles hiding inside innocuous-looking calls:
`groupby(...).apply(fn)` and `groupby(...).transform(fn)` are shuffles because an
arbitrary function cannot be decomposed into partial-and-combine. Use built-in
aggregations, or `dd.Aggregation` with an explicit partial/combine pair.

### The small-data anti-pattern

[0403](#0403-pandas-boundary): dask was ~10x slower than pandas on a 3.2 MB
table, computing identical output. There is no configuration that fixes this.

If your data fits comfortably in memory, use numpy or pandas. If it fits but you
want the cores, measure before committing. Reach for dask when the data is larger
than RAM, starts on disk in a chunked format, or is genuinely distributed.

The same anti-pattern appears inside otherwise sensible pipelines, as staying in
dask too long. Once the heavy reduction has run and the result is 300 rows,
`.compute()` and finish in pandas.

### Silent wrongness at chunk boundaries

The nastiest bug class in the array world, because nothing raises.
[0205](#0205-map_blocks-and-map_overlap) produces 1512 wrong cells out of 24576
-- 6% of the array, all of them on interior chunk edges -- by applying a 3x3 mean
filter with `map_blocks` instead of `map_overlap`. Right shape, right dtype,
plausible values, faint grid artefacts in any plot.

Any operation with a spatial or temporal stencil needs a halo at least as deep as
the kernel radius. If a function passed to `map_blocks` contains a reduction, a
rolling window, a gradient, or a convolution, stop and check whether it should be
`map_overlap`.

### Accidental computation

Laziness ends the moment something needs a concrete value, and the triggers are
easy to miss:

- `.values`, `float()`, `int()`, `bool()`, `np.asarray()`
- `len()` on an axis of unknown length, `.shape[0]` on a dataframe
- `if x > 3:` and any other boolean context
- printing or repr-ing a dataframe
- passing the object to matplotlib, or to any library that calls `np.asarray`
- `list(x)`, `sorted(x)`, iterating

In a notebook, evaluating a cell that ends in a dask-backed object is often a
compute. The habit that catches all of it: check `type(obj.data).__name__` --
`Array` means dask and lazy, `ndarray` means numpy and already paid for.

### Impure tasks

dask may run a task more than once, on a different thread, in a different
process, or on a different machine. After a worker loss on a cluster, it
certainly will. Tasks that mutate shared state, depend on execution order, write
to a fixed filename, or rely on a thread-local are bugs waiting for the day the
cluster reschedules them.

The corollary for I/O: a graph carries one path string, used by client and
workers alike. A path that resolves on your laptop and not on a worker container
fails at execution time with a confusing error. See
[dask-distributed](dask-distributed.md) `0302_shared_storage`.

### Reading task counts as costs

`task_count(obj)` counts the *unoptimized* graph. Culling and fusion routinely
remove half of it, sometimes 85% ([0202](#0202-blocked-algorithms): 61 raw, 9
executed). If the number is meant to predict work, run `optimize` first. And even
then, task count is a proxy: one task that reads a 100 MB chunk from S3 outweighs
fifty that add scalars.

---

## How this maps to open-climate-service

[OCS](https://github.com/dhis2/open-climate-service) is a per-country climate
data platform: it ingests from sources like CHIRPS and ERA5, normalizes
everything to `(time, y, x)`, stores results as GeoZarr inside icechunk, and
exposes them through STAC, Zarr over HTTP, and openEO. Every phase of this
project exists because some part of that stack sits on dask.

### openEO process graphs execute on dask

An openEO request arrives as a JSON *process graph*: nodes with dependencies,
describing load, mask, scale, aggregate, and output steps. OCS translates that
into dask work through `openeo-processes-dask`, and the scheduler executes it.

The correspondence is close enough to be useful rather than merely poetic. Both
are DAGs of pure operations. Both are built completely before anything runs. Both
are optimized before execution -- dask culls and fuses, and the process-graph
layer can push slicing down to the load node. And in both, the graph *is* the
program, which is why the [graph hygiene](#0603-graph-hygiene) checklist applies
directly to OCS request handling: work never put in the graph is never scheduled.

Practically, phases 1 through 3 of this project are the parts you need to reason
about an OCS request. [0101](#0101-delayed-basics) and
[0102](#0102-task-graphs) give you the object; [0202](#0202-blocked-algorithms)
and [0603](#0603-graph-hygiene) give you the reason a bounded request is cheap;
[0301](#0301-schedulers) and [0302](#0302-local-cluster) give you the executor.

The full request path is [0504](#0504-lazy-zarr-pipeline): open a normalized
`(time, y, x)` store lazily, slice to the requested window, derive the product,
write or stream the result. The measurement that matters there is 460 tasks and
0.04 s for a window versus 6179 tasks and 0.49 s for the full domain -- from the
same code, differing only in that the slice came before the derivation.

### The `_uniform_chunks` fix

OCS's downloader appends periods along time and then calls `ensure_north_up`,
which flips south-up sources with `isel(y=slice(None, None, -1))`. The flip
reverses the chunk tuple, so a legal trailing remainder becomes an illegal
leading one, and `to_zarr` refuses at the end of the ingest.

[0601](#0601-zarr-legal-chunks) reproduces that exactly -- on `time` rather than
`y`, since the mechanics are identical and time is easier to narrate -- catches
the real `ValueError`, and re-implements the `_uniform_chunks` helper that fixes
it: detect dims whose chunks are non-uniform except for the final chunk, rechunk
only those to the largest existing chunk, and return the dataset untouched when
nothing is wrong.

This is one of two deliberate re-implementations of OCS code in this repository
(the other is icechunk's open-or-create plus commit-and-append pattern), and it
is kept close to the original on purpose.

The same family of failure shows up on appends, where the message is
`ValueError: Specified Zarr chunks encoding['chunks']=(30, 32, 32) for variable
named 't2m' would overlap multiple Dask chunks` and the fix is
`align_chunks=True`. Both the `climate-pipeline` and `icechunk` projects hit it
independently, on the fifth month in each case.

### Chunk sizing from data resolution, with a 512 spatial cap

OCS does not hard-code a chunk shape. It derives one from the data:

- **Time chunks follow the temporal resolution.** Its
  `time_chunk_for_iso_step` maps an ISO duration to a chunk length: roughly a
  week of sub-daily steps, a month of daily steps, a year of weekly-or-coarser
  steps. The intent is that a chunk holds a comparable *amount* of data
  regardless of how finely time is sampled.
- **Spatial chunks are capped at 512 pixels per side**, matching the pyramid tile
  size. A map client fetching one screen tile fetches at most one chunk, and
  never pulls a huge chunk to fill a small tile.

Read against [0204](#0204-rechunking)'s access-pattern table, this is precisely
the balanced row: neither time-specialized nor map-specialized, deliberately
mediocre at both queries rather than excellent at one and catastrophic at the
other. Given that OCS serves map tiles and time series from the same store, that
is the correct choice, and [0602](#0602-chunk-sizing) is the measurement that
justifies it.

### The rest of the surface

Two more connections are worth naming. OCS builds multiscale pyramids by mean
downsampling, which is `coarsen(y=2, x=2).mean()` --
[0502](#0502-graphs-through-xarray) shows it compiling to 208 dask tasks and
explains why factors of 2 nested inside the chunk grid are the cheap case. And
OCS uses `dask-geopandas` for vector aggregation, which is partitioned
`dask.dataframe` underneath, which is why [phase 4](#phase-4-daskdataframe)
exists at all in an otherwise array-shaped course.

---

## Where to go next

- **[dask-distributed](dask-distributed.md)** -- the same library as a system. A
  scheduler and three worker containers under Docker Compose, where
  serialization, locality, worker memory, failure, and observability all become
  visible. Everything in phase 3 of this project is the single-machine
  rehearsal for it.
- **[xarray](xarray.md)** -- labels and dimensions, from the data model through
  to the dask-backed lazy evaluation that phase 5 here assumes. If phase 5 felt
  like it skipped steps, that project has the missing 25 examples.
- **[Scaling: the ceilings](../scaling.md)** -- which limit you are actually
  hitting, and why adding machines stops helping. Throughput tracks total
  threads, not worker count, and a batch can never finish faster than its longest
  single task.
- **[API reference](../reference/dask.md)** -- generated documentation for
  `climate_stack_dask.helpers`: `random_field`, `chunk_report`, `task_count`.

---

## Further reading

Upstream, in the order they are worth reading:

- [10 Minutes to Dask](https://docs.dask.org/en/stable/10-minutes-to-dask.html) --
  the fastest correct orientation to the collections.
- [Dask Best Practices](https://docs.dask.org/en/stable/best-practices.html) --
  short, dense, and the source of most of the
  [pitfalls](#pitfalls-and-gotchas) above.
- [Chunks](https://docs.dask.org/en/stable/array-chunks.html) -- the definitive
  treatment of chunk selection, and the reference for
  [the deep section](#chunking-the-deep-section).
- [Dask documentation root](https://docs.dask.org/) -- start here for anything
  not covered above; the Deploy and Diagnostics trees are the parts you return
  to.
- [Array Best Practices](https://docs.dask.org/en/stable/array-best-practices.html)
  -- array-specific advice, including when not to use dask.array at all.
- [DataFrame Best Practices](https://docs.dask.org/en/stable/dataframe-best-practices.html)
  -- partition sizing, the shuffle classes, and the parquet advice from
  [0403](#0403-pandas-boundary).
- [Understanding Performance](https://docs.dask.org/en/stable/understanding-performance.html)
  -- the diagnostic decision tree behind [0304](#0304-diagnostics).
- [Custom Graphs](https://docs.dask.org/en/stable/graphs.html) -- the task-graph
  specification itself, for when you want to build or inspect one by hand.
- [Distributed documentation](https://distributed.dask.org/) -- clusters,
  futures, the dashboard, and the scheduler's internals.
- [Dask and xarray](https://docs.xarray.dev/en/stable/user-guide/dask.html) --
  xarray's own guide to the arrangement phase 5 covers.
- [Zarr chunk specification](https://zarr-specs.readthedocs.io/) -- the source of
  the uniformity rule that [0601](#0601-zarr-legal-chunks) runs into.
- [openEO processes](https://openeo.org/documentation/1.0/processes.html) and
  [openeo-processes-dask](https://github.com/Open-EO/openeo-processes-dask) --
  the process-graph layer OCS executes on top of dask.

