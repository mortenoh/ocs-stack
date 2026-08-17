# The stack, layer by layer

Four of the projects are one stack, each layer depending on the one below. The
capstone sits on top of all of them.

```text
climate               a service: normalize -> ingest -> derive -> publish
    |
icechunk              storage: transactions, snapshots, branches, resume
    |
dask-distributed      execution at scale: separate machines, real failure
    |
dask                  execution: task graphs, chunks, schedulers
    |
xarray                the data model: dims, coords, lazy arrays
```

## xarray — naming the axes

A climate dataset is a big rectangular block of numbers with three meanings
attached: when, and where in two directions. In numpy that block is
`values[t, i, j]` and the meaning lives in your head or in a comment. xarray
attaches it to the array: dimensions get names, positions get labels, and
metadata rides along in `attrs`.

The payoff is that operations address dimensions by name. `ds.mean(dim="time")`
is a spatial map and `ds.sel(time="2024-02")` is February — neither requires
knowing which axis is which. Once a dataset has been through three or four
transformations, this is the difference between code you can read and code you
have to trace.

The trap to internalise early is that **attributes are inert**. xarray never
interprets `units`. Adding a Celsius array to a Kelvin array produces a
nonsense number with no warning, which is exactly why a service must
standardise units at ingest rather than trusting the labels
(`xarray/examples/0701_cf_attrs_units.py`).

**Where it stops:** xarray by itself is numpy underneath. One array must fit in
memory.

## dask — computing on more data than fits

dask replaces "the array" with "a plan for computing the array in pieces". A
dask array is a grid of chunks plus a task graph; operations extend the graph
and nothing executes until `.compute()`.

Two consequences that take a while to feel natural:

**Building is free, executing is not.** Chaining a dozen operations on a
100 GB dataset takes milliseconds and touches no data. The wall-clock cost
arrives all at once at the end (`xarray/examples/0602_lazy_graphs.py` shows the
graph growing from 52 to 460 tasks while elapsed time stays near zero).

**Chunk size is the single most consequential tuning decision.** Too small and
the scheduler spends more time dispatching tasks than computing them; too large
and workers exhaust memory and spill to disk. Roughly 100 MB per chunk, with
enough chunks to keep every core busy, is the rule of thumb;
`dask/examples/0602_chunk_sizing.py` measures all three regimes rather than
asserting it.

There is also a sharp edge where dask meets zarr: **zarr requires every chunk
along a dimension to be equal-sized except the last, and dask does not**.
Ordinary operations — concatenating unevenly sized pieces, reversing an axis —
produce layouts zarr refuses to write. OCS carries a `_uniform_chunks` function
for exactly this; `dask/examples/0601_zarr_legal_chunks.py` reproduces the real
error and re-implements the fix.

**Where it stops:** one machine's cores and memory bandwidth.

## dask distributed — when one machine is not enough

The distributed scheduler runs the same graphs across processes and machines.
The API barely changes; the operational reality changes completely, because a
local scheduler quietly hides four things that now matter:

**Everything you send is serialized.** Passing a 50 MB array to a task means
pickling it and pushing it through a socket. Passing the same array to twelve
tasks does that twelve times. `client.scatter` publishes it once instead —
worth about 4x in `dask-distributed/examples/0202_scatter_gather.py` — and
shipping an array at all costs several times more than letting the worker
generate it from a seed, in
`dask-distributed/examples/0201_serialization.py`. (Both measure it live, so
the exact multiplier depends on your machine and moves between runs; the
direction does not.) The lesson generalises: send parameters, not payloads.

**Workers do not share your filesystem.** A path that works on your laptop
points at nothing, or at the wrong thing, inside a worker container
(`dask-distributed/examples/0302_shared_storage.py` shows a just-written file
reporting `False` on all three workers). This has direct consequences for
storage, taken up in [Storage](storage.md).

**Workers die, and that is normal.** They get OOM-killed, preempted,
redeployed. dask treats the graph as the source of truth and recomputes what
was lost; killing a worker mid-computation still yields the right answer
(`dask-distributed/examples/0401_worker_failure.py`). The exception is
scattered data, which has no recipe and simply disappears with the worker
holding it.

**Client and workers must run the same libraries.** Version drift between a
client and a worker produces failures ranging from a warning to silently
different numerics. The Dockerfile in that project pins to `uv.lock` because
the stock image was one patch behind on numpy and warned on every connect.

**Where it stops:** data movement between workers, and the slowest single task.
No amount of hardware makes a batch finish faster than its longest task —
`dask-distributed/examples/0503_task_stream.py` finds that floor in real
task-stream records.

## icechunk — making storage transactional

Plain zarr is a directory of chunk files. Writing means overwriting some of
them, which raises an awkward question for a service: what does a reader see
while a write is in progress? With plain zarr, the honest answer is "some
mixture of old and new chunks, and there is no way to tell".

icechunk fixes this by making a write a **transaction**. You open a session,
write as much as you like, and call `commit`. Until that moment no reader sees
anything; at that moment every reader sees all of it. History is a chain of
immutable snapshots, each addressable, so you can read the store exactly as it
was three commits ago. Branches and tags name points in that history.

Four properties matter in practice:

- **Atomicity.** A crashed write leaves the store untouched, not half-updated.
  `icechunk/examples/0301_atomicity.py` contrasts this with a plain zarr store
  whose per-day means read
  `[126.0, 126.0, 126.0, 125.9, 126.0, 26.0, 26.0, 26.0, 26.0, 25.9]` after an
  interrupted write — five days updated, five not, and nothing in the store
  recording which. The icechunk repository in the same example reports a max of
  29.4 degC: the orphaned chunks reached disk, but no snapshot references them,
  so no reader can observe them.
- **Snapshot isolation.** A reader that opened the store keeps seeing its
  snapshot no matter how many commits land meanwhile. This is what makes it
  safe to append to a dataset that is being served over HTTP right now.
- **Time travel.** Every past state stays readable by id or tag. A bad ingest
  is fixed forward with a new commit, and the bad snapshot remains available
  for audit (`icechunk/examples/0403_rewriting_history.py`).
- **Cheap history, with one caveat.** Snapshots share chunks *by reference*, so
  an append costs one period, not one copy of the dataset. But sharing is by
  reference and not by content hash: rewriting a store with byte-identical
  values costs a full copy, because the writer touched every chunk.
  `icechunk/examples/0501_storage_growth.py` measures both — six appends cost
  about 505 KB each, and history ends up at 0.91x the size of the tip.

**Where it stops:** icechunk does not make anything faster. It makes concurrent
access *correct*. Its ceiling is contention on a single branch tip, which is
the subject of [Storage](storage.md).
