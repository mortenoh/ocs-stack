# playground-python

Self-contained tutorial projects, one per library or subject, living in topic
groups as `<group>/<project>/`. Every project builds and runs on its own: its
own `pyproject.toml`, `.venv`, `uv.lock`, Makefile, examples, and mkdocs site.
There is no root package and no uv workspace.

The current set exists for one reason: to learn the stack behind
[open-climate-service](https://github.com/dhis2/open-climate-service) (OCS)
from the bottom up, so that extending it — S3-backed icechunk, a distributed
dask deployment — is a matter of applying things already understood rather
than learning them under pressure.

```bash
make list                        # every project
make verify PROJECT=data/xarray  # lint, type-check, test, docs, run every example
make verify-all                  # the whole repository
```

Inside a project: `make install`, `make lint`, `make test`, `make run-all`,
`make run EXAMPLE=<name>`, `make docs-serve`.

---

## The projects

| Project | Examples | What it covers |
|---|---:|---|
| [`lang/start`](lang/start/) | 1 | The template itself: uv, src layout, ruff, mypy, pyright, pytest, mkdocs |
| [`data/xarray`](data/xarray/) | 25 | Labeled N-dimensional arrays: the data model through to lazy evaluation |
| [`data/dask`](data/dask/) | 22 | Task graphs, blocked algorithms, schedulers, and chunking in practice |
| [`data/dask-distributed`](data/dask-distributed/) | 15 | A real cluster: scheduler and workers in containers, driven from the host |
| [`data/icechunk`](data/icechunk/) | 14 | Versioned, transactional Zarr v3 storage |
| [`domain/climate`](domain/climate/) | 10 | The capstone: a miniature climate service, end to end |

Each project's `ROADMAP.md` is the syllabus; each example is a self-contained
lesson that prints its own explanation as it runs. 87 examples in total, and
`make verify-all` runs every one of them.

### Where to start

Reading order depends on what you came for.

**To understand the stack from the ground up**, take the layers in order:
`data/xarray` → `data/dask` → `data/dask-distributed` → `data/icechunk`, then
`domain/climate` to see them combined. Within a project, the phases in
`ROADMAP.md` build on each other; the examples are numbered `PPNN` where `PP`
is the phase.

**To see the whole thing working first**, run
[`domain/climate/examples/0401_full_pipeline.py`](domain/climate/examples/0401_full_pipeline.py).
It takes about a second and prints six labelled stages from raw source to
published STAC collection. Then read backwards into whichever stage you want to
understand.

**To answer a specific question**, the sharpest examples are:

| Question | Example |
|---|---|
| What does "lazy" actually mean? | [`0602_lazy_graphs.py`](data/xarray/examples/0602_lazy_graphs.py) |
| How big should chunks be? | [`0602_chunk_sizing.py`](data/dask/examples/0602_chunk_sizing.py) |
| Why is my cluster not faster? | [`0503_task_stream.py`](data/dask-distributed/examples/0503_task_stream.py) |
| What breaks when I deploy to a cluster? | [`0302_shared_storage.py`](data/dask-distributed/examples/0302_shared_storage.py) |
| What does a commit protect me from? | [`0301_atomicity.py`](data/icechunk/examples/0301_atomicity.py) |
| What does history cost to keep? | [`0501_storage_growth.py`](data/icechunk/examples/0501_storage_growth.py) |

---

## The stack, layer by layer

The four `data/` projects are one stack, each layer depending on the one below.
The capstone sits on top of all of them.

```text
domain/climate        a service: normalize -> ingest -> derive -> publish
        |
data/icechunk         storage: transactions, snapshots, branches, resume
        |
data/dask-distributed execution at scale: separate machines, real failure
        |
data/dask             execution: task graphs, chunks, schedulers
        |
data/xarray           the data model: dims, coords, lazy arrays
```

### xarray — naming the axes

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
([`0701_cf_attrs_units.py`](data/xarray/examples/0701_cf_attrs_units.py)).

**Where it stops:** xarray by itself is numpy underneath. One array must fit in
memory.

### dask — computing on more data than fits

dask replaces "the array" with "a plan for computing the array in pieces". A
dask array is a grid of chunks plus a task graph; operations extend the graph
and nothing executes until `.compute()`.

Two consequences that take a while to feel natural:

**Building is free, executing is not.** Chaining a dozen operations on a
100 GB dataset takes milliseconds and touches no data. The wall-clock cost
arrives all at once at the end
([`0602_lazy_graphs.py`](data/xarray/examples/0602_lazy_graphs.py) shows the
graph growing from 52 to 460 tasks while elapsed time stays near zero).

**Chunk size is the single most consequential tuning decision.** Too small and
the scheduler spends more time dispatching tasks than computing them; too large
and workers exhaust memory and spill to disk. Roughly 100 MB per chunk, with
enough chunks to keep every core busy, is the rule of thumb;
[`0602_chunk_sizing.py`](data/dask/examples/0602_chunk_sizing.py) measures all
three regimes rather than asserting it.

There is also a sharp edge where dask meets zarr: **zarr requires every chunk
along a dimension to be equal-sized except the last, and dask does not**.
Ordinary operations — concatenating unevenly sized pieces, reversing an axis —
produce layouts zarr refuses to write. OCS carries a `_uniform_chunks` function
for exactly this;
[`0601_zarr_legal_chunks.py`](data/dask/examples/0601_zarr_legal_chunks.py)
reproduces the real error and re-implements the fix.

**Where it stops:** one machine's cores and memory bandwidth.

### dask distributed — when one machine is not enough

The distributed scheduler runs the same graphs across processes and machines.
The API barely changes; the operational reality changes completely, because a
local scheduler quietly hides four things that now matter:

**Everything you send is serialized.** Passing a 50 MB array to a task means
pickling it and pushing it through a socket. Passing the same array to twelve
tasks does that twelve times. `client.scatter` publishes it once instead —
worth about 4x in
[`0202_scatter_gather.py`](data/dask-distributed/examples/0202_scatter_gather.py) —
and shipping an array at all costs several times more than letting the worker
generate it from a seed, in
[`0201_serialization.py`](data/dask-distributed/examples/0201_serialization.py).
(Both examples measure it live, so the exact multiplier depends on your machine
and moves between runs; the direction does not.) The lesson generalises: send
parameters, not payloads.

**Workers do not share your filesystem.** A path that works on your laptop
points at nothing, or at the wrong thing, inside a worker container
([`0302_shared_storage.py`](data/dask-distributed/examples/0302_shared_storage.py)
shows a just-written file reporting `False` on all three workers). This one has
direct consequences for storage, taken up below.

**Workers die, and that is normal.** They get OOM-killed, preempted,
redeployed. dask treats the graph as the source of truth and recomputes what
was lost; killing a worker mid-computation still yields the right answer
([`0401_worker_failure.py`](data/dask-distributed/examples/0401_worker_failure.py)).
The exception is scattered data, which has no recipe and simply disappears with
the worker holding it.

**Client and workers must run the same libraries.** Version drift between a
client and a worker produces failures ranging from a warning to silently
different numerics. The Dockerfile in that project pins to `uv.lock` because
the stock image was one patch behind on numpy and warned on every connect.

**Where it stops:** data movement between workers, and the slowest single task.
No amount of hardware makes a batch finish faster than its longest task —
[`0503_task_stream.py`](data/dask-distributed/examples/0503_task_stream.py)
finds that floor in real task-stream records.

### icechunk — making storage transactional

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
  [`0301_atomicity.py`](data/icechunk/examples/0301_atomicity.py) contrasts
  this with a plain zarr store whose per-day means read
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
  for audit ([`0403_rewriting_history.py`](data/icechunk/examples/0403_rewriting_history.py)).
- **Cheap history, with one caveat.** Snapshots share chunks *by reference*, so
  an append costs one period, not one copy of the dataset. But sharing is by
  reference and not by content hash: rewriting a store with byte-identical
  values costs a full copy, because the writer touched every chunk.
  [`0501_storage_growth.py`](data/icechunk/examples/0501_storage_growth.py)
  measures both — six appends cost about 505 KB each, and history ends up at
  0.91x the size of the tip.

**Where it stops:** icechunk does not make anything faster. It makes concurrent
access *correct*. Its ceiling is contention on a single branch tip, which is
the subject of the next section.

### The ceilings, in one place

| Layer | Scales by | Ceiling | What you reach for next |
|---|---|---|---|
| numpy | nothing; it is the baseline | one array must fit in RAM | chunking |
| xarray + dask | splitting into chunks, computing chunk by chunk | one machine's cores and memory bandwidth | more machines |
| dask distributed | adding worker processes and machines | data movement, and the slowest single task | a better graph, not more hardware |
| icechunk | not throughput — correctness under concurrency | commit contention on one branch | separate branches, or fewer committers |

The row that surprises people is the third. Once work is genuinely distributed,
adding hardware stops helping: throughput tracks *total threads* rather than
worker count, and a batch can never finish faster than its longest single task.
[`0403_scaling.py`](data/dask-distributed/examples/0403_scaling.py) measures the
ladder — the same batch on 6 slots, then 4, then 2, at roughly 1.05s, 1.56s and
3.08s, very nearly the inverse of the slot count — and
[`0503_task_stream.py`](data/dask-distributed/examples/0503_task_stream.py)
catches a 1.50s straggler holding up a batch whose theoretical floor was 1.09s.
When that happens the fix is splitting the task, not growing the cluster.

---

## Storage: do you actually need S3?

This is the question worth understanding properly, because the answer is not
"yes" and it is not "no" — it depends on a specific property, and knowing which
one tells you exactly when the local filesystem stops being adequate.

### What a commit actually does

A commit is **compare-and-swap on a branch pointer**. icechunk writes the new
chunks and manifests as fresh immutable objects, then updates `main` to point
at the new snapshot — but only if `main` still points where it did when the
session started. If someone else moved it first, the swap fails and you get a
conflict, which you resolve by rebasing onto their work and retrying.

That single conditional update is the entire concurrency story. Everything else
icechunk writes is immutable and content-addressed, so two writers producing
different chunks never corrupt each other. The only contended resource is the
branch tip.

Object stores provide exactly the primitive this needs: a conditional PUT that
succeeds only if the object is unchanged. POSIX filesystems have no portable
equivalent — local tricks exist, but they do not survive NFS and similar
network filesystems, which is precisely where people put "shared" storage.

icechunk is candid about this. Opening a local-filesystem repository prints:

> The LocalFileSystem storage is not safe for concurrent commits. If more than
> one thread/process will attempt to commit at the same time, prefer using
> object stores.

That warning is the migration argument, not noise to silence. (The examples in
`data/icechunk` do silence it, via `quiet_icechunk_logs()`, because they are
single-writer by construction and the warning would drown the lesson — the
helper's docstring says so.)

### The rule

**One committer at a time makes local filesystem storage correct.** More than
one makes it a race.

Note the emphasis on *committer*, not *writer*. That distinction is what makes
distributed writes possible without an object store, and it is worth
understanding before deciding anything.

### How distributed writes actually work

The natural worry is that a dask cluster means many machines writing to one
store, and therefore many committers. That is not how icechunk does it.

The model is **fork/merge**, and it looks like this:

```python
session = repo.writable_session("main")   # coordinator opens one session
forks   = [session.fork() for _ in workers]  # one serializable child each
# ... each ForkSession is pickled to a worker, which does all its writes ...
session.merge(*returned_forks)            # coordinator merges the change sets
session.commit("ingest 2024-05")          # exactly ONE commit
```

Workers write chunk objects directly to storage — that part is genuinely
distributed and parallel. But every worker returns its change set to the
coordinator, which merges them and performs a single compare-and-swap. Many
writers, one committer.

So the concurrency hazard is not "distributed dask", it is "two independent
jobs committing to the same branch at once" — a second ingest, a manual fix
running while a scheduled sync is in flight, two replicas of the same service.

### The constraint that actually forces object storage

There is a second, more mundane reason to move, and in practice it bites first.

**Every participant must reach the same storage under the same identifier.**

A dask graph carries one path string, used by the client that built it and by
every worker that executes it. A local path satisfies that only when every
participant runs on the same machine — or on the same network mount, which
brings back the conditional-write problem and adds latency.

This is not theoretical. Building `data/dask-distributed`, a lazy zarr pipeline
driven from the host failed with `FileNotFoundError: /data/source.zarr` — the
workers could open it happily, the client could not see it at all, and one path
string cannot mean two things.
[`0303_distributed_xarray.py`](data/dask-distributed/examples/0303_distributed_xarray.py)
demonstrates the failure and the two shapes that do work: push the whole job to
one worker (correct, but you have thrown away the cluster), or keep the data in
the graph and never touch storage.

An `s3://bucket/store.zarr` URL dissolves the problem, because it resolves
identically everywhere — each side configuring its own endpoint and credentials,
while the identifier in the graph stays the same string.

### The decision table

| Situation | Local filesystem | Object store |
|---|---|---|
| Single process ingesting and serving (OCS today) | Fine | Unnecessary |
| Several processes on one machine, one committer | Fine | Optional |
| Dask workers on one machine, fork/merge writes | Fine | Optional |
| Dask workers on **separate machines** | Broken — no shared path | **Required** |
| Client builds a lazy graph, workers execute it | Broken unless co-located | **Required** |
| Two independent jobs committing concurrently | Unsafe race | **Required** |
| Clients reading the store directly over HTTP | Needs a server in front | Native |
| Horizontal scaling, replicas, ephemeral compute | Impractical | **Required** |

Reading down that table: the local filesystem is not a toy, and a
single-instance service on one box is a legitimate, correct deployment. It stops
being adequate the moment compute is spread across machines or a second writer
appears — and both of those arrive together the day you deploy a real cluster.

### What changes when you migrate, and what does not

Reassuringly little changes in code:

```python
storage = icechunk.local_filesystem_storage("/data/temperature.icechunk")
storage = icechunk.s3_storage(bucket="climate", prefix="temperature", region="eu-west-1")
```

The `Repository` API is identical across every backend icechunk ships —
`s3_storage`, `gcs_storage`, `azure_storage`, `r2_storage`, `tigris_storage`,
`http_storage`, `in_memory_storage`. Sessions, commits, branches, tags,
ancestry, expiry, and garbage collection all behave the same. Everything
[`data/icechunk`](data/icechunk/) teaches against the local backend transfers
unchanged.

What does change is the operational envelope:

- **Latency per operation rises**, from microseconds to milliseconds. Chunks
  should be larger than you would choose on local disk, and the number of tiny
  objects starts to matter.
- **Storage costs money and requests cost money.** The chunk-sharing behaviour
  in [`0501_storage_growth.py`](data/icechunk/examples/0501_storage_growth.py)
  stops being trivia: a rewrite that duplicates a dataset is now a line on a
  bill. Expiry and garbage collection
  ([`0502_expiry_and_gc.py`](data/icechunk/examples/0502_expiry_and_gc.py))
  become routine maintenance rather than a curiosity.
- **Conflicts become real.** With concurrent committers actually possible, the
  rebase paths in
  [`0303_conflicts.py`](data/icechunk/examples/0303_conflicts.py) turn into
  code you need: disjoint chunk edits rebase cleanly, two appends to the same
  dimension do not, and nothing resolves a doubly-updated array shape
  automatically.
- **Credentials and endpoints become configuration** that must be right in the
  API process and in every worker image — the same discipline as version
  pinning, and it fails the same way when it is wrong.

---

## Relationship to open-climate-service

OCS is a climate data platform: each instance is scoped to one country, ingests
from sources like CHIRPS and ERA5, stores results as GeoZarr in icechunk, and
exposes them through STAC, Zarr over HTTP, and openEO. Every project here was
chosen because OCS depends on it.

| OCS does this | Learn it here |
|---|---|
| Normalizes every source to `(time, y, x)`, Kelvin to Celsius | [`0101_normalize.py`](domain/climate/examples/0101_normalize.py) |
| Ingests one period at a time, committing each | [`0401_append_periods.py`](data/icechunk/examples/0401_append_periods.py) |
| Resumes an interrupted ingest from committed time steps | [`0402_resume.py`](data/icechunk/examples/0402_resume.py) |
| Builds multiscale pyramids by mean downsampling | [`0305_rolling_coarsen.py`](data/xarray/examples/0305_rolling_coarsen.py) |
| Rechunks to Zarr-legal uniform chunks before writing | [`0601_zarr_legal_chunks.py`](data/dask/examples/0601_zarr_legal_chunks.py) |
| Writes GeoZarr root attributes so clients can place the grid | [`0301_geozarr.py`](domain/climate/examples/0301_geozarr.py) |
| Publishes a STAC collection per dataset | [`0302_stac.py`](domain/climate/examples/0302_stac.py) |
| Runs openEO process graphs on dask | [`data/dask` phases 1–3](data/dask/ROADMAP.md) |

Two things here are deliberate re-implementations of OCS code, kept close to
the original: the `_uniform_chunks` fix in
[`0601_zarr_legal_chunks.py`](data/dask/examples/0601_zarr_legal_chunks.py),
and the open-or-create plus commit-and-append pattern in
[`helpers.py`](data/icechunk/src/playground_data_icechunk/helpers.py).

[`domain/climate`](domain/climate/) is the whole shape in miniature — a messy
source normalized, ingested as one commit per period, derived into
climatologies and indices, and published with GeoZarr attributes and a STAC
collection whose extents are read back off the store rather than declared. Run
[`0401_full_pipeline.py`](domain/climate/examples/0401_full_pipeline.py) to see
all six stages in one pass.

### One trap worth knowing before extending OCS

Appending variable-length months to a store chunked at 30 days along time fails
outright once the final chunk is partial:

> `ValueError: Specified Zarr chunks encoding['chunks']=(30, 32, 32) for
> variable named 't2m' would overlap multiple Dask chunks`

It is not a corner case — it appeared independently in `domain/climate` and
`data/icechunk`, on the same period, the fifth month in both. The fix is
`align_chunks=True` on the append, and both projects demonstrate it rather than
working around it
([`0401_append_periods.py`](data/icechunk/examples/0401_append_periods.py)
probes each period and reports honestly which ones would have succeeded
unaligned). It is the same family as the `_uniform_chunks` problem: dask and
zarr disagree about what a legal chunk layout is, and the disagreement surfaces
at write time.

---

## Conventions

Projects follow the [chapkit](https://github.com/dhis2-chap/chapkit) template:
Python 3.13, `uv` with the `uv_build` backend, src layout, ruff (120 columns,
Google docstrings), mypy and pyright in strict mode, pytest with coverage, and
mkdocs-material. Package names carry the full path — `data/xarray` is
`playground-data-xarray`, module `playground_data_xarray` — so names stay
unique across groups and never shadow the library being studied.

Projects needing real services add a `compose.yml` and `up`/`down` targets, and
their examples still run without Docker: `connect()` in
[`data/dask-distributed`](data/dask-distributed/) probes the cluster, falls
back to an in-process substitute, and prints what the fallback cannot show.

Verification is not a clean type-check: `make verify` runs every example and
reads its output, because compiling proves it builds, not that it works. Every
number quoted in this README came out of an example that actually ran. See
[`CLAUDE.md`](CLAUDE.md) for the full working rules.
