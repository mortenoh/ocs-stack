# icechunk

The `icechunk` project is fourteen runnable lessons in versioned, transactional
storage for Zarr v3 — the layer that sits underneath every dataset in
[open-climate-service](https://github.com/dhis2/open-climate-service). It starts
from an empty directory and ends at a retention policy, and along the way it
demonstrates the one property the whole storage layer rests on: a write either
commits in full or leaves no trace, so a dataset can be appended to while it is
being served over HTTP without a reader ever seeing half of it. Everything here
runs offline in about a second per example, against a temporary directory, with
no credentials and no cloud account.

---

## Contents

- [Introduction to icechunk](#introduction-to-icechunk) — the problem, the model,
  and what icechunk does not do
- [Setup](#setup) — commands and the shared helpers
- [Core concepts](#core-concepts) — repository, session, commit, branch, tag,
  ancestry, conflict, expiry
- [Phase 1 — Repositories, sessions, commits](#phase-1-repositories-sessions-commits)
- [Phase 2 — Time travel](#phase-2-time-travel)
- [Phase 3 — Transactions and safety](#phase-3-transactions-and-safety)
- [Phase 4 — The OCS ingest pattern](#phase-4-the-ocs-ingest-pattern)
- [Phase 5 — Operations](#phase-5-operations)
- [What is actually on disk](#what-is-actually-on-disk)
- [Storage backends](#storage-backends)
- [Pitfalls and gotchas](#pitfalls-and-gotchas)
- [How this maps to open-climate-service](#how-this-maps-to-open-climate-service)
- [Where to go next](#where-to-go-next)
- [Further reading](#further-reading)

---

## Introduction to icechunk

### The shape of the problem

Start with a service that does two things at once.

It ingests. Every night a job wakes up, fetches yesterday's ERA5 or CHIRPS
grids, normalizes them, and appends them to a dataset on disk. The dataset is
one array of daily values with dimensions `(time, y, x)`, and the append grows
it along `time`.

It serves. At the same time, an HTTP API answers requests against that same
dataset. Somebody asks for a time series at a point, or a map for a date, or a
monthly mean over a region. Those requests arrive whenever they arrive, which
includes the exact moment the ingest job is writing.

Both halves of that are ordinary. Put them together against a plain zarr store
and you have a problem that is not ordinary at all, and that does not announce
itself: it shows up as a wrong answer, occasionally, under load.

### What a plain zarr store is

To see why, you need to know what zarr actually is on disk, because the answer
is more literal than people expect.

A [Zarr v3](https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html) store
is a **key-value mapping**. That is the entire abstraction. Keys are strings that
look like paths; values are byte strings. The spec calls the thing that provides
this mapping a *store*, and everything else in zarr is defined in terms of
operations on it: get a key, set a key, list keys with a prefix, delete a key.

Two kinds of value live in that mapping.

**Metadata.** A key ending in `zarr.json` holds a JSON document describing a
group or an array. For an array it records the shape, the data type, the chunk
grid, the fill value, the codec pipeline used to compress chunks, the dimension
names, and any user attributes. So `t2m/zarr.json` might say: shape
`[182, 32, 32]`, dtype `float64`, chunks of `[30, 32, 32]`, dimension names
`["time", "y", "x"]`, attributes `{"units": "degC"}`.

**Chunks.** The array is cut into a regular grid of blocks, and each block is
stored under its own key, typically `t2m/c/0/0/0`, `t2m/c/1/0/0`, and so on —
the numbers being the block's coordinates on the chunk grid, not element
indices. Each value is that block's elements, serialized and put through the
codec pipeline, which usually means compressed. Reading `t2m[45, 10, 10]` means
working out which block that element lands in, fetching exactly that one key,
decompressing it, and indexing into the result. That is the whole trick, and it
is why zarr scales: you never read more than the blocks you touch, and blocks
are independent, so N readers or N writers can work on N different blocks at
once with no coordination.

The store abstraction is deliberately thin because it is meant to be
implemented over many different things. A directory on a local filesystem is one
implementation: keys become file paths. An S3 bucket is another: keys become
object keys. A ZIP file, an in-memory dict, an HTTP server serving static files
— all legitimate stores. Python's zarr library ships several
(`LocalStore`, `MemoryStore`, `FsspecStore`, `ZipStore`) and lets you supply
your own. That extension point is exactly where icechunk plugs in, which we will
come back to.

The [zarr-python documentation](https://zarr.readthedocs.io/) is the practical
reference for all of this; the spec above is the normative one.

### What a reader sees mid-write

Now the awkward question. A writer is appending February to a dataset that
already holds January. What, precisely, does a reader see while that is
happening?

Work through what the writer has to do. Appending 29 days along `time` to an
array chunked at 30 days per block means:

1. Some new chunk keys get written — the blocks covering the new time steps.
2. Possibly some *existing* chunk keys get rewritten — if the last January
   block was partial, the first new days land inside it, so that block is read,
   modified, and put back.
3. The array's `zarr.json` gets rewritten, because `shape` changed from
   `[31, ...]` to `[60, ...]`.
4. The `time` coordinate array's chunks and metadata get the same treatment.

That is not one operation. It is a few dozen independent key writes against the
store, in whatever order the writer happens to issue them, with no relationship
between them that the store knows about. There is no transaction. There is no
"begin", there is no "commit", and there is nothing that makes step 3 wait for
step 1 or vice versa.

So a reader arriving in the middle can land on any of these:

- **Metadata ahead of data.** `zarr.json` already says the array has 60 time
  steps, but the chunks for days 31 to 60 have not been written yet. The reader
  asks for the new days and gets... whatever the store does for a missing chunk,
  which in zarr is the **fill value**, silently. Not an error. Not a warning.
  A block of zeros, or NaNs, or whatever the fill value happens to be, presented
  as data. If that reader is computing a monthly mean, it gets a number, and the
  number is wrong.
- **Data ahead of metadata.** The chunks are written but `zarr.json` still says
  31 time steps. The reader sees January only. That is at least self-consistent
  — it is just stale — but the reader has no way to know it is stale.
- **A torn rewrite.** The partial January block is being read-modified-written.
  A reader that fetches it between the delete and the put gets a missing chunk,
  hence fill values, in the middle of data that definitely exists.
- **Half a reprocess.** The worse version. A correction rewrites all of 2024
  with new values. The writer gets through March and dies. The store now holds
  January to March at the new revision and April to December at the old one, and
  it looks completely healthy. Nothing on disk records that a write was in
  flight. Nothing will ever tell you.

[`0301_atomicity.py`](../../icechunk/examples/0301_atomicity.py) does exactly
that last one against a plain zarr directory, deliberately, and prints the
result:

```text
The same crash against a plain zarr directory (one chunk per time step):
  before: step means=[26.0, 26.0, 26.0, 25.9, 26.0, 26.0, 26.0, 26.0, 26.0, 25.9]
  ingest raised: upstream API returned 503 halfway through the ingest
  after:  step means=[126.0, 126.0, 126.0, 125.9, 126.0, 26.0, 26.0, 26.0, 26.0, 25.9]
  steps 0-4 are the new revision, steps 5-9 are the old one -- a store no reader should see
```

Five days at the new revision, five at the old, in one array, with no marker of
any kind. Every subsequent read of that store returns that mixture as if it were
the dataset.

Note what did *not* happen. Nothing crashed on the read side. No file was
corrupt. Every individual chunk is a perfectly valid, correctly compressed
block. The store is internally consistent at the level zarr cares about, and
wrong at the level you care about. That is the failure mode: it is invisible
locally and only detectable by knowing what the data should have been.

### The workarounds, and why they are not enough

People reach for the same three fixes, and it is worth knowing why each one is
partial.

**Write to a temporary location and swap.** Build `dataset.zarr.new` from
scratch, then rename it over `dataset.zarr`. This does give atomicity, on a
local POSIX filesystem, for a directory rename. But it costs a full copy of the
dataset on every append — you cannot rename a directory *into* an existing one
atomically, so incremental updates are out. And on object storage there is no
rename at all; "rename" is copy-then-delete over every object, which is neither
atomic nor cheap. For a growing archive this is a non-starter.

**Take a lock.** Have the writer hold an exclusive lock and readers take shared
ones. Now correctness depends on every participant, forever, honouring a
convention that lives outside the data. Readers block during ingest, which for a
service means requests that time out at exactly the busiest moment. And
distributed locking over object storage is its own research problem.

**Never write in place; publish versioned copies.** Write
`dataset-2024-02.zarr` as a new full store, then flip a pointer. This is
actually correct, and it is the shape of the right answer — but done naively it
stores one full copy per version, and you build the pointer-flipping, the
retention, and the cleanup yourself.

That third answer, done properly — with sharing between versions so a new
version costs only what changed — is essentially what icechunk is.

### What icechunk is

[icechunk](https://icechunk.io/) is a **transactional storage engine for Zarr**.
Concretely, it is an implementation of the zarr store interface, written in Rust
with Python bindings, that adds a transaction boundary and a version history
around the key-value mapping zarr expects.

The core idea is a redirection. In a plain zarr store, the chunk at coordinate
`[1, 0, 0]` of array `/t2m` lives at a key derived from those coordinates —
`t2m/c/1/0/0` — so the coordinate *is* the address, and writing that coordinate
means overwriting that address. In icechunk, chunks are written as immutable
objects under opaque, content-independent ids, and a separate structure records
which object currently holds which coordinate. Change the data and you write a
*new* object and a *new* mapping; the old object is untouched and the old
mapping still points at it.

That indirection buys everything else:

- **Immutability.** Nothing written is ever modified. Chunk objects, manifests,
  and snapshots are all write-once.
- **Atomicity.** Since the new data does not overwrite the old, it can all be
  written at leisure while readers continue to resolve the old mapping. The
  change becomes visible in exactly one step: a single pointer update.
- **Versioning.** The old mapping did not go anywhere, so it is still a complete,
  readable view of the dataset. Every state the dataset has been in remains
  addressable.
- **Sharing.** A new version references the chunk objects it did not change
  rather than copying them, so a version costs what it changed and nothing more.

The pointer update at the end is a **compare-and-swap**: set the branch to the
new snapshot, but only if it still points where it did when this write began. If
it moved, somebody else committed first, the swap fails, and you get a conflict
instead of a silent overwrite. That single conditional operation is the entire
concurrency mechanism. [Storage](../storage.md) works through what it implies
for backend choice; the short version is that object stores provide conditional
writes natively and POSIX filesystems do not.

### The git-like model

icechunk borrows git's vocabulary, and the analogy holds up further than most
borrowed vocabularies do. It is worth being precise about where it holds and
where it does not.

**Snapshot.** One complete, immutable state of the whole repository: every group,
every array, all metadata, and a mapping from every chunk coordinate to the
object holding it. This is git's commit object, and like a commit it names its
parent, carries a message, and carries a timestamp. It is identified by a
20-character id such as `G69TFKYKQYA7XHG9J7C0`. These ids are generated fresh
per commit, so every id quoted in this page will differ if you run the example
yourself — only the *shape* of them is stable. One id is not generated: the root
snapshot of every icechunk repository ever created is `1CECHNKREP0F1RSTCMT0`, a
fixed sentinel, and it shows up in the history of all fourteen examples.

**Ancestry.** The parent chain from a snapshot back to that root. `repo.ancestry()`
walks it, newest first, and it always terminates. This is `git log`.

**Branch.** A mutable name pointing at a snapshot. Committing on a branch moves
the name to the new snapshot. Every repository starts with `main`. This is a git
branch, and it is exactly as cheap: creating one writes a name and a 20-character
id, and copies no data.

**Tag.** An immutable name pointing at a snapshot. Unlike git, icechunk enforces
the immutability: `create_tag` on a name that already exists raises
`AlreadyExistsError` rather than repointing. That is stricter than git's
`--force` and it is the right default for a name published in a model card.

**Commit.** Turning a session's accumulated changes into a snapshot and moving
the branch to it. Returns the new snapshot id.

**Conflict and rebase.** Two sessions started from the same snapshot; the second
to commit finds the branch moved and raises `ConflictError`. `session.rebase()`
replays the session's changes against the new tip, succeeding when the two
change sets are compatible and raising `RebaseFailedError` with a list of
specific conflicts when they are not.

Now the differences, which matter as much as the similarities:

- **There is no merge of two branches.** Git merges histories. icechunk rebases a
  *session* onto a moved branch tip. You cannot merge `experiment` into `main`;
  you promote a snapshot onto a branch with `reset_branch`, which is closer to
  `git reset --hard` than to `git merge`.
- **There is no working directory.** A session is not a checkout you edit and
  then stage. It is a live store you write through, and its changes exist only
  in the session until commit.
- **The diff is structural, not textual.** `repo.diff` reports arrays created,
  deleted, or updated, and the exact chunk *coordinates* rewritten. There is no
  line-level anything.
- **The diff is directional.** `from` must be an ancestor of `to`. It answers
  "what happened between then and now", never "how do these two states differ".
- **History can be destroyed on purpose.** `expire_snapshots` plus
  `garbage_collect` is a retention policy, and it is irreversible. Git's garbage
  collection only removes what is genuinely unreachable; icechunk's expiry
  *makes* things unreachable, deliberately, because chunk data is large and
  storage costs money.

### Where it sits under zarr and xarray

The stack, bottom to top:

```text
    xarray                 Dataset / DataArray, labeled dims, coords
       |
       |  ds.to_zarr(store, ...) / xr.open_zarr(store, ...)
       v
    zarr-python            groups, arrays, chunk grid, codecs, indexing
       |
       |  the store interface: get(key) / set(key, value) / list_prefix(...)
       v
    icechunk               IcechunkStore: transactions, snapshots, branches
       |
       |  immutable objects with opaque ids
       v
    object storage         local directory, S3, GCS, Azure, R2, Tigris, HTTP
```

The interface between icechunk and zarr is the store, which is why the
integration is as small as it is. `session.store` is an `IcechunkStore`, and an
`IcechunkStore` satisfies the same contract as zarr's `LocalStore`. Every layer
above it is unmodified, unaware, and unchanged:

```python
session = repo.writable_session("main")
ds.to_zarr(session.store, mode="w", zarr_format=3, consolidated=False)
session.commit("ingest 2024-01")
```

The middle line is a plain `xarray.Dataset.to_zarr` call. There is no icechunk
argument in it. xarray does not know what it is writing to; zarr does not know
either. The two lines around it are the entire icechunk-specific surface of a
write, and reads are one line:

```python
ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
```

[`0101_repo_basics.py`](../../icechunk/examples/0101_repo_basics.py) confirms the
type at runtime, and then round-trips through xarray to prove the point:

```text
  session.store is a IcechunkStore
...
And the payoff: a readonly session's store opens like any other zarr store.
  sizes={'time': 30, 'y': 64, 'x': 64}, vars=['t2m']
  t2m chunks=(15, 32, 64), mean=26.000
  attrs={'units': 'degC', 'long_name': '2 metre temperature'}
```

Two practical notes on those calls, both of which appear in every example:

**`consolidated=False` everywhere.** Consolidated metadata is a zarr v2-era
optimisation: copy every `zarr.json` in the hierarchy into one file at the root
so that opening a store costs one request instead of one per node. It is a
latency workaround for stores where listing is expensive, and it is a
*duplicate* of the real metadata, which means it can go stale. icechunk stores
the whole hierarchy inside the snapshot object already, so opening is one fetch
regardless, and a consolidated copy would be a second source of truth for no
gain. Pass `consolidated=False` on both read and write and stop thinking about
it.

**`zarr_format=3` on creating writes.** icechunk is a Zarr v3 store. The flag is
explicit in the examples so nothing depends on the library's default, which has
changed across versions.

### What icechunk does not do

This section exists because the most common wrong expectation about icechunk is
that it is an optimisation.

**It is not faster.** Reading a chunk through icechunk means resolving a branch
to a snapshot, consulting a manifest to map the coordinate to an object id, and
then fetching that object — where a plain zarr store computes the key
arithmetically and fetches it. That is strictly more work. Caching amortises the
lookup well and the difference is usually small, but the direction is not in
doubt. If your problem is that reads are slow, icechunk is not the answer;
chunk sizing is (`dask/examples/0602_chunk_sizing.py`), or a pyramid is
(`climate-pipeline/examples/0203_pyramid.py`).

**It does not make writes parallel.** Parallel writing is a zarr property —
independent chunks, independent keys — and it works fine without icechunk.
icechunk makes concurrent access *correct*: readers see coherent states, and
two writers who conflict find out rather than silently interleaving. Correct,
not fast. That is the entire trade.

**It does not compress better.** Codecs are zarr's business and are configured
through zarr's encoding, exactly as they would be otherwise.

**It does not deduplicate by content.** This one costs people real money, so it
is worth stating plainly. Chunks are shared between snapshots **by reference**:
a snapshot reuses a chunk object when the write never touched that coordinate.
It is not a content-addressed store. Writing byte-identical data to a chunk
writes a new object.
[`0501_storage_growth.py`](../../icechunk/examples/0501_storage_growth.py)
measures precisely this — a full rewrite with identical values roughly doubled
the store:

```text
Rewriting the whole dataset with byte-identical values:
  full rewrite, same values          snapshots=10  chunk bytes=6,509,787  delta=+3,001,972  on disk=6,531,943
  the tip's values are unchanged (True) but the store grew by a full dataset
```

So "is this write a no-op?" is a question about which chunks your code touched,
never about which values it wrote.

**It does not merge branches.** See the previous section.

**It does not resolve every conflict.** Disjoint chunk edits rebase cleanly. Two
appends to the same dimension cannot be reconciled by any policy, because both
sides changed the array's shape and there is no correct answer.
[`0303_conflicts.py`](../../icechunk/examples/0303_conflicts.py) demonstrates
both outcomes.

**It does not give you a merge-free multi-writer system.** One committer at a
time per branch is the model. Many writers is supported, but through fork/merge,
which is the next section.

**It does not undo expiry.** `expire_snapshots` is the one operation in the whole
project that destroys history, and there is no inverse.

### Distributed writes: fork and merge

There is a natural worry when you first meet the compare-and-swap model: if a
commit is one conditional pointer update, and a dask cluster has forty workers
writing to one dataset, does that mean forty racing commits?

No, and the reason is worth understanding before you plan any cluster
deployment. icechunk's distributed write model is **fork/merge**, documented
upstream at <https://icechunk.io/en/latest/parallel/>. The shape is:

```python
session = repo.writable_session("main")     # coordinator opens one session
forks = [session.fork() for _ in workers]   # one serializable child per worker
# each ForkSession is pickled to a worker, which writes its chunks directly
# to storage and returns its change set
session.merge(*returned_forks)              # coordinator folds the change sets in
session.commit("ingest 2024-05")            # exactly ONE compare-and-swap
```

`Session.fork()` returns a `ForkSession` that can be pickled and shipped to a
worker. The worker writes chunk objects to storage itself — that part is
genuinely distributed and genuinely parallel, and it is where the throughput
comes from. What the worker does *not* do is commit. It returns its change set,
the coordinator merges every worker's change set into its own session, and
performs a single commit.

Many writers. One committer.

The consequence for planning: the concurrency hazard in a real deployment is
never "we run dask". It is "two independent jobs commit to the same branch at
the same time" — a scheduled ingest overlapping a manual backfill, two replicas
of a service both configured to sync, a retry firing while the original is
still running. Those are the cases that need either an object store's
conditional PUT or an external guarantee that only one of them runs.

The `icechunk` project's examples are all single-process and single-writer, so
they do not exercise `fork`/`merge`; the upstream page above is the reference,
and [Storage](../storage.md) works through the deployment argument in full.

### Reference links

Everything above is a summary of material that is documented properly upstream.

- **icechunk** — <https://icechunk.io/> — the project site, with the full Python
  API reference, the format specification, and the version-control guide.
- **Parallel and distributed writes** — <https://icechunk.io/en/latest/parallel/>
  — the fork/merge model in detail, including the dask and cubed integrations.
- **Zarr v3 core specification** —
  <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html> — the normative
  definition of the store interface, the chunk grid, metadata documents, and the
  codec pipeline.
- **zarr-python** — <https://zarr.readthedocs.io/> — the practical library
  reference: creating arrays, encoding, codecs, and the store implementations
  icechunk sits beside.

---

## Setup

The project is self-contained: its own `pyproject.toml`, its own `.venv`, its own
`uv.lock`. There is no root package and no uv workspace, so nothing outside the
directory needs installing.

```bash
cd icechunk
make install                            # uv sync
make run EXAMPLE=0101_repo_basics       # run one example
make run-all                            # run all fourteen, in order
make test                               # pytest over tests/
make lint                               # ruff format, ruff check --fix, mypy, pyright
make ci                                 # lint + test
```

`make install` is `uv sync`, which resolves and installs from `uv.lock` into
`.venv`. The dependency set is small: `icechunk`, `zarr`, `xarray`, `dask`,
`netcdf4`, and `scipy`. At the time of writing the versions actually resolved are
icechunk 2.1.2, zarr 3.3.0, and xarray 2026.7.0 — worth checking against your own
`uv.lock`, since a couple of the behaviours described here (the
`total_chunks_storage` deprecation in particular) are version-specific.

`make run EXAMPLE=<name>` takes the example's basename without the `.py`:

```bash
make run EXAMPLE=0401_append_periods
```

The Makefile refuses politely if you forget:

```text
set EXAMPLE=<name>, e.g. make run EXAMPLE=0101_basics
```

`make run-all` loops over `examples/*.py` in sorted order and stops at the first
non-zero exit, so it doubles as a smoke test. Shell-glob ordering matches the
`PPNN` numbering, so it runs the phases in teaching order.

You can also skip the Makefile:

```bash
cd icechunk
uv run python examples/0303_conflicts.py
```

Every example is self-contained, offline, deterministic in its data (the noise
term is seeded), and non-interactive. Each writes into a
`tempfile.TemporaryDirectory()` and cleans up after itself, so running the whole
set leaves nothing behind. Snapshot ids are *not* deterministic — they are
generated per commit — so every id printed will differ from every id quoted in
this document.

`make test` runs ten tests over the helper module:

```text
>>> Running tests
..........                                                               [100%]
10 passed in 0.50s
```

The timing there is machine-dependent, like every timing in this page.

### The helpers

`src/playground_icechunk/helpers.py` holds six functions, re-exported from the
package root. They exist so the examples can say what they mean without
repeating six lines of session boilerplate each time, and two of them are
deliberate re-implementations of open-climate-service code kept close to the
original.

The full API reference is at [reference/icechunk.md](../reference/icechunk.md),
generated from the docstrings. What follows is what each one is for.

#### `quiet_icechunk_logs()`

```python
def quiet_icechunk_logs() -> None:
    """Silence icechunk's Rust-layer INFO and WARN output."""
    import icechunk

    icechunk.set_logs_filter("error")
```

Every example calls this as the first line of `main()`, and it deserves a proper
explanation rather than being dismissed as noise suppression, because what it
suppresses is a genuinely important warning.

icechunk's core is Rust, and the Rust layer has its own logging that does not go
through Python's `logging` module — it writes to stderr directly, formatted and
colourised by the `tracing` crate. Opening a local-filesystem repository triggers
two of these on a fresh process. Here is exactly what they look like, captured by
creating and then reopening a repository *without* the helper (ANSI colour codes
stripped):

```text
=== create ===
  2026-08-17T17:49:23.602279Z  WARN icechunk_arrow_object_store: The LocalFileSystem
  storage is not safe for concurrent commits. If more than one thread/process will
  attempt to commit at the same time, prefer using object stores.
    at icechunk-arrow-object-store/src/lib.rs:329

  2026-08-17T17:49:23.603297Z  WARN icechunk_storage::readback: conditional PUT is
  enabled but `unsafe_use_metadata` is disabled - lost-response recovery for
  conditional writes requires user metadata to stamp write-ids; without it, transient
  PUT failures may surface as spurious conflicts even when the write actually landed.
  See icechunk_storage::Settings::unsafe_use_metadata.
    at icechunk-storage/src/readback.rs:28

=== open ===
  2026-08-17T17:49:24.210678Z  WARN icechunk_arrow_object_store: The LocalFileSystem
  storage is not safe for concurrent commits. If more than one thread/process will
  attempt to commit at the same time, prefer using object stores.
    at icechunk-arrow-object-store/src/lib.rs:329
```

(Line-wrapped here for readability; each warning is one long line plus a source
location in the real output.)

The first is the important one. It is the single most consequential fact about
running icechunk on a local filesystem, and [Storage](../storage.md) treats it as
the migration argument rather than as noise: a commit needs a conditional write,
POSIX has no portable one, so two processes committing to the same local
repository at the same instant can both believe they won. icechunk warns instead
of pretending.

The second concerns lost-response recovery: without user metadata stamping
write-ids, a PUT that succeeded but whose response was lost can surface as a
spurious conflict on retry. That is object-store failure-mode territory, not
something a local example can exercise.

So why silence them?

Because these examples are single-writer by construction. Every one of them
creates a fresh repository in a temporary directory, writes from one process, and
throws it away. The warning is correct advice about a hazard none of them can
hit, and each example opens a repository several times — some open one per
readonly session — so leaving it on means the same paragraph interleaving with
the lesson a dozen times per run. The lesson would be unreadable.

The rule this implies for real code is the opposite of what the helper does:
**do not silence these in production.** If your service prints "not safe for
concurrent commits" on every open and you have more than one process that
commits, that warning is describing your actual bug.

#### `open_repo(path)`

```python
def open_repo(path: Path | str) -> Any:
    """Open an icechunk repository, creating it if it does not exist."""
    import icechunk

    path = Path(path)
    storage = icechunk.local_filesystem_storage(str(path))
    if path.exists():
        return icechunk.Repository.open(storage)
    path.parent.mkdir(parents=True, exist_ok=True)
    return icechunk.Repository.create(storage)
```

Open-or-create, mirroring `open_or_create_repo` in open-climate-service. Note the
two-step shape: `Repository.create` and `Repository.open` are separate calls, and
which one you want depends on whether the directory is already there. icechunk
does ship `Repository.open_or_create(storage)` which does this in one call; the
helper spells it out because OCS spells it out, and because the explicit form
makes it obvious that "does this repository exist" is a question about storage,
not about icechunk.

The return type is `Any`, and so is every icechunk object in this project. That
is not laziness: icechunk ships no type stubs, so mypy and pyright — both in
strict mode, both required to pass — cannot see into it. The examples annotate
icechunk locals as `Any` explicitly and say so in a comment where it first
appears.

The `.icechunk` suffix on the path is convention, not requirement. OCS uses
`{data_dir}/downloads/{dataset_id}.icechunk` and the examples follow suit.

#### `write_dataset(repo, ds, message, *, branch="main", append_dim=None)`

```python
def write_dataset(
    repo: Any, ds: xr.Dataset, message: str, *, branch: str = "main", append_dim: str | None = None
) -> str:
    """Write a dataset to a branch and commit it as one transaction."""
    session = repo.writable_session(branch)
    if append_dim is None:
        ds.to_zarr(session.store, mode="w", zarr_format=3, consolidated=False)
    else:
        ds.to_zarr(session.store, append_dim=append_dim, consolidated=False)
    return str(session.commit(message))
```

The complete write cycle in three lines: open a writable session on a branch,
hand `session.store` to xarray, commit. It returns the snapshot id as a string,
which is the handle everything else in the project is built on.

The `append_dim` branch is the difference between the first write and every
subsequent one. `mode="w"` creates the arrays and fixes their chunk shape;
`append_dim="time"` extends them. Getting that wrong the other way — `mode="w"`
on a store that already holds data — is a full overwrite that deletes and
recreates the arrays, which
[`0203_diffing.py`](../../icechunk/examples/0203_diffing.py) demonstrates and
recommends against.

This helper is deliberately simple, and the examples outgrow it where the lesson
needs more: an explicit `encoding={"t2m": {"chunks": ...}}` on the creating
write, `align_chunks=True` on appends, `region={"time": slice(...)}` for
corrections. Where an example manages its own session rather than calling
`write_dataset`, that is why.

#### `read_dataset(repo, *, branch="main", snapshot_id=None, tag=None)`

```python
def read_dataset(
    repo: Any, *, branch: str | None = "main", snapshot_id: str | None = None, tag: str | None = None
) -> xr.Dataset:
    """Open a dataset from a branch tip, a tag, or a specific snapshot."""
    if snapshot_id is not None:
        session = repo.readonly_session(snapshot_id=snapshot_id)
    elif tag is not None:
        session = repo.readonly_session(tag=tag)
    else:
        session = repo.readonly_session(branch)
    ds: xr.Dataset = xr.open_zarr(session.store, consolidated=False)
    return ds
```

The three ways to name a point in history, in one signature. A snapshot id or a
tag takes precedence over the branch. Underneath, all three produce a readonly
session, which pins one snapshot for the life of the session — that pinning is
the subject of [`0302_isolation.py`](../../icechunk/examples/0302_isolation.py).

icechunk also supports `readonly_session(branch, as_of=datetime)`, which resolves
to the last snapshot on that branch at or before a wall-clock time. The helper
does not expose it; it is worth knowing exists.

#### `describe_history(repo, *, branch="main")`

```python
def describe_history(repo: Any, *, branch: str = "main") -> list[dict[str, Any]]:
    """Summarize a branch's commit history, newest first."""
    history: list[dict[str, Any]] = []
    for snapshot in repo.ancestry(branch=branch):
        history.append(
            {
                "id": str(snapshot.id),
                "short": str(snapshot.id)[:8],
                "message": snapshot.message,
                "written_at": snapshot.written_at,
            }
        )
    return history
```

`repo.ancestry()` yields `SnapshotInfo` objects lazily, newest first. This flattens
them into plain dicts so examples can print, count, slice, and compare them
without holding icechunk objects. The `short` field is the first eight characters
of the id, which is what every example prints — enough to be unambiguous within
one run, short enough to fit in a table.

Note that `ancestry` is an iterator over a chain that ends at the root, so it
always terminates, but on a long history it is doing one fetch per step. Take a
slice rather than materialising the whole thing when you only need the tip.

#### `climate_dataset(days=30, ny=32, nx=32, start="2024-01-01", offset=0.0, seed=0)`

```python
def climate_dataset(
    days: int = 30, ny: int = 32, nx: int = 32,
    start: str = "2024-01-01", offset: float = 0.0, seed: int = 0,
) -> xr.Dataset:
    """Build a small OCS-shaped dataset with dims (time, y, x)."""
    for name, value in (("days", days), ("ny", ny), ("nx", nx)):
        if value < 1:
            raise ValueError(f"{name} must be at least 1, got {value}")

    rng = np.random.default_rng(seed)
    gradient = np.linspace(2.0, -2.0, ny).reshape(1, ny, 1)
    values = 26.0 + offset + gradient + rng.normal(0.0, 0.5, size=(days, ny, nx))
    return xr.DataArray(
        values,
        dims=("time", "y", "x"),
        coords={"time": pd.date_range(start, periods=days, freq="D")},
        name="t2m",
        attrs={"units": "degC", "long_name": "2 metre temperature"},
    ).to_dataset()
```

Synthetic data in the shape OCS actually stores: one variable `t2m` in degrees
Celsius, dimensions `(time, y, x)`, a daily time coordinate, a north-south
gradient, and seeded noise.

The `offset` parameter is the workhorse of the whole project. Every example that
needs to tell two revisions apart writes them with different offsets, so a single
number — the mean — identifies which version you are looking at. Base is 26.0, so
`offset=5.0` gives a mean near 31.0 and `offset=273.15` gives a plausibly
Kelvin-looking 299.15. When you see `mean=26.002` in an output block below, that
is offset 0; `mean=126.002` is offset 100, the deliberately-broken revision.

---

## Core concepts

This section is the reference the phase walkthroughs assume. Each concept gets
the API, a runnable snippet, and real output. Everything here is drawn from the
examples, so nothing is theoretical.

### Repository

A repository binds a storage backend to a versioned history. It is the top-level
object and it is cheap to construct — creating one writes a few hundred bytes.

```python
import icechunk

storage = icechunk.local_filesystem_storage("/data/era5_t2m.icechunk")
repo = icechunk.Repository.create(storage)     # fails if one already exists
repo = icechunk.Repository.open(storage)       # fails if one does not
repo = icechunk.Repository.open_or_create(storage)
```

`Repository.exists(storage)` answers the question without raising.

A freshly created repository is not empty. From
[`0101_repo_basics.py`](../../icechunk/examples/0101_repo_basics.py):

```text
  created era5_t2m.icechunk
  branches at creation: ['main']

Creation alone already writes files -- an empty repository is not an empty
directory. It has one snapshot: the empty root the first commit descends from.
  on disk after create:
    .                1 entries       272 bytes
    snapshots        1 entries       192 bytes
    transactions     1 entries       118 bytes
```

One branch, `main`. One snapshot, the root, whose message is
`Repository initialized` and whose id is the fixed sentinel
`1CECHNKREP0F1RSTCMT0`. Every real commit descends from it, which is what
guarantees an ancestry walk terminates.

Nothing about the repository lives in memory. Reopening the path gives you the
same history:

```text
Reopening the same path returns the same history -- nothing lives in memory.
  tip of main: G69TFKYKQYA7XHG9J7C0
  matches the snapshot we committed: True
```

### Storage backends

The backend is chosen once, when the `Storage` object is built, and nothing above
it changes:

```python
icechunk.local_filesystem_storage("/data/era5.icechunk")
icechunk.s3_storage(bucket="climate", prefix="era5", region="eu-west-1")
icechunk.gcs_storage(bucket="climate", prefix="era5")
icechunk.azure_storage(account="climate", container="data", prefix="era5")
icechunk.r2_storage(bucket="climate", prefix="era5", account_id="...")
icechunk.tigris_storage(bucket="climate", prefix="era5")
icechunk.http_storage("https://example.org/store/")     # read-only
icechunk.in_memory_storage()                            # tests
```

The `Repository` API is identical across all of them. [Storage
backends](#storage-backends) below covers the shapes in more detail;
[Storage](../storage.md) covers *which one you actually need*, which is the more
interesting question.

### Session

A session is a view of the repository that you read or write through. It is the
only way to reach the data.

```python
writable = repo.writable_session("main")            # bound to a branch
readonly = repo.readonly_session("main")            # pinned to that branch's tip now
readonly = repo.readonly_session(tag="v2")          # pinned to a tag
readonly = repo.readonly_session(snapshot_id="...")  # pinned to a snapshot
```

The difference is enforced by the store, not by convention.
[`0101_repo_basics.py`](../../icechunk/examples/0101_repo_basics.py):

```text
Two kinds of session, and the difference is enforced by the store itself:
  writable_session('main'): read_only=False, branch='main'
  readonly_session('main'): read_only=True, branch=None
  the readonly session pinned snapshot G69TFKYKQYA7XHG9J7C0
  writing to the readonly store raises ValueError: Store is read-only but mode is 'w'. Create a writable store or use 'r' mode.
```

Note `branch=None` on the readonly session. That is the important asymmetry: a
writable session knows which branch it will commit to, a readonly session does
not have a branch at all — it resolved one to a snapshot id at creation and then
forgot about it. Which is precisely why a reader is not affected by later
commits.

Useful session members:

| Member | Meaning |
|---|---|
| `session.store` | the `IcechunkStore` to hand to zarr or xarray |
| `session.read_only` | whether writes are permitted |
| `session.branch` | the branch a writable session will commit to, else `None` |
| `session.snapshot_id` | the snapshot this session is based on |
| `session.has_uncommitted_changes` | whether anything has been written since it opened |
| `session.status()` | a `Diff` of the uncommitted changes |
| `session.discard_changes()` | throw away uncommitted work, keep the session |
| `session.commit(message)` | turn changes into a snapshot |
| `session.rebase(solver)` | replay changes onto a moved branch tip |
| `session.fork()` | a picklable child session for a distributed worker |
| `session.merge(*forks)` | fold workers' change sets back in |

### Commit

```python
snapshot_id = session.commit("ingest 2024-01")
```

Commit does two things in order: writes a snapshot object recording the complete
state, then compare-and-swaps the branch pointer from the session's parent
snapshot to the new one. If the branch moved in between, the swap fails and you
get `icechunk.ConflictError`.

The whole change becomes visible at that instant and not before. Nothing partial
is ever observable. From
[`0102_commits.py`](../../icechunk/examples/0102_commits.py), with an independent
reader peeking at each stage:

```text
  reader before the write : nothing -- xarray raises GroupNotFoundError, the store looks empty
  writer has written data: has_uncommitted_changes=True
  reader after the write  : nothing -- xarray raises GroupNotFoundError, the store looks empty
  The bytes are on disk. The reader still cannot see them, because no
  snapshot points at them yet.

Now commit -> snapshot FCG51Q80KG62RX3MAM90
  reader after the commit : time=30, t2m mean=26.002
```

The bytes were on disk before the commit. They were simply unreachable.

Commit takes several optional arguments worth knowing:

```python
session.commit(
    "ingest 2024-01",
    metadata={"source": "era5", "run_id": 91},   # arbitrary structured metadata
    rebase_with=icechunk.ConflictDetector(),     # auto-rebase on conflict
    rebase_tries=1000,                           # how many times
    allow_empty=False,                           # commit even with no changes
)
```

`rebase_with` folds the whole retry loop into the commit call, which is what you
want in production code rather than hand-writing try/except/rebase/retry.

There is also a context-manager form that opens a session, yields the store, and
commits on clean exit:

```python
with repo.transaction("main", message="ingest 2024-01") as store:
    ds.to_zarr(store, append_dim="time", consolidated=False)
```

### Snapshot id

A 20-character identifier for one immutable state of the repository, returned by
`commit()`.

```text
  offset=  +0.0 -> VCFTZ4H1X2YMAHXYCZ7G  ingest v1: raw era5
  offset=  +5.0 -> 35904JPGDW4XK8ZMQXHG  ingest v2: bias correction applied
  offset=  -2.0 -> 4YTV515PA82MG322BE20  ingest v3: bias correction fixed
```

Those are from one run of
[`0201_reading_the_past.py`](../../icechunk/examples/0201_reading_the_past.py) and
they will be different in yours. The ids are generated per commit; nothing about
them is derived from content.

What matters is that the id is a permanent, self-sufficient handle. A caller with
an id needs nothing else — no branch name, no coordination with the writer, no
knowledge of what has happened since:

```text
And the id is all a caller needs -- no branch, no coordination with the writer:
  reopened the repo from disk, read snapshot 35904JPGDW4XK8ZMQXHG
  sizes={'time': 30, 'y': 32, 'x': 32}, mean=31.002
```

That is what makes a prediction reproducible after a reprocess: record the
snapshot id alongside the model, and "what did this train on" has an exact
answer forever, or at least until somebody expires it.

### Branch

A mutable name for a snapshot. `main` exists from creation.

```python
repo.create_branch("fix-units", snapshot_id)   # fork from ANY snapshot
repo.list_branches()                           # -> {"main", "fix-units"}
repo.lookup_branch("main")                     # -> current tip snapshot id
repo.reset_branch("main", snapshot_id)         # move a branch, atomically
repo.delete_branch("fix-units")                # remove the name
```

Creating a branch copies no data. From
[`0202_tags_and_branches.py`](../../icechunk/examples/0202_tags_and_branches.py):

```text
  create_branch('fix-units', NT862D77...)
  the new branch starts at the shipped snapshot; main stays on the broken tip:
    main         tip=ZAQJQK31  mean= 299.152
    fix-units    tip=NT862D77  mean=  28.002
  No data was copied to make fix-units -- it is a name pointing at a snapshot.
```

`reset_branch` is how a fix gets promoted. It is one atomic pointer move, so
there is no window in which `main` points at something half-built:

```text
When the fix reads correctly, move main to it. reset_branch is the single
atomic step that promotes the repaired data:
    main         tip=KTZ080G8  mean=  29.002
    fix-units    tip=KTZ080G8  mean=  29.002
```

Deleting a branch deletes the name, not the snapshots. As long as another
reference reaches them, they stay.

### Tag

An immutable name for a snapshot.

```python
repo.create_tag("era5-2024q1-final", snapshot_id)
repo.list_tags()                          # -> {"era5-2024q1-final", ...}
repo.lookup_tag("era5-2024q1-final")      # -> snapshot id
repo.delete_tag("era5-2024q1-final")
```

The immutability is enforced:

```text
  Tags are immutable by design -- pointing one at a new snapshot is an error,
  which is exactly what you want from a name you published in a model card:
    retagging raises AlreadyExistsError: tag already exists, tags are immutable: `era5-2024q1-final`
```

A tag also keeps its snapshot's chunks alive through a retention sweep, which
makes it the mechanism for "this version must survive expiry".

### Ancestry

The parent chain, newest first:

```python
for snapshot in repo.ancestry(branch="main"):
    print(snapshot.id, snapshot.parent_id, snapshot.message, snapshot.written_at)
```

`ancestry` also accepts `tag=` or `snapshot_id=` instead of `branch=`. From
[`0103_history.py`](../../icechunk/examples/0103_history.py):

```text
  history of main: (4 snapshots, newest first)
    6GASFTTD  2026-08-17 17:48:07  ingest era5 t2m 2024-03
    E1EH5E51  2026-08-17 17:48:07  ingest era5 t2m 2024-02
    SXXHCXEA  2026-08-17 17:48:07  ingest era5 t2m 2024-01
    1CECHNKR  2026-08-17 17:48:07  Repository initialized
```

The chain is explicit — each snapshot names its parent, and the root names none:

```text
    6GASFTTD  parent=E1EH5E51
    E1EH5E51  parent=SXXHCXEA
    SXXHCXEA  parent=1CECHNKR
    1CECHNKR  parent=(none)
```

Timestamps are UTC and strictly increase down the chain.

### Diff

What changed between two snapshots:

```python
diff = repo.diff(from_snapshot_id=old, to_snapshot_id=new)
diff.is_empty()
diff.new_groups, diff.new_arrays
diff.deleted_groups, diff.deleted_arrays
diff.updated_groups, diff.updated_arrays
diff.updated_chunks       # {array_path: [chunk coordinate, ...]}
```

`from_branch=` / `from_tag=` and `to_branch=` / `to_tag=` also work. The
constraint is that `from` must be an ancestor of `to`.

The most informative field is `updated_chunks`, because it names exact chunk
coordinates. From [`0203_diffing.py`](../../icechunk/examples/0203_diffing.py),
after a single-day correction:

```text
    updated_chunks    2 chunk(s) across 1 array(s)
      /t2m: [0, 0, 0], [0, 1, 0]
```

That is evidence a fix was surgical, rather than the writer's word for it.

### Conflict and rebase

Two sessions from one branch tip; the second to commit loses.

```python
try:
    session.commit("B corrects 2024-01-04")
except icechunk.ConflictError as exc:
    print(exc.expected_parent, exc.actual_parent)
    session.rebase(icechunk.ConflictDetector())
    session.commit("B corrects 2024-01-04")
```

The check is purely on the parent pointer, so it fires even when the two writers
touched completely different chunks. From
[`0303_conflicts.py`](../../icechunk/examples/0303_conflicts.py):

```text
  B commits second and raises icechunk.ConflictError:
    Failed to commit, expected parent: Some("0H4WQRH6C3HTREJRFH3G"), actual parent: Some("G5MNGX82ZW3WSCPTTSY0")
    expected_parent=0H4WQRH6  actual_parent=G5MNGX82
  the check is purely on the parent pointer, so it fires even for disjoint edits
```

Two solvers ship:

- `icechunk.ConflictDetector()` — replays and raises on any real overlap. Reports,
  never decides.
- `icechunk.BasicConflictSolver(on_chunk_conflict=icechunk.VersionSelection.UseOurs)`
  — can pick a side when two writers wrote the *same* chunk.

Neither can reconcile two appends to the same dimension, because both sides
changed the array's shape metadata and there is no correct answer. That failure
is covered in detail in the phase 3 walkthrough.

### Expiry and garbage collection

The retention policy, in two steps.

```python
expired = repo.expire_snapshots(older_than=cutoff_datetime)
summary = repo.garbage_collect(delete_object_older_than, dry_run=True)
summary = repo.garbage_collect(delete_object_older_than)
```

`expire_snapshots` rewrites the ancestry so those snapshots are no longer
reachable. It never expires the root or a branch tip. It does not delete a byte.

`garbage_collect` deletes objects that nothing reachable references. It returns a
`GCSummary` with `chunks_deleted`, `manifests_deleted`, `snapshots_deleted`, and
`bytes_deleted`.

From [`0502_expiry_and_gc.py`](../../icechunk/examples/0502_expiry_and_gc.py) —
the two-step nature is visible in the numbers:

```text
  after ingest + fixes   snapshots= 8  reachable chunk bytes=3,507,418  on disk=3,523,403
  ...
  expired 5 snapshots
  after expire           snapshots= 3  reachable chunk bytes=2,508,575  on disk=3,524,244
  ...
  after gc               snapshots= 3  reachable chunk bytes=2,508,575  on disk=2,519,615
```

Expiry moved the reachable figure and left disk alone. Garbage collection moved
disk and left reachable alone. Both are irreversible.

To measure a repository without changing it:

```python
stats = repo.chunk_storage_stats()
stats.native_bytes      # chunk data icechunk wrote
stats.virtual_bytes     # data referenced in place, in other formats
stats.inlined_bytes     # tiny arrays stored inside snapshots
```

`repo.total_chunks_storage()` still exists in icechunk 2.1.2 but emits a
`DeprecationWarning`; `chunk_storage_stats().native_bytes` is the replacement and
reports the same number.

---

## Phase 1 — Repositories, sessions, commits

Three examples that build the mental model: what a repository is on disk, what a
session is, and what the history looks like once you have committed a few times.

### 0101 — Repository basics

Source: [`icechunk/examples/0101_repo_basics.py`](../../icechunk/examples/0101_repo_basics.py)

**What it teaches.** What a repository actually is: a storage backend plus a
history. It creates one, lists the files it writes before and after the first
commit, demonstrates the difference between writable and readonly sessions, and
finishes on the fact that a session's store is an ordinary zarr store as far as
xarray is concerned.

**Why it matters.** Every OCS dataset lives at
`{data_dir}/downloads/{dataset_id}.icechunk`. That path *looks* like a directory
of data. It is not a zarr tree you can inspect with `ls` and understand — it is
a log of snapshots, manifests, and content-addressed chunks. Getting that
straight up front prevents a whole category of confusion later, and it is what
makes it obvious why appending to a served dataset is safe.

**Key code.** Creation binds a repository to a backend:

```python
repo: Any = icechunk.Repository.create(icechunk.local_filesystem_storage(str(repo_path)))
print(f"  branches at creation: {sorted(repo.list_branches())}")
```

The example walks the directory with a small helper that reports per-directory
file counts and byte totals:

```python
def walk_repo(root: Path) -> list[tuple[str, int, int]]:
    rows: list[tuple[str, int, int]] = []
    for directory in sorted(p for p in root.rglob("*") if p.is_dir()):
        files = [p for p in directory.iterdir() if p.is_file()]
        rows.append((directory.relative_to(root).as_posix(), len(files), sum(p.stat().st_size for p in files)))
    top_files = [p for p in root.iterdir() if p.is_file()]
    rows.insert(0, (".", len(top_files), sum(p.stat().st_size for p in top_files)))
    return rows
```

Then the write itself, which is the three-line cycle in full:

```python
session: Any = repo.writable_session("main")
ds = climate_dataset(days=30, ny=64, nx=64)
ds.to_zarr(session.store, mode="w", zarr_format=3, consolidated=False)
snapshot_id = session.commit("ingest 2024-01 t2m")
```

And the check that a readonly store really refuses writes:

```python
try:
    ds.to_zarr(readonly.store, mode="w", zarr_format=3, consolidated=False)
except ValueError as exc:
    print(f"  writing to the readonly store raises {type(exc).__name__}: {exc}")
```

**Real output.** Creation alone writes files:

```text
A repository is created against a storage backend. Here that backend is a
local directory; in production it is S3 or GCS with the same API.
  created era5_t2m.icechunk
  branches at creation: ['main']

Creation alone already writes files -- an empty repository is not an empty
directory. It has one snapshot: the empty root the first commit descends from.
  on disk after create:
    .                1 entries       272 bytes
    snapshots        1 entries       192 bytes
    transactions     1 entries       118 bytes
```

Three hundred and eighty-two bytes for an empty repository: a `repo` config file,
one snapshot (the root), one transaction record. There is no chunk directory yet
because there are no chunks.

The session, and the write:

```text
Writes go through a session bound to a branch. The session hands you a store;
xarray writes into it exactly as it would write a plain zarr directory.
  session.store is a IcechunkStore
  uncommitted changes before writing: False
  uncommitted changes after writing:  True
  committed as snapshot G69TFKYKQYA7XHG9J7C0
```

That snapshot id will differ in your run. `has_uncommitted_changes` flipping
`False` to `True` across the `to_zarr` call is the session tracking that
something happened, which it needs in order to know there is anything to commit.

After the commit, four new kinds of file:

```text
  on disk after commit:
    .                1 entries       425 bytes
    chunks           4 entries    892277 bytes
    manifests        2 entries       450 bytes
    overwritten      1 entries       272 bytes
    snapshots        2 entries      1153 bytes
    transactions     2 entries       349 bytes
  chunks/      the compressed array data, content-addressed
  manifests/   which chunk file holds which chunk coordinate
  snapshots/   the immutable tree of arrays and metadata per commit
  transactions/ the change log entry each commit appends
  overwritten/ superseded copies of the small repository config file
```

Four chunk files for a 30 x 64 x 64 array. xarray chose chunks of
`(15, 32, 64)`, so the grid is 2 x 2 x 1 = 4 blocks. 892 KB of chunk data
against about 983 KB uncompressed, so compression is doing modest work on
smooth-ish synthetic data. The manifests are 450 bytes total; the snapshots
1153. The bookkeeping is small compared to the data, which is the whole design
intent.

Then the point the example is really making:

```text
Note what is NOT there. A plain zarr v3 store on disk would contain
zarr.json metadata files and a t2m/ directory of chunk keys:
  any file named zarr.json? False
  any directory named t2m?  False
  every filename is an opaque id:
    1CECHNKREP0F1RSTCMT0
    1WPFJ973DPF1JSJQDDM0
    2BDGNB5G9TQWGFX680J0
    6ZW5BS18ARXYHJCSH3S0
  The zarr hierarchy exists only inside the snapshot files. You cannot
  read this store by pointing zarr at the directory -- you go through icechunk.
```

No `zarr.json`. No `t2m/`. No `c/0/0/0`. The array names, shapes, dtypes,
attributes, and the mapping from chunk coordinate to file all live *inside* the
snapshot and manifest objects. Note the first id in that list:
`1CECHNKREP0F1RSTCMT0` is the fixed root-snapshot sentinel, identical in every
icechunk repository ever created — the only id in this whole document that will
be the same in your run.

Sessions:

```text
Two kinds of session, and the difference is enforced by the store itself:
  writable_session('main'): read_only=False, branch='main'
  readonly_session('main'): read_only=True, branch=None
  the readonly session pinned snapshot G69TFKYKQYA7XHG9J7C0
  writing to the readonly store raises ValueError: Store is read-only but mode is 'w'. Create a writable store or use 'r' mode.
```

The `branch=None` on the readonly session is the detail to notice. A readonly
session has no branch, only a pinned snapshot. It resolved `main` once, at
creation, and then stopped caring what `main` does.

And the payoff:

```text
And the payoff: a readonly session's store opens like any other zarr store.
  sizes={'time': 30, 'y': 64, 'x': 64}, vars=['t2m']
  t2m chunks=(15, 32, 64), mean=26.000
  attrs={'units': 'degC', 'long_name': '2 metre temperature'}
```

`xr.open_zarr(readonly.store, consolidated=False)` and everything is there,
including the encoding and the attributes. No icechunk-aware code above the store
boundary.

**Traps.**

- **Do not point zarr at the directory.** `zarr.open_group("/data/x.icechunk")`
  will not work and cannot be made to work. The hierarchy is inside the snapshot
  objects. Everything goes through a session.
- **Do not `rsync` half a repository.** Because the objects are immutable and the
  refs are tiny, a partial copy can produce a repository whose branch points at a
  snapshot whose manifests or chunks are missing. Copy refs last, or copy the
  whole thing with a tool that finishes.
- **`Repository.create` on an existing repository raises.** Use
  `open_or_create`, or the existence check the `open_repo` helper does.
- **The chunk shape shown is xarray's default choice**, not something icechunk
  picked. It came from xarray's heuristic on an unchunked in-memory array. On a
  real ingest you pass `encoding={"t2m": {"chunks": (...)}}` on the creating
  write, because that shape is fixed for the life of the array — see 0401.
- **`overwritten/` is normal.** It holds superseded copies of the small `repo`
  config file, which is the one mutable-ish object in the layout. It is not
  garbage from a failed write.

### 0102 — Commits are atomic

Source: [`icechunk/examples/0102_commits.py`](../../icechunk/examples/0102_commits.py)

**What it teaches.** The single property everything else rests on: a write is
invisible to every other reader until `commit()` returns, and then the entire
change appears at once. It demonstrates this with an independent reader peeking
at each stage, then shows that a reader who opened *before* a commit is unmoved
by it, and finally that abandoning a session leaves nothing behind.

**Why it matters.** This is the property that lets OCS append to a dataset that
the API is simultaneously serving. Without it, every ingest is a window during
which a request can return a mixture of two revisions, silently.

**Key code.** The peek function opens a *fresh* readonly session each time, so it
is genuinely observing what an independent reader would see:

```python
def peek(repo: Any, label: str) -> None:
    session: Any = repo.readonly_session("main")
    try:
        ds = xr.open_zarr(session.store, consolidated=False)
    except zarr.errors.GroupNotFoundError:
        print(f"  {label}: nothing -- xarray raises GroupNotFoundError, the store looks empty")
        return
    print(f"  {label}: time={ds.sizes['time']}, t2m mean={float(ds.t2m.mean()):.3f}")
```

The first round writes without committing, peeks, then commits and peeks again:

```python
writer: Any = repo.writable_session("main")
climate_dataset(days=30, ny=32, nx=32, offset=0.0).to_zarr(
    writer.store, mode="w", zarr_format=3, consolidated=False
)
peek(repo, "reader after the write  ")
first_id = writer.commit("ingest 2024-01, offset 0.0")
peek(repo, "reader after the commit ")
```

The second round holds a reader open across a commit:

```python
held: Any = repo.readonly_session("main")
held_ds = xr.open_zarr(held.store, consolidated=False)
writer2: Any = repo.writable_session("main")
climate_dataset(days=30, ny=32, nx=32, offset=10.0).to_zarr(
    writer2.store, mode="w", zarr_format=3, consolidated=False
)
second_id = writer2.commit("reprocess 2024-01, offset 10.0")
print(f"  held reader STILL reads mean={float(held_ds.t2m.mean()):.3f}")
```

And the third abandons a write:

```python
doomed: Any = repo.writable_session("main")
climate_dataset(days=30, ny=32, nx=32, offset=-99.0).to_zarr(
    doomed.store, mode="w", zarr_format=3, consolidated=False
)
doomed.discard_changes()
```

**Real output.** The first round:

```text
  reader before the write : nothing -- xarray raises GroupNotFoundError, the store looks empty
  writer has written data: has_uncommitted_changes=True
  reader after the write  : nothing -- xarray raises GroupNotFoundError, the store looks empty
  The bytes are on disk. The reader still cannot see them, because no
  snapshot points at them yet.

Now commit -> snapshot FCG51Q80KG62RX3MAM90
  reader after the commit : time=30, t2m mean=26.002
  The whole 30-day dataset appeared in one step. There was no moment where
  a reader could have seen 10 days, or one array but not the other.
```

Read that middle line carefully. The writer has written 30 days of data. The
chunk objects are on disk — you could find them with `find`. And a completely
independent reader, opening a fresh session at that exact moment, sees an empty
store. Not a partial store: an empty one, indistinguishable from before the write
started. There is no snapshot referencing those chunks, so from the reader's
point of view they do not exist.

The second round, with a reader held open:

```text
  reader opened at snapshot FCG51Q80KG62RX3MAM90, mean=26.002
  writer2 has rewritten all chunks, not yet committed.
  held reader still reads mean=26.002  (unchanged)
  a brand new reader too   : time=30, t2m mean=26.002

Commit -> snapshot 0KTT1WCWW24D9EKY5V20
  held reader STILL reads mean=26.002
  It is pinned to the snapshot it opened at; a commit cannot move it.
  a new reader now sees    : time=30, t2m mean=36.002
```

Offset 0 gives 26.002, offset +10 gives 36.002. The reader opened before the
commit still sees 26.002 *after* the commit; a reader opened after sees 36.002.
Two readers, same repository, same instant, different and each internally
coherent views. That is snapshot isolation, and 0302 takes it further.

The abandoned write:

```text
And the failure path: write into a session, then never commit it.
  doomed session has uncommitted changes: True
  after discard_changes():                False
  reader is untouched      : time=30, t2m mean=36.002
  A crash instead of discard_changes() has the same effect: the branch tip
  never moved, so the orphaned chunks are simply unreachable.
```

The offset was -99, which would have produced a mean near -73. No reader ever
sees it, and no reader ever could have.

Finally, both committed versions remain readable:

```text
Two commits landed, and both are permanent points in the history:
  0KTT1WCW  reprocess 2024-01, offset 10.0
  FCG51Q80  ingest 2024-01, offset 0.0
  1CECHNKR  Repository initialized

Both are still readable, which is what the next examples build on:
  first  FCG51Q80: mean=26.002
  second 0KTT1WCW: mean=36.002
```

**Traps.**

- **A `GroupNotFoundError` from a fresh repository is expected, not a bug.** A
  repository with a root snapshot and nothing else genuinely has no zarr group.
  Code that opens a possibly-empty repository has to handle it — the
  `committed_periods` helper in 0402 does exactly that.
- **`discard_changes()` does not reclaim disk.** The chunk objects the doomed
  session wrote stay on disk as orphans. They are unreachable and invisible, and
  `garbage_collect` removes them (0502). A long-running process that abandons
  many writes will accumulate them.
- **A held reader is stale by design.** "The reader still reads 26.002" is the
  feature, but it means a long-lived session serves increasingly old data. To see
  new commits you must open a new session. Staleness is opt-out, not accidental.
- **`has_uncommitted_changes` is about the session, not the branch.** It says
  nothing about whether someone else committed while you were writing. That is
  what `ConflictError` is for.

### 0103 — History

Source: [`icechunk/examples/0103_history.py`](../../icechunk/examples/0103_history.py)

**What it teaches.** How to walk a branch's ancestry, what each snapshot carries,
where the chain terminates, and — the load-bearing claim — that adding commits
never alters the ones already there.

**Why it matters.** An OCS store accumulates one commit per ingested period, so
the ancestry chain *is* the audit trail: which periods arrived, when, in what
order, and under what message. Because it is genuinely append-only rather than
append-mostly, "what did the API serve last Tuesday" has an exact answer instead
of an estimate.

**Key code.** The ingest loop is the OCS shape in miniature — the first period
creates the arrays, the rest append:

```python
periods = (
    ("2024-01-01", 31, "ingest era5 t2m 2024-01"),
    ("2024-02-01", 29, "ingest era5 t2m 2024-02"),
    ("2024-03-01", 31, "ingest era5 t2m 2024-03"),
)
for index, (start, days, message) in enumerate(periods):
    ds = climate_dataset(days=days, ny=32, nx=32, start=start, seed=index)
    append_dim = None if index == 0 else "time"
    snapshot_id = write_dataset(repo, ds, message, append_dim=append_dim)
```

Walking the parent links directly:

```python
for snapshot in repo.ancestry(branch="main"):
    parent = str(snapshot.parent_id)[:8] if snapshot.parent_id else "(none)"
    print(f"    {str(snapshot.id)[:8]}  parent={parent}")
```

And the append-only check, which captures ids, commits, and compares:

```python
before = [row["id"] for row in describe_history(repo)]
fourth = write_dataset(repo, climate_dataset(...), "ingest era5 t2m 2024-04", append_dim="time")
after = [row["id"] for row in describe_history(repo)]
print(f"  every previously seen id is still present, in order: {after[1:] == before}")
```

**Real output.** A fresh repository already has a row:

```text
Before anything is written, the history is not empty. Creating a repository
makes a root snapshot so that the first real commit has a parent.
  history of a fresh repository: (1 snapshot, newest first)
    1CECHNKR  2026-08-17 17:48:07  Repository initialized
```

After three periods:

```text
  history of main: (4 snapshots, newest first)
    6GASFTTD  2026-08-17 17:48:07  ingest era5 t2m 2024-03
    E1EH5E51  2026-08-17 17:48:07  ingest era5 t2m 2024-02
    SXXHCXEA  2026-08-17 17:48:07  ingest era5 t2m 2024-01
    1CECHNKR  2026-08-17 17:48:07  Repository initialized
```

All four timestamps show the same second because the whole example runs in well
under a second on this machine — machine-dependent, and not a claim that the
commits were simultaneous. The finer resolution is available:

```text
  Timestamps are UTC and strictly increase down the chain (newest first):
    newest 2026-08-17T17:48:07.702382+00:00
    oldest 2026-08-17T17:48:07.483148+00:00
    monotonic: True
```

219 milliseconds for repository creation plus three ingests, on this machine.

The root sentinel:

```text
  The last row is the root: 'Repository initialized'. Its id is a fixed
  sentinel, the same in every icechunk repository ever created:
    1CECHNKREP0F1RSTCMT0
  Everything else descends from it, so a full walk always terminates there.
```

The explicit parent chain:

```text
    6GASFTTD  parent=E1EH5E51
    E1EH5E51  parent=SXXHCXEA
    SXXHCXEA  parent=1CECHNKR
    1CECHNKR  parent=(none)
```

And the append-only verification:

```text
Append-only means a new commit adds a row and touches nothing above it.
Capture the ids, commit a fourth period, and compare.
  before: 4 snapshots, after: 5 snapshots
  the new tip is S195D1W1, prepended to the walk
  every previously seen id is still present, in order: True
  Nothing was rewritten. There is no icechunk operation that edits a
  committed snapshot -- a correction is another commit on top.
```

`after[1:] == before` compares full 20-character ids, in order, not just the
count. The list is genuinely prepended to.

Four periods stacked:

```text
  And the tip holds all four periods stacked along time:
    time=121 steps, 2024-01-01 .. 2024-04-30
```

31 + 29 + 31 + 30 = 121. February has 29 days because 2024 is a leap year, which
is the detail that makes appending calendar months against a fixed time chunk
awkward — see 0401.

**Traps.**

- **"Append-only" has an exception, and it is deliberate.** `expire_snapshots`
  removes rows from the chain. It is the only thing that does, it is irreversible,
  and 0502 covers it. Nothing else can edit a committed snapshot.
- **`ancestry` is lazy and does I/O per step.** On a repository with thousands of
  commits, materialising the whole chain to count it is thousands of fetches.
  Take a slice: `list(itertools.islice(repo.ancestry(branch="main"), 20))`.
- **Ancestry is per-reference.** `repo.ancestry(branch="main")` walks main's
  chain. A snapshot on another branch that main never descended from will not
  appear, however recent. 0202 shows two branches sharing an ancestor and
  diverging above it.
- **Commit messages are the audit trail, so write them like one.** "update" tells
  you nothing in six months. `ingest era5 t2m 2024-03` tells you everything. The
  `metadata=` argument to `commit()` takes structured data if you want machine-
  readable provenance alongside the prose.
- **Timestamps come from the writer's clock.** They are UTC and monotonic within
  one process. Across machines with skewed clocks, monotonicity is a hope rather
  than a guarantee — worth knowing before writing a retention policy that keys
  off `written_at`.

---

## Phase 2 — Time travel

Phase 1 established that history accumulates. Phase 2 is about using it: reading
any past state, naming the states worth naming, and asking the store what
actually changed between two of them.

### 0201 — Reading the past

Source: [`icechunk/examples/0201_reading_the_past.py`](../../icechunk/examples/0201_reading_the_past.py)

**What it teaches.** That a snapshot id is a permanent handle on a dataset
version, and that later writes are provably incapable of disturbing it. The
example writes five revisions of the same grid, each shifted by a known offset,
and then verifies element-by-element that the first two are unchanged after three
more commits landed on top.

**Why it matters.** When an OCS dataset is reprocessed, two questions stay
answerable: "what did this model actually train on" and "what did the API return
in March". A snapshot id is quotable — you can put it in a model card, a report
footer, or a database row — and it keeps meaning the same thing after the store
has moved on.

**Key code.** The offsets are the whole experiment design: a single number, the
mean, identifies which revision you are looking at.

```python
revisions = (
    (0.0, "ingest v1: raw era5"),
    (5.0, "ingest v2: bias correction applied"),
    (-2.0, "ingest v3: bias correction fixed"),
)
ids: dict[str, str] = {}
for offset, message in revisions:
    ds = climate_dataset(days=30, ny=32, nx=32, offset=offset, seed=0)
    snapshot_id = write_dataset(repo, ds, message)
    ids[message] = snapshot_id
```

Reading one back is a single call:

```python
def summarize(repo: Any, snapshot_id: str) -> tuple[float, float]:
    ds = read_dataset(repo, snapshot_id=snapshot_id)
    return float(ds.t2m.mean()), float(ds.t2m.isel(time=0, y=0, x=0))
```

The load-bearing part is the array comparison, not the means:

```python
before_v1 = read_dataset(repo, snapshot_id=ids["ingest v1: raw era5"]).t2m.values.copy()
before_v2 = read_dataset(repo, snapshot_id=ids["ingest v2: bias correction applied"]).t2m.values.copy()

for offset, message in ((100.0, "ingest v4: bad units, degK"), (0.5, "ingest v5: reverted to degC")):
    snapshot_id = write_dataset(repo, climate_dataset(days=30, ny=32, nx=32, offset=offset, seed=0), message)

after_v1 = read_dataset(repo, snapshot_id=ids["ingest v1: raw era5"]).t2m.values
print(f"  v1 identical after two more commits: {bool(np.array_equal(before_v1, after_v1))}")
```

`np.array_equal` over the full 30 x 32 x 32 array, not a summary statistic. That
distinction matters: two arrays can have the same mean and differ everywhere.

**Real output.** Three revisions, three ids:

```text
  offset=  +0.0 -> VCFTZ4H1X2YMAHXYCZ7G  ingest v1: raw era5
  offset=  +5.0 -> 35904JPGDW4XK8ZMQXHG  ingest v2: bias correction applied
  offset=  -2.0 -> 4YTV515PA82MG322BE20  ingest v3: bias correction fixed
```

Ids differ per run. Reading each one back:

```text
  VCFTZ4H1  mean= 26.002  t2m[0,0,0]= 28.063  ingest v1: raw era5
  35904JPG  mean= 31.002  t2m[0,0,0]= 33.063  ingest v2: bias correction applied
  4YTV515P  mean= 24.002  t2m[0,0,0]= 26.063  ingest v3: bias correction fixed
```

Both the mean and a single element track the offsets exactly, which rules out an
averaging artefact. The differences are checked explicitly:

```text
  v2 - v1 = +5.000  (expected +5.000)
  v3 - v1 = -2.000  (expected -2.000)
```

Then the claim the example exists to make:

```text
  wrote RB2W553B  ingest v4: bad units, degK
  wrote Q606YA51  ingest v5: reverted to degC
  v1 identical after two more commits: True
  v2 identical after two more commits: True
  max |difference| at v1: 0.0e+00
```

Exactly zero, not "small". This is not floating-point tolerance — the old
snapshot points at the same chunk objects it always pointed at, and those objects
were never opened for writing. There is no mechanism by which they could drift.

The recovery narrative:

```text
This is what makes a bad ingest survivable. v4 shipped Kelvin by mistake;
the correct data was never lost, it just stopped being the tip:
  VCFTZ4H1  mean=  26.002  ingest v1: raw era5
  35904JPG  mean=  31.002  ingest v2: bias correction applied
  4YTV515P  mean=  24.002  ingest v3: bias correction fixed
  RB2W553B  mean= 126.002  ingest v4: bad units, degK
  Q606YA51  mean=  26.502  ingest v5: reverted to degC
  A model trained against the v2 id keeps reading v2 forever, even though
  main has moved on twice since.
```

v4 at 126.002 is the deliberately-broken revision. It is still there, still
readable, and still labelled. Nothing had to be restored from a backup.

And the id needs no context:

```text
  reopened the repo from disk, read snapshot 35904JPGDW4XK8ZMQXHG
  sizes={'time': 30, 'y': 32, 'x': 32}, mean=31.002
```

A fresh `Repository.open` and a snapshot id was sufficient. No branch, no tag, no
coordination with whoever wrote it.

**Traps.**

- **The permanence is not unconditional.** `expire_snapshots` can remove a
  snapshot, at which point reading its id raises `SnapshotNotFoundError` —
  0502 shows exactly that. If an id must survive a retention sweep, tag it.
- **A snapshot id is opaque and carries no ordering.** You cannot tell from two
  ids which came first, or whether one is an ancestor of the other. Use
  `ancestry` for that.
- **`mode="w"` on every revision, in this example, is deliberate and not
  advisable in general.** Five full rewrites of the same grid is exactly the
  pattern 0501 measures as expensive and 0203 describes as unauditable. It is
  used here because it makes five clearly distinct versions in the fewest lines.
- **Record the id at the point of use.** An id in a log file that nobody reads is
  not provenance. It belongs next to whatever consumed the data — the model
  artefact, the report, the row in the results table.

### 0202 — Tags and branches

Source: [`icechunk/examples/0202_tags_and_branches.py`](../../icechunk/examples/0202_tags_and_branches.py)

**What it teaches.** The two naming mechanisms and their different mutability.
A tag is an immutable human name for a snapshot; a branch is a mutable one that
moves as you commit. The example uses them together to run the standard repair
workflow: branch from the last good snapshot, fix on the branch, promote onto
main when it reads correctly.

**Why it matters.** Nobody quotes `35904JPGDW4XK8ZMQXHG` in a model card. A tag
turns a point in an OCS store into `era5-2024q1-final`, a name that survives
reprocessing and means the same thing forever. And a branch lets you rebuild a
suspect period from before the mistake *without taking the served dataset
offline* — main keeps serving what it was serving until you decide to move it.

**Key code.** Three revisions, the third deliberately in Kelvin:

```python
good = write_dataset(repo, climate_dataset(days=30, ny=32, nx=32, offset=0.0), "ingest era5 2024-q1 raw")
shipped = write_dataset(repo, climate_dataset(days=30, ny=32, nx=32, offset=2.0),
                        "ingest era5 2024-q1 bias corrected")
broken = write_dataset(repo, climate_dataset(days=30, ny=32, nx=32, offset=273.15),
                       "reprocess era5 2024-q1 (bad units)")
```

Tagging, and the immutability check:

```python
repo.create_tag("era5-2024q1-final", shipped)
by_tag = read_dataset(repo, tag="era5-2024q1-final")
try:
    repo.create_tag("era5-2024q1-final", broken)
except icechunk.AlreadyExistsError as exc:
    print(f"    retagging raises {type(exc).__name__}: {str(exc).splitlines()[0]}")
```

Branching from an older snapshot — note that `create_branch` takes any snapshot
id, not only a tip:

```python
repo.create_branch("fix-units", shipped)
fixed = write_dataset(
    repo,
    climate_dataset(days=30, ny=32, nx=32, offset=3.0),
    "reprocess era5 2024-q1 (degC, corrected)",
    branch="fix-units",
)
```

Promotion:

```python
repo.reset_branch("main", fixed)
repo.create_tag("era5-2024q1-v2", fixed)
repo.delete_branch("fix-units")
```

**Real output.** The three revisions, with 299.152 marking the Kelvin mistake:

```text
  raw     GB6CQZXW  mean=26.002
  shipped NT862D77  mean=28.002
  broken  ZAQJQK31  mean=299.152
```

Tagging and reading by tag:

```text
  create_tag('era5-2024q1-final', NT862D77...)
  read_dataset(repo, tag='era5-2024q1-final') -> mean=28.002
  which resolves to the same snapshot: True
```

The immutability, enforced rather than documented:

```text
    retagging raises AlreadyExistsError: tag already exists, tags are immutable: `era5-2024q1-final`
```

This is stricter than git, where `git tag -f` will happily repoint. For a name
you published, strictness is correct: silently repointing a tag means every
citation of it now means something different, with no record of the change.

Branching:

```text
  create_branch('fix-units', NT862D77...)
  the new branch starts at the shipped snapshot; main stays on the broken tip:
    main         tip=ZAQJQK31  mean= 299.152
    fix-units    tip=NT862D77  mean=  28.002
  No data was copied to make fix-units -- it is a name pointing at a snapshot.
```

Writing on the branch:

```text
  wrote KTZ080G8 on fix-units
    main         tip=ZAQJQK31  mean= 299.152
    fix-units    tip=KTZ080G8  mean=  29.002
  main did not move. Anything serving from main is still serving what it
  was serving before -- wrong, but stable and unaffected by the repair work.
```

That is the operationally important sentence. During the repair, the service is
serving wrong data — but it is serving *consistently* wrong data, at full speed,
with no outage. You choose when to switch.

The divergence, printed as two chains:

```text
    main         ZAQJQK31 <- NT862D77 <- GB6CQZXW <- 1CECHNKR
    fix-units    KTZ080G8 <- NT862D77 <- GB6CQZXW <- 1CECHNKR
  their shared ancestor is NT862D77, the snapshot fix-units forked from
```

Both chains run back through `NT862D77` and end at the sentinel root. The tag
pointing at `NT862D77` is unmoved by any of it.

Promotion, in one step:

```text
    main         tip=KTZ080G8  mean=  29.002
    fix-units    tip=KTZ080G8  mean=  29.002
  tagged the promoted snapshot era5-2024q1-v2
```

`reset_branch` is atomic. There is no moment at which `main` points at something
partly built, and every reader either sees the old tip or the new one.

The two namespaces:

```text
  branches: ['fix-units', 'main']
  tag era5-2024q1-final    -> NT862D77  mean=  28.002
  tag era5-2024q1-raw      -> GB6CQZXW  mean=  26.002
  tag era5-2024q1-v2       -> KTZ080G8  mean=  29.002
```

And cleanup:

```text
  Cleaning up the now-redundant branch: list_branches() -> ['main']
  Deleting the branch does not delete its snapshots; the history main now
  points at still runs through KTZ080G8.
```

**Traps.**

- **`reset_branch` is `git reset --hard`, not `git merge`.** It moves the pointer,
  discarding nothing but also merging nothing. If main had received commits while
  you were fixing on the branch, those commits are no longer on main after the
  reset. They are still reachable by id, but nothing points at them. Pass
  `from_snapshot_id=` to make the reset conditional on main not having moved.
- **A deleted branch's snapshots become expiry candidates.** They stay readable
  by id, but nothing references them, so a retention sweep will take them.
  Tag anything that must survive.
- **Tags are immutable but not indestructible.** `delete_tag` exists, and
  `expire_snapshots(delete_expired_tags=True)` will remove tags pointing at
  expired snapshots. Immutable means "cannot be repointed", not "cannot be
  removed".
- **Branching is cheap; branch proliferation is not free.** Every branch is a
  root for reachability, so a forgotten branch pins every chunk in its history
  against garbage collection indefinitely.
- **Tag names are a public interface.** Pick a scheme and stick to it. The example
  uses `{dataset}-{period}-{qualifier}`, which sorts usefully and reads
  unambiguously in a citation.

### 0203 — Diffing

Source: [`icechunk/examples/0203_diffing.py`](../../icechunk/examples/0203_diffing.py)

**What it teaches.** How to ask the store what changed between two snapshots, and
what the answer looks like for five genuinely different kinds of change: a
from-nothing ingest, an append, a surgical region write, a metadata-only edit, and
a full overwrite. Plus the one structural constraint on which pairs can be
compared.

**Why it matters.** Two questions come up constantly against a live store. "What
did last night's ingest actually add?" and "the fix for that bad day — did it
touch only that day?" A diff answers both *from the store itself*, rather than
from a log the writer had to remember to emit. Chunk coordinates are precise
enough to falsify a claim that a fix was surgical.

**Key code.** The diff call and the fields worth printing:

```python
def show_diff(repo: Any, from_id: str, to_id: str, label: str) -> None:
    diff: Any = repo.diff(from_snapshot_id=from_id, to_snapshot_id=to_id)
    chunk_total = sum(len(coords) for coords in diff.updated_chunks.values())
    print(f"    is_empty()        {diff.is_empty()}")
    print(f"    new_arrays        {sorted(diff.new_arrays)}")
    print(f"    deleted_arrays    {sorted(diff.deleted_arrays)}")
    print(f"    updated_groups    {sorted(diff.updated_groups)}   (metadata/attrs)")
    print(f"    updated_arrays    {sorted(diff.updated_arrays)}   (shape/dtype/attrs)")
    print(f"    updated_chunks    {chunk_total} chunk(s) across {len(diff.updated_chunks)} array(s)")
    for array, coords in sorted(diff.updated_chunks.items()):
        print(f"      {array}: {', '.join(str(c) for c in coords[:6])}")
```

The surgical correction is a `region=` write, which is the important pattern:

```python
session: Any = repo.writable_session("main")
patch = read_dataset(repo).isel(time=slice(0, 1)).load()
patch["t2m"] = patch.t2m + 1.5
patch.drop_vars("time").to_zarr(session.store, region={"time": slice(0, 1)}, consolidated=False)
third = session.commit("correct 2024-01-01")
```

Note `drop_vars("time")` — a region write must not rewrite the coordinate it is
indexing by, and xarray objects if you try.

The metadata-only change goes through zarr directly, bypassing xarray:

```python
meta_session: Any = repo.writable_session("main")
group = zarr.open_group(meta_session.store, mode="a")
group.attrs["source"] = "era5-land"
group.attrs["ingest_version"] = "2.1"
fourth = meta_session.commit("record provenance attrs")
```

**Real output.** The first ingest, diffed against the empty root:

```text
  first ingest: 30 days on a 64x64 grid
    1CECHNKR -> FFABNPQN
    is_empty()        False
    new_groups        ['/']
    new_arrays        ['/t2m', '/time']
    deleted_groups    []
    deleted_arrays    []
    updated_groups    []   (metadata/attrs)
    updated_arrays    []   (shape/dtype/attrs)
    updated_chunks    5 chunk(s) across 2 array(s)
      /t2m: [0, 0, 0], [0, 1, 0], [1, 0, 0], [1, 1, 0]
      /time: [0]
```

Everything is new; nothing is updated. `/time` gets its own chunk, which is worth
noticing — coordinates are arrays too, and they show up in diffs.

The append:

```text
  append 29 days along time
    FFABNPQN -> ZVNV3CMN
    new_arrays        []
    updated_arrays    ['/t2m', '/time']   (shape/dtype/attrs)
    updated_chunks    5 chunk(s) across 2 array(s)
      /t2m: [2, 0, 0], [2, 1, 0], [3, 0, 0], [3, 1, 0]
      /time: [1]
```

(Trimmed: the empty fields are omitted here.) Both arrays are *updated* because
their shape grew, and the chunk coordinates are `[2, ...]` and `[3, ...]` — the
January chunks at `[0, ...]` and `[1, ...]` do not appear at all. They were not
rewritten; they are shared with the previous snapshot. That is the answer to
"what did last night add", stated in a way that cannot be wrong.

The surgical correction:

```text
  region write over time step 0
    ZVNV3CMN -> 8BJK5JW7
    new_arrays        []
    deleted_arrays    []
    updated_arrays    []   (shape/dtype/attrs)
    updated_chunks    2 chunk(s) across 1 array(s)
      /t2m: [0, 0, 0], [0, 1, 0]
```

Two chunks, one array. `/time` is absent — the coordinate was not touched. No
array definition changed, so `updated_arrays` is empty: the shape is the same, and
only values moved. This is what "surgical" looks like as evidence.

The example is honest about the resolution limit:

```text
    Only t2m changed, and only 2 of its chunks. With chunks=(15, 32, 64) the grid
    is split in two along y, so day 0 lives in chunks [0, 0, 0] and [0, 1, 0].
    The time array is untouched, no other array moved, and no chunk outside
    time-chunk 0 was rewritten: the fix was surgical, and this is the evidence
    rather than the writer's word for it. Note the honest caveat -- chunking is
    the resolution of the audit, so those 2 chunks also carry days 1..14.
```

You can prove a fix did not touch days 15 onward. You cannot prove from the diff
alone that it did not touch days 1 to 14, because those live in the same chunks.
Chunk size is the granularity of the audit trail, which is a consideration when
choosing it.

The metadata-only edit:

```text
  set two attrs on the root group
    8BJK5JW7 -> S7H26FX4
    updated_groups    ['/']   (metadata/attrs)
    updated_arrays    []   (shape/dtype/attrs)
    updated_chunks    0 chunk(s) across 0 array(s)
```

Zero chunks. Proof that adding provenance attributes did not silently rewrite
anything.

And the case to avoid:

```text
  mode='w' over the whole store
    S7H26FX4 -> AQQ5SYS4
    new_groups        ['/']
    new_arrays        ['/t2m', '/time']
    deleted_groups    ['/']
    deleted_arrays    ['/t2m', '/time']
    updated_chunks    9 chunk(s) across 2 array(s)
      /t2m: [0, 0, 0], [0, 0, 1], [0, 1, 0], [0, 1, 1], [1, 0, 0], [1, 0, 1], ...
```

The same paths appear under both `deleted_arrays` and `new_arrays`. The diff is
honest — that really is what `mode="w"` does — but it conveys nothing about which
values changed. And the attributes are gone:

```text
      attrs at S7H26FX4: {'source': 'era5-land', 'ingest_version': '2.1'}
      attrs at AQQ5SYS4: {}
```

A full overwrite silently discards the root attributes, because it recreated the
group from scratch. On a GeoZarr dataset, that is the CRS and the grid mapping
going missing.

Diffs span any distance:

```text
  everything between the first ingest and the attrs commit
    FFABNPQN -> S7H26FX4
    updated_groups    ['/']   (metadata/attrs)
    updated_arrays    ['/t2m', '/time']   (shape/dtype/attrs)
    updated_chunks    7 chunk(s) across 2 array(s)
      /t2m: [0, 0, 0], [0, 1, 0], [2, 0, 0], [2, 1, 0], [3, 0, 0], [3, 1, 0]
      /time: [1]
```

Three commits collapsed into one answer: the union of what they touched. Note
that the chunk list is the *set* of chunks written across the range, not a count
of writes — `[0, 0, 0]` appears once even though it was written by the correction.

The constraint:

```text
  backwards, newer -> older  raises InvalidInputError: session error: `to` snapshot ancestry doesn't include `from`
  a snapshot against itself  raises InvalidInputError: session error: `to` snapshot ancestry doesn't include `from`
  So a diff always reads as 'what happened between then and now', never as
  'how do these two branches differ' -- for that, diff each from their fork.
```

Both the backwards case and the self-comparison raise the same error, which
confirms the implementation is a strict ancestry walk: `to`'s ancestry must
*include* `from`, and a snapshot's ancestry does not include itself.

**Traps.**

- **`diff` is directional and will not tell you.** It raises rather than
  silently returning something reversed. If you want branch-versus-branch, find
  the fork point and diff each side from it.
- **A snapshot does not diff against itself.** If you compute a diff over a range
  that might be empty, handle `InvalidInputError`.
- **`updated_chunks` is chunk-resolution, not element-resolution.** The audit is
  only as fine as your chunking.
- **`mode="w"` destroys root attributes.** For anything with GeoZarr metadata or
  provenance attributes, prefer `append_dim=` or `region=`. If you must rewrite,
  re-apply the attributes in the same commit.
- **A region write must drop the coordinate it indexes by.** `patch.drop_vars("time")`
  in the code above is not optional.
- **`is_empty()` is not the same as "nothing happened".** A commit that only set
  an attribute reports `is_empty() == False` with zero chunks, which is correct
  and occasionally surprising.

---

## Phase 3 — Transactions and safety

The three properties that make icechunk different from a directory of chunk
files. Phase 1 and 2 showed the model; phase 3 shows what it protects you from,
with the failure it prevents demonstrated side by side.

### 0301 — Atomicity

Source: [`icechunk/examples/0301_atomicity.py`](../../icechunk/examples/0301_atomicity.py)

**What it teaches.** That a write which fails before `commit()` leaves the
repository byte-identical to how it started, as far as any reader can tell. And,
crucially, the same crash against a plain zarr directory, so the contrast is
demonstrated rather than asserted.

**Why it matters.** OCS appends to datasets that are simultaneously being served
over HTTP. Atomicity is the property that makes that safe: an ingest that dies
mid-flight cannot leave a client reading a store where January is the new
revision and February is still the old one.

**Key code.** The failing icechunk write. Note that the exception is raised
*after* the data reaches the session, so the session is genuinely dirty:

```python
def crashing_icechunk_write(repo: Any, path: Path) -> None:
    before_bytes = directory_size(path)
    session = repo.writable_session("main")
    try:
        revision = climate_dataset(days=10, ny=16, nx=16, start="2024-01-11", offset=100.0)
        revision.to_zarr(session.store, append_dim="time", consolidated=False)
        print(f"  wrote 10 more steps into the session: has_uncommitted_changes={session.has_uncommitted_changes}")
        raise RuntimeError("upstream API returned 503 halfway through the ingest")
    except RuntimeError as exc:
        print(f"  ingest raised: {exc}")
    print(f"  on-disk bytes before={before_bytes}, after={directory_size(path)} (chunk objects exist but are orphaned)")
```

There is no `discard_changes()` and no cleanup. The session is simply abandoned,
which is what a crashed process does.

The plain-zarr comparison writes one time step at a time and dies at step 5:

```python
def crashing_plain_zarr_write(path: Path) -> None:
    revision = climate_dataset(days=10, ny=16, nx=16, offset=100.0)
    try:
        for i in range(10):
            if i == 5:
                raise RuntimeError("upstream API returned 503 halfway through the ingest")
            step = revision.isel(time=slice(i, i + 1)).drop_vars("time")
            step.to_zarr(path, region={"time": slice(i, i + 1)}, consolidated=False)
    except RuntimeError as exc:
        print(f"  ingest raised: {exc}")
```

The store is created with one chunk per time step, so each iteration is exactly
one chunk write — the cleanest possible model of an interrupted multi-chunk
update:

```python
baseline.to_zarr(
    zarr_path, mode="w", zarr_format=3, consolidated=False, encoding={"t2m": {"chunks": (1, 16, 16)}}
)
```

**Real output.** The baseline:

```text
A repository with one committed snapshot: 10 days of t2m around 26 degC.
  snapshot 657J8RG1: time=10, step means=[26.0, 26.0, 26.0, 25.9, 26.0, 26.0, 26.0, 26.0, 26.0, 25.9]
```

The failed write:

```text
Now an ingest appends 10 more steps at +100 degC and dies before commit():
  wrote 10 more steps into the session: has_uncommitted_changes=True
  ingest raised: upstream API returned 503 halfway through the ingest
  the session is abandoned without commit() -- no snapshot was created
  on-disk bytes before=21255, after=39534 (chunk objects exist but are orphaned)
```

The on-disk size nearly doubled. The chunk objects for those ten +100 degC steps
really were written; they are sitting there. What did not happen is any snapshot
referencing them, so they are unreachable.

What a reader sees:

```text
What a reader sees after the failure -- open the branch tip again:
  time=10, step means=[26.0, 26.0, 26.0, 25.9, 26.0, 26.0, 26.0, 26.0, 26.0, 25.9]
  identical to before: True
  no partial data: max value is 29.4 degC, not 126
  history is still 2 snapshots deep, tip is still 657J8RG1:
    657J8RG1  ingest 2024-01-01..10
    1CECHNKR  Repository initialized
```

Ten time steps, not twenty. Maximum value 29.4 degC, so not a single element of
the +100 revision leaked in. The history did not grow. The branch tip did not
move. From the reader's side, the failed ingest is not merely harmless — it is
undetectable.

Then the same crash against plain zarr:

```text
The same crash against a plain zarr directory (one chunk per time step):
  before: step means=[26.0, 26.0, 26.0, 25.9, 26.0, 26.0, 26.0, 26.0, 26.0, 25.9]
  ingest raised: upstream API returned 503 halfway through the ingest
  after:  step means=[126.0, 126.0, 126.0, 125.9, 126.0, 26.0, 26.0, 26.0, 26.0, 25.9]
  steps 0-4 are the new revision, steps 5-9 are the old one -- a store no reader should see
```

That array is the whole argument. Five days at 126, five days at 26, in one
variable, on one time axis. It is not corrupt in any technical sense — every
chunk is valid, the metadata is consistent, nothing will raise on read. It is
simply, permanently, wrong, and there is nothing on disk that records that fact.
Every downstream computation from this point silently incorporates it.

The mechanism, stated plainly:

```text
Why this is the property that matters for a served dataset:
  a zarr chunk write is one PUT per chunk; there is no boundary around the set
  icechunk buffers chunk writes, then makes exactly one atomic reference update at commit()
  until that update lands, every reader resolves the branch to the previous snapshot
```

**Traps.**

- **Atomicity is not free of storage cost.** The orphaned chunks from a failed
  write stay on disk. In this run, 21,255 bytes became 39,534. A job that retries
  a large ingest repeatedly can accumulate a lot of unreachable data before
  anything reclaims it. `garbage_collect` is the reclaim, and it is not automatic.
- **Orphans are invisible to every measurement that matters.**
  `chunk_storage_stats().native_bytes` reports *reachable* bytes, so it will not
  show them. Only the on-disk size will, which is why 0301 and 0501 both measure
  both numbers.
- **Atomicity is per-commit, not per-job.** An ingest loop that commits once per
  period, interrupted after three periods, leaves three committed periods. That
  is intentional and is what makes 0402's resume work — but it means "the job
  failed" and "nothing was written" are different statements.
- **The plain-zarr failure mode is silent on read.** No exception, no warning, no
  flag. If you are migrating an existing zarr-based pipeline, assume this has
  already happened at least once and that nobody noticed.
- **`discard_changes()` and a crash are equivalent from the repository's point of
  view.** Neither moves the branch tip. The former is tidier only in that it lets
  the session object be reused.

### 0302 — Isolation

Source: [`icechunk/examples/0302_isolation.py`](../../icechunk/examples/0302_isolation.py)

**What it teaches.** That a readonly session resolves the branch to a snapshot id
exactly once, at creation, and then never again. Two commits land underneath an
open reader and it does not notice either of them.

**Why it matters.** OCS serves requests out of the same repositories its ingest
jobs append to. A request that started before an ingest finished must return a
coherent dataset, not a mixture. Snapshot isolation gives that for free, with no
locks and no coordination between the reader and the writer.

**Key code.** The reader pins once:

```python
reader: Any = repo.readonly_session("main")
print(f"  reader.snapshot_id = {str(reader.snapshot_id)[:8]}  read_only={reader.read_only}")
reader_view = xr.open_zarr(reader.store, consolidated=False)
```

Then two commits land, using the helper:

```python
v2 = write_dataset(
    repo,
    climate_dataset(days=10, ny=16, nx=16, start="2024-01-11", offset=50.0),
    "ingest 2024-01-11..20",
    append_dim="time",
)
```

And the reader is re-interrogated two ways — through the already-open dataset,
and by re-opening the *same session*:

```python
summarize("reader view", reader_view)
summarize("fresh read of the same session", xr.open_zarr(reader.store, consolidated=False))
```

That second check matters. The first could be explained by xarray caching. The
second goes back through the store and still returns the old data, which proves
the pinning is at the session level, not at the xarray level.

**Real output.**

```text
A repository with 10 committed days. A long-running request opens it now.
  branch tip is snapshot TJ57K1Q7

The reader takes a readonly session. That call resolves the branch exactly once:
  reader.snapshot_id = TJ57K1Q7  read_only=True
  reader view: time=10  last=2024-01-10  mean= 25.98 degC
```

A commit lands:

```text
Meanwhile an ingest job appends 10 more days at +50 degC and commits:
  branch tip moved to snapshot Y7F0E3FY

The reader is still holding its session. Ask it again -- nothing changed:
  reader view: time=10  last=2024-01-10  mean= 25.98 degC
  fresh read of the same session: time=10  last=2024-01-10  mean= 25.98 degC
  reader.snapshot_id is still TJ57K1Q7 -- sessions never follow the branch
```

Both reads return ten days. The branch tip is `Y7F0E3FY`; the session's snapshot
id is still `TJ57K1Q7`. A second commit changes nothing:

```text
A second ingest lands while the reader is still open:
  branch tip moved to snapshot C5YKQ50N
  reader view: time=10  last=2024-01-10  mean= 25.98 degC
```

A new session sees everything:

```text
A new request opens a new session and sees everything committed so far:
  fresh reader: time=30  last=2024-01-30  mean= 72.65 degC
```

The mean of 72.65 is the three periods averaged together: ten days at offset 0,
ten at +50, ten at +90, so roughly (26 + 76 + 116) / 3.

And each intermediate state remains addressable:

```text
And any past snapshot is still directly addressable by id:
  pinned to TJ57K1Q7: time=10  last=2024-01-10  mean= 25.98 degC
  pinned to Y7F0E3FY: time=20  last=2024-01-20  mean= 50.98 degC
  pinned to C5YKQ50N: time=30  last=2024-01-30  mean= 72.65 degC
```

Three views of one repository, all valid, all internally coherent, differing only
in when they were pinned.

**Traps.**

- **The reader will never see new data.** This is the feature and the trap in one
  sentence. A service that opens a session at startup and holds it will serve the
  startup state forever. To pick up commits, open a new session — per request is
  the simplest correct policy, and session creation is cheap because it is one
  ref lookup.
- **Session creation is cheap; `xr.open_zarr` is less so.** It reads the snapshot
  and builds the xarray structure. If you open per request, that is a real cost.
  The usual shape is a short-lived cache keyed on the snapshot id, refreshed on a
  timer — but the cache key must be the snapshot id, or you have reinvented the
  staleness problem without the coherence.
- **Isolation does not mean the reader sees the *newest* data.** It means the
  reader sees *one* coherent state. Those are different guarantees and only the
  second is provided.
- **`readonly_session(branch)` resolves at creation time, so there is still a
  race for "latest".** Between resolving and reading, a commit may land. You will
  read the older one, coherently. If you need a specific state, pass
  `snapshot_id=` or `tag=` explicitly.
- **Long-held sessions pin snapshots against expiry.** A retention sweep does not
  know your reader is holding one. Reading through an expired snapshot fails.

### 0303 — Conflicts

Source: [`icechunk/examples/0303_conflicts.py`](../../icechunk/examples/0303_conflicts.py)

**What it teaches.** What happens when two writable sessions start from the same
branch tip and both commit. The first wins; the second raises `ConflictError`.
Then both outcomes of a rebase: a conflict that replays cleanly, and one that
cannot be reconciled by any policy.

**Why it matters.** OCS can have a scheduled ingest and a manual backfill aimed at
the same dataset. icechunk will not silently interleave them. The second commit
fails loudly, and the losing job has to decide whether its work still makes sense
against the new tip — which is a decision only the job can make.

**Key code.** The seed uses one chunk per day so the two writers can genuinely
edit different chunks:

```python
ds.to_zarr(session.store, mode="w", zarr_format=3, consolidated=False, encoding={"t2m": {"chunks": (1, 16, 16)}})
```

Case 1, disjoint edits:

```python
writer_a: Any = repo.writable_session("main")
writer_b: Any = repo.writable_session("main")

fix_a = climate_dataset(days=1, ny=16, nx=16, start="2024-01-01", offset=50.0).drop_vars("time")
fix_a.to_zarr(writer_a.store, region={"time": slice(0, 1)}, consolidated=False)
fix_b = climate_dataset(days=1, ny=16, nx=16, start="2024-01-04", offset=70.0).drop_vars("time")
fix_b.to_zarr(writer_b.store, region={"time": slice(3, 4)}, consolidated=False)

writer_a.commit("A corrects 2024-01-01")
try:
    writer_b.commit("B corrects 2024-01-04")
except icechunk.ConflictError as exc:
    print(f"    expected_parent={str(exc.expected_parent)[:8]}  actual_parent={str(exc.actual_parent)[:8]}")
    writer_b.rebase(icechunk.ConflictDetector())
    writer_b.commit("B corrects 2024-01-04")
```

Case 2, two appends, and both solvers tried:

```python
batch_a.to_zarr(writer_a.store, append_dim="time", consolidated=False)
batch_b.to_zarr(writer_b.store, append_dim="time", consolidated=False)
writer_a.commit("A ingests 2024-01-05..06")

for solver_name, solver in (
    ("ConflictDetector()", icechunk.ConflictDetector()),
    ("BasicConflictSolver()", icechunk.BasicConflictSolver()),
):
    try:
        writer_b.rebase(solver)
    except icechunk.RebaseFailedError as exc:
        for conflict in exc.conflicts:
            chunks = "" if conflict.conflicted_chunks is None else f"  chunks={conflict.conflicted_chunks}"
            print(f"      {conflict.conflict_type} on {conflict.path}{chunks}")
```

**Real output.** Case 1:

```text
  both sessions start from parent 0H4WQRH6
  writer A rewrites step 0; writer B rewrites step 3 -- different chunks
  A commits first: G5MNGX82
  B commits second and raises icechunk.ConflictError:
    Failed to commit, expected parent: Some("0H4WQRH6C3HTREJRFH3G"), actual parent: Some("G5MNGX82ZW3WSCPTTSY0")
    expected_parent=0H4WQRH6  actual_parent=G5MNGX82
  the check is purely on the parent pointer, so it fires even for disjoint edits
```

The two writers touched entirely different chunks and the commit still failed.
That is not a defect: the commit is a compare-and-swap on a pointer, and the
pointer moved. icechunk does not inspect the change sets before failing — it
fails first, then lets you ask whether the changes were actually compatible.

That question is what `rebase` answers:

```text
  B rebases with a ConflictDetector, which raises only on real overlap:
    rebase returned -- no chunk was written by both sides
  B commits again: Y47FZ1VT
  both corrections survive: step means = [76.0, 26.0, 26.0, 96.0]
```

Step 0 at 76 (26 + 50, A's fix), step 3 at 96 (26 + 70, B's fix), steps 1 and 2
untouched at 26. Both writers' work survived, and the history records both:

```text
    Y47FZ1VT  B corrects 2024-01-04
    G5MNGX82  A corrects 2024-01-01
    0H4WQRH6  seed 2024-01-01..04
    1CECHNKR  Repository initialized
```

Case 2 is the one to remember:

```text
  both append 2 steps for 2024-01-05..06 -- both grow time from 4 to 6
  A commits first: R88627W7
  B raises icechunk.ConflictError: Failed to commit, expected parent: Some("Y47FZ1VTRAZPQZR6HCMG"), actual parent: Some("R88627W73K848QRJQY40")
  rebase(ConflictDetector()) raises icechunk.RebaseFailedError:
    Rebase failed on snapshot R88627W73K848QRJQY40: 6 conflicts found
      Zarr metadata double update on /t2m
      Zarr metadata double update on /time
      Chunks updated in updated array on /time
      Chunks updated in updated array on /t2m
      Chunk double update on /time  chunks=[[1]]
      Chunk double update on /t2m  chunks=[[4, 0, 0], [5, 0, 0]]
```

Six distinct conflicts, and the first two are the fatal ones. "Zarr metadata
double update" means both sides changed the array's shape — A grew `time` from 4
to 6, and B grew `time` from 4 to 6. There is no merge of those two facts. The
correct result might be a time axis of 6 (if they wrote the same period) or 8 (if
they wrote different ones), and nothing in the store can tell which.

`BasicConflictSolver` fares no better:

```text
  rebase(BasicConflictSolver()) raises icechunk.RebaseFailedError:
    Rebase failed on snapshot R88627W73K848QRJQY40: 6 conflicts found
      Zarr metadata double update on /t2m
      ...
```

Identical conflict list. `BasicConflictSolver(on_chunk_conflict=...)` can pick a
side for the *chunk* conflicts, but the metadata double-update is not something a
policy can resolve.

The resolution is to re-read reality and redo the work:

```text
  the resolution is to discard B's session, open a fresh one, and redo the work:
    fresh session sees time=6; B's 2 steps are already there, so B appends the next 2
    retry commits: BT1YWFCJ
  final step means = [76.0, 26.0, 26.0, 96.0, 36.0, 36.0, 46.0, 46.0]
```

Eight steps: the four seed days with A's and B's corrections, then A's two at
offset 10 (36.0), then B's retry at offset 20 (46.0). Note what B did on retry —
it looked at the store, saw that the period it meant to write was already present,
and wrote the *next* period instead. That is the resume logic of 0402 applied to
a conflict.

And the example closes by connecting conflicts to the storage question:

```text
Why local filesystem storage warns about concurrent commits:
  the commit is a compare-and-swap on the branch reference: write the new snapshot id
  only if the reference still points at the parent this session started from
  object stores like S3 provide that as a conditional PUT, so the swap is atomic
  a POSIX filesystem has no portable conditional write, so two processes committing
  at the same instant can both believe they won -- icechunk warns rather than pretend
  in-process sessions like this example are still detected correctly; the gap is
  multi-process writers against one local directory, which is why OCS uses one writer
```

That distinction is precise and important. This example's two sessions are in one
process, so the conflict detection works perfectly. The hazard the warning
describes is two *processes*, which no example here can safely demonstrate.

**Traps.**

- **`ConflictError` fires on any branch movement, even a compatible one.** Do not
  read it as "your changes conflict". It means "the branch moved". Whether your
  changes conflict is what `rebase` determines.
- **Two appends to one dimension cannot be rebased. Ever.** Not with
  `BasicConflictSolver`, not with any option. Design around it: one appender per
  dataset, or partition the work so appends never overlap.
- **`BasicConflictSolver()` with no arguments is not a "just make it work"
  button.** It resolves chunk-level conflicts by policy and nothing else.
- **Rebase replays your session against a tip you have not read.** If your changes
  were computed from the old tip — a correction based on values that have since
  been rewritten — a successful rebase can produce a result that is
  self-consistent and wrong. Recomputing against the new tip is often safer than
  rebasing.
- **Use `commit(rebase_with=..., rebase_tries=N)` in production.** Hand-rolled
  try/except/rebase/retry loops get the retry count and the failure path wrong.
- **The in-process conflict detection here does not generalise to processes on a
  local filesystem.** See [Storage](../storage.md).

---

## Phase 4 — The OCS ingest pattern

Phases 1 to 3 taught the machinery. Phase 4 assembles it into the shape a real
ingest has: one period per commit, resumable from what the store actually holds,
and correctable when an upstream feed sends nonsense.

### 0401 — Appending periods

Source: [`icechunk/examples/0401_append_periods.py`](../../icechunk/examples/0401_append_periods.py)

**What it teaches.** The ingest loop itself — six calendar months, each its own
session and its own commit — and the chunk-alignment problem that variable-length
periods provoke against a fixed time chunk. Before each append the example probes
what a *naive* append would do on a throwaway session, so the error is
demonstrated on real data rather than described.

**Why it matters.** This is the loop OCS runs. One period per commit means the
history is a per-period audit trail, a failure only loses the period in flight,
and a reader always sees whole periods — never half of March.

**Key code.** The constants set up the collision deliberately: a 30-day time
chunk against calendar months of 28 to 31 days.

```python
PERIODS = ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06"]
TIME_CHUNK = 30
NY = 32
NX = 32
```

The first write is `mode="w"` with an explicit chunk encoding, because that shape
is fixed for the life of the array:

```python
first: Any = repo.writable_session("main")
month_dataset(PERIODS[0], 0.0).to_zarr(
    first.store,
    mode="w",
    zarr_format=3,
    consolidated=False,
    encoding={"t2m": {"chunks": (TIME_CHUNK, NY, NX)}},
)
snapshot = str(first.commit(f"ingest {PERIODS[0]}"))
```

Each later period runs the probe, then the real append:

```python
def probe_naive_append(repo: Any, ds: xr.Dataset) -> str:
    scratch: Any = repo.writable_session("main")
    try:
        ds.to_zarr(scratch.store, append_dim="time", consolidated=False)
        result = "would have worked unaligned too"
    except ValueError as exc:
        result = f"ValueError: {str(exc).split('. ')[0]}"
    scratch.discard_changes()
    return result
```

The probe is safe precisely because of what 0102 established: a session that is
never committed changes nothing. It can attempt a real write against the real
store and be discarded.

```python
for index, period in enumerate(PERIODS[1:], start=1):
    month = month_dataset(period, float(index))
    print(f"  {period}  naive append -> {probe_naive_append(repo, month)}")
    session: Any = repo.writable_session("main")
    month.to_zarr(session.store, append_dim="time", consolidated=False, align_chunks=True)
    snapshot = str(session.commit(f"ingest {period}"))
```

The incoming data is dask-backed, like a real ingest:

```python
def month_dataset(period: str, offset: float) -> xr.Dataset:
    start = pd.Timestamp(f"{period}-01")
    ds = climate_dataset(days=days_in(period), ny=NY, nx=NX, start=str(start.date()), offset=offset)
    return ds.chunk({"time": TIME_CHUNK})
```

That `.chunk()` call is what makes the alignment problem real. An in-memory numpy
array can be sliced arbitrarily on write; a dask array arrives as blocks, and
those blocks have to map onto the store's chunk grid.

**Real output.** The example works out the collision before writing anything:

```text
Calendar months are 28-31 days, so periods and chunks drift apart as the axis grows:
    2024-01: 31 days -> time[  0: 31]  touches 2 zarr chunks, starts aligned
    2024-02: 29 days -> time[ 31: 60]  touches one zarr chunk, starts offset 1 into a chunk
    2024-03: 31 days -> time[ 60: 91]  touches 2 zarr chunks, starts aligned
    2024-04: 30 days -> time[ 91:121]  touches 2 zarr chunks, starts offset 1 into a chunk
    2024-05: 31 days -> time[121:152]  touches 2 zarr chunks, starts offset 1 into a chunk
    2024-06: 30 days -> time[152:182]  touches 2 zarr chunks, starts offset 2 into a chunk
```

Read down the "starts" column: the offset drifts 0, 1, 0, 1, 1, 2. January's 31
days push the boundary one past the 30-day chunk; February's 29 pull it back;
and so on. Nothing about this is pathological — it is what happens whenever the
natural unit of ingest is not the unit of chunking, which is almost always.

The ingest:

```text
Period 1 of 6 -- mode='w', creating the arrays:
  2024-01  JVT1FVHV  time= 31  last=2024-01-31

Each later period: probe the naive append, then append with align_chunks and commit.
  2024-02  naive append -> would have worked unaligned too
  2024-02  KN3XFSNA  time= 60  last=2024-02-29
  2024-03  naive append -> would have worked unaligned too
  2024-03  2PVSM7ZA  time= 91  last=2024-03-31
  2024-04  naive append -> would have worked unaligned too
  2024-04  HS90B86E  time=121  last=2024-04-30
  2024-05  naive append -> ValueError: Specified Zarr chunks encoding['chunks']=(30, 32, 32) for variable named 't2m' would overlap multiple Dask chunks
  2024-05  Q3KH9GRN  time=152  last=2024-05-31
  2024-06  naive append -> would have worked unaligned too
  2024-06  KXN1M5PP  time=182  last=2024-06-30
```

This is the honest version of the alignment story, and it is more useful than a
blanket rule. Four of the five appends would have succeeded without
`align_chunks=True`. The fifth would not:

```text
ValueError: Specified Zarr chunks encoding['chunks']=(30, 32, 32) for variable
named 't2m' would overlap multiple Dask chunks
```

That is a real error from a real write attempt, not a constructed one. It fires
on 2024-05, appending 31 days starting at offset 121 — one past a chunk boundary
— as one dask block that would straddle two zarr chunks. xarray refuses because
two parallel write tasks would have to read-modify-write the same chunk, and
there is no ordering between them.

The important consequence: **you cannot predict which periods will fail without
tracking the arithmetic**, and an ingest that worked for four months running is
not evidence that it will work for the fifth. Set `align_chunks=True`
unconditionally.

!!! note
    The same error, on the same fifth month, appeared independently in the
    `climate-pipeline` project. That is not a coincidence — it is the arithmetic
    of calendar months against a 30-day chunk. See
    [climate-pipeline](climate-pipeline.md) and the trap section of
    [Open Climate Service](../open-climate-service.md).

The history is exactly one snapshot per period:

```text
The history is one snapshot per period, newest first:
  KXN1M5PP  2026-08-17 17:48:13  ingest 2024-06
  Q3KH9GRN  2026-08-17 17:48:13  ingest 2024-05
  HS90B86E  2026-08-17 17:48:13  ingest 2024-04
  2PVSM7ZA  2026-08-17 17:48:13  ingest 2024-03
  KN3XFSNA  2026-08-17 17:48:13  ingest 2024-02
  JVT1FVHV  2026-08-17 17:48:13  ingest 2024-01
  1CECHNKR  2026-08-17 17:48:12  Repository initialized
```

Six periods, six snapshots, plus the root. Note the probe sessions left no trace:
there are no extra rows, because they were discarded.

The finished dataset:

```text
The finished dataset:
  dims={'time': 182, 'y': 32, 'x': 32}
  time spans 2024-01-01 .. 2024-06-30
  t2m chunks on disk: (30, 32, 32) -- unchanged by six appends
  mean per month (each period carries its own offset): 01:26.0 02:27.0 03:28.0 04:29.0 05:30.0 06:31.0
```

182 days, and the chunk shape is exactly what the first write specified. Six
appends, including one that needed rechunking on the way in, and the store's
layout never moved. `align_chunks=True` rechunks the *incoming* data to fit the
store, not the store to fit the data — which is the right direction, because the
store's layout is what every future read depends on.

**Traps.**

- **`align_chunks=True` on every append. No exceptions.** The cost is a rechunk
  of the incoming block; the alternative is an ingest that fails on an
  unpredictable month.
- **The first write fixes the chunk shape forever.** Always pass explicit
  `encoding={"var": {"chunks": (...)}}` on the creating write. Letting xarray
  choose gives you whatever its heuristic decided that day, and 0101 shows what
  that looks like: `(15, 32, 64)` for a 30 x 64 x 64 array.
- **`encoding=` on an append is ignored, and silently.** Chunk shape is a property
  of the existing array. If you think you are changing it on an append, you are
  not.
- **`mode="w"` in the loop is a full overwrite.** The first period uses it because
  the arrays do not exist yet. Using it for period two destroys period one — and
  destroys the root attributes too, as 0203 shows.
- **`append_dim` on a non-existent store raises.** The `if index == 0` branch is
  load-bearing. 0402 generalises it to "have I written anything yet", derived
  from the store.
- **Chunk size is a trade-off across three axes.** Bigger chunks mean fewer
  objects, better compression, and coarser audit resolution (0203) and coarser
  correction granularity (0403). Smaller means the opposite plus more metadata.
  See `dask/examples/0602_chunk_sizing.py`.
- **A probe session still writes chunk objects to storage before being
  discarded.** Cheap here; on metered object storage a probe per period is real
  PUT traffic.

### 0402 — Resume

Source: [`icechunk/examples/0402_resume.py`](../../icechunk/examples/0402_resume.py)

**What it teaches.** How to restart an interrupted ingest by asking the store
what it already contains, rather than consulting external bookkeeping. Three
periods land, the job dies, a second run derives its own to-do list and finishes,
and a third run correctly does nothing.

**Why it matters.** OCS ingest jobs get restarted — by a scheduler, by an
operator, after a pod eviction. Any bookkeeping kept beside the data (a status
row, a marker file, a log line) can disagree with the data after a crash, because
writing the bookkeeping and writing the data are two operations and a crash can
land between them. The committed time coordinate cannot disagree, because it only
exists if the commit that wrote it landed.

**Key code.** The whole idea is this function:

```python
def committed_periods(repo: Any) -> set[str]:
    try:
        ds = read_dataset(repo)
    except Exception:  # a repository with nothing committed to main raises from the zarr layer
        return set()
    times = pd.to_datetime(np.asarray(ds.time.values))
    return {f"{ts.year:04d}-{ts.month:02d}" for ts in times}
```

Read the branch tip, take the time coordinate, map each timestamp to its period.
That is the entire state model. There is no second source of truth to keep in
sync, because there is no second source.

The `except Exception` is not laziness — a repository with a root snapshot and
nothing written to main genuinely has no zarr group, and the error surfaces from
the zarr layer. An empty store means no periods.

The ingest loop derives its own work:

```python
def ingest(repo: Any, periods: list[str], *, stop_after: int | None = None) -> list[str]:
    present = committed_periods(repo)
    todo = [p for p in periods if p not in present]

    done: list[str] = []
    for period in todo:
        session: Any = repo.writable_session("main")
        month = month_dataset(period)
        if present or done:
            month.to_zarr(session.store, append_dim="time", consolidated=False, align_chunks=True)
        else:
            month.to_zarr(
                session.store, mode="w", zarr_format=3, consolidated=False,
                encoding={"t2m": {"chunks": (TIME_CHUNK, NY, NX)}},
            )
        snapshot = str(session.commit(f"ingest {period}"))
        done.append(period)
        if stop_after is not None and len(done) >= stop_after:
            raise RuntimeError(f"job killed after {stop_after} periods")
    return done
```

The `if present or done` condition generalises 0401's `if index == 0`: create the
arrays if the store is empty *and* this run has not created them yet; otherwise
append. That handles both a first run and a resume with the same code.

**Real output.** Run 1 dies after three periods:

```text
Run 1 -- ingest ['2024-01', '2024-02', '2024-03', '2024-04', '2024-05', '2024-06'] but the job is killed after 3 periods.
  store already holds []
  still to ingest:     ['2024-01', '2024-02', '2024-03', '2024-04', '2024-05', '2024-06']
  committed 2024-01 as GF96AYYR
  committed 2024-02 as N13ENSXM
  committed 2024-03 as 7B6EEYEH
  job killed after 3 periods
```

What survived:

```text
What survived the crash is exactly what was committed:
  time=91  spans 2024-01-01 .. 2024-03-31
  periods present: ['2024-01', '2024-02', '2024-03']
```

91 days = 31 + 29 + 31. Three whole periods, no partial fourth. That is 0301's
atomicity in service of 0402's resume: because a period either commits or does
not, the time axis can never end mid-period, and therefore period membership is
a reliable signal.

Run 2 re-derives:

```text
Run 2 -- the scheduler restarts the same job. It re-derives its own to-do list:
  store already holds ['2024-01', '2024-02', '2024-03']
  still to ingest:     ['2024-04', '2024-05', '2024-06']
  committed 2024-04 as XGR8E88B
  committed 2024-05 as XRHQ9266
  committed 2024-06 as P3BNH6SR
  this run ingested ['2024-04', '2024-05', '2024-06']
```

The second run was given the same six-period list and no information about the
first run. It worked out what to do from the data.

Run 3 does nothing:

```text
Run 3 -- the job runs again with nothing left to do:
  store already holds ['2024-01', '2024-02', '2024-03', '2024-04', '2024-05', '2024-06']
  still to ingest:     []
  this run ingested [] -- resuming is idempotent, so a retry is always safe
```

Idempotence falls out of the design rather than being added. There is no
"already ran" flag to check, so there is no flag to get wrong. A scheduler that
fires twice, an operator who reruns to be sure, a retry after a network blip —
all safe, all no-ops.

The result:

```text
  dims={'time': 182, 'y': 16, 'x': 16}  spans 2024-01-01 .. 2024-06-30
  mean per month: 01:27.0 02:28.0 03:29.0 04:30.0 05:31.0 06:32.0
  one snapshot per period, whichever run wrote it:
    P3BNH6SR  ingest 2024-06
    XRHQ9266  ingest 2024-05
    XGR8E88B  ingest 2024-04
    7B6EEYEH  ingest 2024-03
    N13ENSXM  ingest 2024-02
    GF96AYYR  ingest 2024-01
    1CECHNKR  Repository initialized
```

The history shows six commits with no indication that it took three runs to
produce them, and none is needed — a period arrived, it is recorded, and which
process wrote it is not a property of the data.

**Traps.**

- **This works because periods align with time steps.** A dataset ingested in
  units that do not map cleanly onto the time coordinate — overlapping windows,
  irregular batches — needs a different derivation. The principle holds; the
  specific `{year}-{month}` mapping does not.
- **Presence is not completeness.** A period with one committed day looks
  "present" to this logic. It cannot happen here, because a period is one
  commit and commits are atomic — but if you ever commit *within* a period, the
  check must become "does this period have the expected number of steps".
- **The `except Exception` is broad on purpose but broad nonetheless.** It exists
  because an empty repository raises from the zarr layer, and the exception type
  is not stable API. In production, narrow it once you have confirmed the type
  your version raises, or check `repo.ancestry()` depth instead.
- **Reading the full time coordinate costs a fetch of the time array.** Trivial
  for daily data over years; less trivial for hourly data over decades. If it
  matters, `ds.time[-1]` and arithmetic beats materialising the whole axis.
- **Resume assumes the to-do list is deterministic.** If the period list itself is
  computed from "now", two runs on either side of midnight disagree about what
  the job is. Pin the list.
- **Do not add a status table.** The entire point is that there is one source of
  truth. A status table that agrees with the store adds nothing; one that
  disagrees is worse than nothing.

### 0403 — Rewriting history

Source: [`icechunk/examples/0403_rewriting_history.py`](../../icechunk/examples/0403_rewriting_history.py)

**What it teaches.** How to correct a bad period: not by editing the snapshot
that holds it, which is impossible, but by committing a region write on top. The
bad snapshot survives, stays readable, and the example reads it back to prove it.
It then costs out both sides of that trade.

**Why it matters.** OCS republishes periods when an upstream provider reissues
data. Knowing what was served on a given day is an audit requirement; keeping
every superseded version forever is a storage bill. Both are true, and this
example makes both concrete.

**Key code.** Four months, one poisoned:

```python
for index, period in enumerate(PERIODS):
    offset = 900.0 if period == BAD_PERIOD else float(index)
```

Finding where the period lives on the time axis, which is what a region write
needs:

```python
def period_slice(ds: xr.Dataset, period: str) -> slice:
    times = pd.DatetimeIndex(ds.time.values)
    hits = [i for i, ts in enumerate(times) if f"{ts.year:04d}-{ts.month:02d}" == period]
    if not hits:
        raise ValueError(f"period {period} is not present on the time axis")
    return slice(hits[0], hits[-1] + 1)
```

The correction itself — three lines, and none of them touch the old snapshot:

```python
session = repo.writable_session("main")
corrected = month_dataset(BAD_PERIOD, 1.0).drop_vars("time")
corrected.to_zarr(session.store, region={"time": target}, consolidated=False)
snapshots["fix"] = str(session.commit(f"correct {BAD_PERIOD}: upstream reissued the month"))
```

Again `drop_vars("time")`: the time coordinate is not being corrected, only the
values indexed by it.

**Real output.** The bad ingest:

```text
Ingest four months. The upstream feed for 2024-02 is broken and delivers +900 degC.
  2024-01  37BSYF3D
  2024-02  DN93N077
  2024-03  FQS16XD8
  2024-04  QWQZM75C
  as served: 2024-01=   26.0  2024-02=  926.0  2024-03=   28.0  2024-04=   29.0
  2024-02 is obviously wrong: 926.0 degC is not a temperature
```

The fix:

```text
The fix is forward: rewrite only 2024-02's region in a new commit.
  the snapshot that holds the bad data is immutable -- nothing can edit it in place
  2024-02 occupies time[31:60], so that is the region to write
  committed PP5K41T0
  tip now  : 2024-01=   26.0  2024-02=   27.0  2024-03=   28.0  2024-04=   29.0
  time is still 121 steps -- a correction replaces values, not the axis
```

The time axis did not change length. That is the difference between a correction
and an append, and it is why a correction cannot conflict with itself the way two
appends do.

The bad snapshot is still there:

```text
The bad snapshot did not go anywhere. Read it by id:
  DN93N077  2024-02 ingest    2024-02 mean =    926.0 degC
  QWQZM75C  last ingest       2024-02 mean =    926.0 degC
  PP5K41T0  correction        2024-02 mean =     27.0 degC
```

Two snapshots still serve 926.0 — the one that ingested the bad month and the one
after it, since the badness persisted until the fix. That is exactly what you
want for the question "what did the API return last Tuesday": the answer is
whatever snapshot was the tip then, and it still reads.

The scope of the change, computed by differencing arrays rather than by trusting
the writer:

```text
  mean change per period: 2024-01=0.0  2024-02=-899.0  2024-03=0.0  2024-04=0.0
  only 2024-02 moved; the other three periods hold exactly the values they held before
```

-899.0 = 1.0 - 900.0, exactly the intended shift. Zero everywhere else.

And then the honest caveat about cost:

```text
  the unit of rewriting is the chunk, though: the time chunk is 31 days, so
  time[31:60] lands inside chunk 1, which also holds the first days of 2024-03
  -- those March values are read and written straight back out, unchanged
  so the correction costs one period's worth of chunks, not the whole dataset
```

The 29-day February region sits inside a 31-day chunk, so correcting February
rewrites chunk 1 in full, which also carries the first two days of March. Those
March values are read and written straight back — unchanged in value, but stored
again as new objects. Chunk granularity is the unit of rewriting, exactly as it
is the unit of auditing in 0203.

The history reads as an audit trail:

```text
  PP5K41T0  17:48:14  correct 2024-02: upstream reissued the month
  QWQZM75C  17:48:14  ingest 2024-04
  FQS16XD8  17:48:14  ingest 2024-03
  DN93N077  17:48:14  ingest 2024-02
  37BSYF3D  17:48:14  ingest 2024-01
  1CECHNKR  17:48:14  Repository initialized
```

The mistake and its correction are both in the log. Nothing is airbrushed.

The trade-off, stated both ways:

```text
When keeping the bad snapshot is a feature:
  a user reports an anomaly in a report generated last Tuesday -- you can reproduce
  exactly what the service returned then, instead of arguing about it
  a downstream model trained on the bad month can be identified and retrained
  corrections are visible: 'this month was reissued' is in the log, not in a ticket

When it is a cost:
  the bad month's chunks stay on disk as long as any reference reaches that snapshot
  a feed that reissues the same period nightly accumulates one full copy per night
  the fix is a retention policy: expire old snapshots, then garbage-collect -- see 0502
```

"One full copy per night" is the number that turns this from a curiosity into a
budget line on metered storage.

**Traps.**

- **A region write must not include the coordinate it indexes by.** `drop_vars`
  is mandatory.
- **The region must exist already.** `region=` writes into an existing extent. It
  cannot grow the array — that is `append_dim=`.
- **Corrections cost chunk-granularity, not period-granularity.** Choose chunk
  boundaries that align with your correction unit if corrections are frequent.
  Here a 31-day chunk against a 29-day February wastes two days of rewriting per
  fix; over a nightly reissue that compounds.
- **`period_slice` derives the target from the data.** Hard-coding
  `slice(31, 60)` works right up until a period has a different length or a gap.
  Derive it.
- **A nightly reissue of the same period is a storage leak by default.** Every
  night writes a full copy of that period's chunks, and every copy stays reachable
  from its snapshot. Pair a reissuing feed with a retention policy from day one.
- **`reset_branch` back to a pre-mistake snapshot is *not* a correction.** It
  makes the intervening commits unreachable from the branch, which loses the
  audit trail — the opposite of what this example is for. Fix forward.

---

## Phase 5 — Operations

Two examples about running a repository over time rather than building one:
what history costs, and how to stop paying for the parts you no longer need.

### 0501 — Storage growth

Source: [`icechunk/examples/0501_storage_growth.py`](../../icechunk/examples/0501_storage_growth.py)

**What it teaches.** What a commit actually costs, measured after every commit,
for four different kinds of commit: an append, a metadata-only change, a
single-period correction, and a full rewrite with byte-identical values. The
numbers show precisely what is shared between snapshots and what is not — and
the last case is the one that surprises people.

**Why it matters.** OCS repositories are append-mostly and long-lived. Knowing
that a snapshot costs only the chunks it changed is what makes commit-per-period
affordable at all. Knowing that icechunk shares *by reference* and not by content
is what stops you from assuming an identical rewrite is free.

**Key code.** The measurement is deliberately double: what icechunk considers
reachable, and what is actually on disk.

```python
def chunk_bytes(repo: Any) -> int:
    """``Repository.total_chunks_storage()`` still exists in icechunk 2.1.2 but
    emits a DeprecationWarning; ``chunk_storage_stats().native_bytes`` is the
    replacement and reports the same number."""
    return int(repo.chunk_storage_stats().native_bytes)


def directory_size(path: Path) -> int:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
```

The periods are chunk-aligned on purpose so that every number is a whole number
of periods and nothing is confused by partial chunks:

```python
TIME_CHUNK = 30
NY = 48
NX = 48
PERIOD_BYTES = TIME_CHUNK * NY * NX * 8      # 552,960
```

The final case is the important one — a full rewrite with values that are, by
construction, identical to what is already stored:

```python
session = repo.writable_session("main")
rewrite = xr.concat([month(i, 500.0 if i == 2 else float(i)) for i in range(6)], dim="time")
rewrite.to_zarr(
    session.store, mode="w", zarr_format=3, consolidated=False,
    encoding={"t2m": {"chunks": (TIME_CHUNK, NY, NX)}},
)
session.commit("rewrite with identical values")
identical = bool(read_dataset(repo).t2m.equals(rewrite.t2m))
```

**Real output.** Six appends:

```text
Each period is 30 days x 48 x 48 float64 = 552,960 bytes, exactly one chunk.

Six appends, one commit each:
  append period 1                    snapshots= 2  chunk bytes=  505,856  delta= +505,856  on disk=  508,369
  append period 2                    snapshots= 3  chunk bytes=1,011,699  delta= +505,843  on disk=1,016,351
  append period 3                    snapshots= 4  chunk bytes=1,517,542  delta= +505,843  on disk=1,524,483
  append period 4                    snapshots= 5  chunk bytes=2,023,327  delta= +505,785  on disk=2,032,709
  append period 5                    snapshots= 6  chunk bytes=2,527,033  delta= +503,706  on disk=2,538,994
  append period 6                    snapshots= 7  chunk bytes=3,025,021  delta= +497,988  on disk=3,039,703
```

The delta column is flat at roughly 500 KB per commit. Not 500 KB, then 1 MB,
then 1.5 MB. Each snapshot costs one period, regardless of how much data the
repository already holds. The small variation (505,856 down to 497,988) is
compression responding to different random data per period, not overhead.

The comparison that makes the point:

```text
  after 6 snapshots the store holds 3,025,021 chunk bytes
  the tip is 3,317,760 uncompressed bytes, so the whole history costs 0.91x it
  six full copies would have cost about 18,150,126 bytes -- 19,906,560 uncompressed
```

Six snapshots of a dataset that ends at 3.3 MB uncompressed cost 3.0 MB in total
— *less* than one uncompressed copy of the final state, because compression more
than pays for the versioning. The naive versioning scheme, one full copy per
version, would have cost 18 MB. Sharing is the default, not an optimisation you
enable.

A metadata-only commit:

```text
  metadata-only commit               snapshots= 8  chunk bytes=3,025,021  delta=       +0  on disk=3,041,708
  zero chunk bytes: no array data changed, so no chunk was written
```

Exactly zero chunk bytes. The on-disk figure moved by about 2 KB — a new snapshot
object and a new transaction record — which is the true cost of a provenance
commit.

A correction:

```text
  correct 1 of 6 periods             snapshots= 9  chunk bytes=3,507,815  delta= +482,794  on disk=3,526,921
  the delta is one period's worth, the same size as each append above (504,170)
  the other five periods were not touched, so their chunks are simply referenced again
  the superseded chunk is still reachable from the pre-correction snapshots, so it stays
```

Correcting one of six periods cost one period. The other five were referenced
again, not copied. And the superseded chunk stayed, because a snapshot below the
correction still points at it — which is the storage cost of the auditability
0403 was selling.

And the case worth internalising:

```text
Rewriting the whole dataset with byte-identical values:
  full rewrite, same values          snapshots=10  chunk bytes=6,509,787  delta=+3,001,972  on disk=6,531,943
  the tip's values are unchanged (True) but the store grew by a full dataset
  icechunk shares chunks by reference, not by content hash: a snapshot reuses a chunk
  only when the write never touched it. Writing the same bytes writes new objects.
  so 'is this a no-op?' is a question about which chunks your code wrote, not about values
```

The values are verifiably unchanged — `read_dataset(repo).t2m.equals(rewrite.t2m)`
returned `True` — and the store roughly doubled, from 3.5 MB to 6.5 MB. Three
megabytes for a write that changed nothing.

If icechunk were content-addressed, the delta would be zero. It is not. Sharing
is by reference: a snapshot inherits a chunk when the write never touched that
coordinate. Touch it and you get a new object, whatever you put in it.

The practical consequence: a pipeline that reprocesses "the whole dataset" every
night because that is simpler than working out what changed will pay for a full
copy every night, forever, no matter how little the data actually moves. On local
disk that is a disk-full alert eventually. On S3 it is a line item every month.

The breakdown:

```text
The full breakdown from chunk_storage_stats():
  native_bytes=6,509,787  virtual_bytes=0
  inlined_bytes=1,558  (tiny arrays like the time coordinate)
  on-disk total=6,531,943 -- chunks plus snapshots, manifests, and refs
```

`native_bytes` is chunk data icechunk wrote. `virtual_bytes` is data referenced
in place in some other format (icechunk's virtual chunk feature, which lets a
repository reference existing NetCDF or HDF5 files without copying them) —
zero here because nothing is virtual. `inlined_bytes` is tiny arrays stored
directly inside snapshot objects rather than as separate chunk files; the `time`
coordinate is small enough to qualify, which is why there is no chunk file for it.

The gap between the on-disk total (6,531,943) and `native_bytes + inlined_bytes`
(6,511,345) is about 20 KB: ten snapshots, their manifests, the transaction log,
and the repo config. Roughly 0.3 percent overhead for the entire version history.

**Traps.**

- **`total_chunks_storage()` is deprecated.** It still works in 2.1.2 and reports
  the same number, with a `DeprecationWarning`. Use
  `chunk_storage_stats().native_bytes`.
- **`native_bytes` counts reachable chunks only.** Orphans from failed writes
  (0301) and superseded chunks that expiry has cut loose (0502) do not appear.
  Only `du` sees those. Measure both, as this example does.
- **Identical values are not free.** The single most expensive misconception about
  icechunk.
- **`chunk_storage_stats()` walks manifests and is not free either.** It takes
  `max_snapshots_in_memory`, `max_compressed_manifest_mem_bytes`, and
  `max_concurrent_manifest_fetches` for a reason. On a repository with thousands
  of snapshots over object storage, do not call it in a request handler.
- **The 0.91x ratio is specific to this data.** Smooth synthetic values compress
  well. Real climate fields with more structure compress less, and the history
  overhead relative to the tip goes up accordingly.
- **A correction's cost is chunk-aligned, not period-aligned.** Here the periods
  were made exactly one chunk, so the numbers are clean. 0403's 29-day February
  in a 31-day chunk is the messier and more realistic case.

### 0502 — Expiry and garbage collection

Source: [`icechunk/examples/0502_expiry_and_gc.py`](../../icechunk/examples/0502_expiry_and_gc.py)

**What it teaches.** The retention policy, in its two distinct steps, with
measurements at every stage. It builds a history with genuinely superseded data —
four ingests plus three corrections to the same period — expires the older
snapshots, garbage-collects, and then tries to read what was expired.

**Why it matters.** A repository that reingests periods accumulates superseded
chunks forever. Expiry plus garbage collection is how you stop that, and it is
the only operation in this entire project that destroys data. It is worth
understanding exactly what it removes *before* running it, because there is no
inverse.

**Key code.** The history is built to have something worth collecting:

```python
PERIODS = 4
CORRECTIONS = 3

for revision in range(CORRECTIONS):
    session = repo.writable_session("main")
    fixed = climate_dataset(days=TIME_CHUNK, ny=NY, nx=NX, start="2024-01-01", offset=100.0 + revision)
    fixed.drop_vars("time").to_zarr(session.store, region={"time": slice(0, TIME_CHUNK)}, consolidated=False)
    snapshots[f"correction {revision + 1}"] = str(session.commit(f"correct period 1, revision {revision + 1}"))
```

Three corrections to period 1 means three superseded copies of period 1's chunk —
exactly the pattern a nightly-reissuing feed produces.

The cutoff is derived from the history rather than hard-coded:

```python
history = describe_history(repo)
cutoff = history[1]["written_at"]
expired: set[str] = {str(s) for s in repo.expire_snapshots(older_than=cutoff)}
```

`history[1]` is the second-newest snapshot, so this expires everything older than
it — all but the last two.

Then garbage collection, dry run first:

```python
cutoff_objects = dt.datetime.now(dt.UTC)
dry: Any = repo.garbage_collect(cutoff_objects, dry_run=True)
print(f"  would delete: {dry.chunks_deleted} chunks, {dry.manifests_deleted} manifests,")
print(f"                {dry.snapshots_deleted} snapshots, {dry.bytes_deleted:,} bytes in total")

summary: Any = repo.garbage_collect(cutoff_objects)
```

**Real output.** The starting state:

```text
  after ingest + fixes   snapshots= 8  reachable chunk bytes=3,507,418  on disk=3,523,403

  history, newest first:
    3MTJFJN8  17:48:16.094612  correct period 1, revision 3
    PYT75R94  17:48:16.089086  correct period 1, revision 2
    564C31X4  17:48:16.083545  correct period 1, revision 1
    W4W56MZ6  17:48:16.077933  ingest period 4
    J8F8992C  17:48:16.068711  ingest period 3
    C2253WD8  17:48:16.058999  ingest period 2
    CKDKXAH6  17:48:16.047959  ingest period 1
    1CECHNKR  17:48:15.830249  Repository initialized
```

Every revision is readable, and each returns a different value:

```text
  every past revision of period 1 is still readable by snapshot id:
    W4W56MZ6  ingest 4                 readable, period 1 mean = 26.0 degC
    564C31X4  correction 1             readable, period 1 mean = 126.0 degC
    PYT75R94  correction 2             readable, period 1 mean = 127.0 degC
    3MTJFJN8  correction 3             readable, period 1 mean = 128.0 degC
```

Four versions of period 1 on disk simultaneously. That is the storage the
retention policy is about to reclaim.

Expiry:

```text
Expire everything written before 17:48:16.089086 -- that is, all but the last two.
  expire_snapshots walks the refs and removes those snapshots from the ancestry chain
  the root snapshot and the main branch tip are never expired
  expired 5 snapshots
  after expire           snapshots= 3  reachable chunk bytes=2,508,575  on disk=3,524,244
```

Read those numbers carefully, because they are the whole lesson of this step.
Snapshots went 8 to 3. Reachable chunk bytes dropped by about a megabyte.
**On-disk size went up by 841 bytes.** Expiry rewrote the ancestry — which is
itself a write — and deleted nothing.

The history afterwards:

```text
  history is now:
    3MTJFJN8  17:48:16.094612  correct period 1, revision 3
    PYT75R94  17:48:16.089086  correct period 1, revision 2
    1CECHNKR  17:48:15.830249  Repository initialized
```

Note what survived and why. `3MTJFJN8` is the branch tip, never expired.
`1CECHNKR` is the root, never expired. `PYT75R94` is at the cutoff, and
`older_than` is strict. Everything else went.

Garbage collection, dry run:

```text
Garbage-collect objects that nothing reachable references. Dry run first:
  would delete: 2 chunks, 8 manifests,
                5 snapshots, 1,005,508 bytes in total
  after dry run          snapshots= 3  reachable chunk bytes=2,508,575  on disk=3,524,244
  a dry run changes nothing, which the unchanged on-disk size confirms
```

The dry run reports what would go and demonstrably changes nothing — the on-disk
figure is identical to before it.

Then for real:

```text
Now for real:
  deleted: 2 chunks, 8 manifests,
           5 snapshots, 1,005,508 bytes in total
  after gc               snapshots= 3  reachable chunk bytes=2,508,575  on disk=2,519,615
```

The real run matches the dry run exactly. On disk went from 3,524,244 to
2,519,615 — 1,004,629 bytes reclaimed, tracking the reported 1,005,508 closely.

Note **two chunks**, not four. Period 1 had four versions, but two of them are
still reachable: revision 3 is the tip's version, and revision 2 is still in the
ancestry. Only revisions from the expired snapshots were collectible.

The tip is untouched:

```text
The tip is untouched -- the data being served is exactly what it was:
  dims={'time': 120, 'y': 48, 'x': 48}  period 1 mean = 128.0 degC (the last correction)
```

120 time steps, four periods, period 1 showing the last correction. The served
dataset is bit-identical to before the sweep. That is the guarantee: retention
touches history, never the current state.

And the part that cannot be undone:

```text
The expired snapshots are gone, and gone means gone:
    W4W56MZ6  ingest 4                 unreadable: SnapshotNotFoundError
    564C31X4  correction 1             unreadable: SnapshotNotFoundError
    PYT75R94  correction 2             readable, period 1 mean = 127.0 degC
    3MTJFJN8  correction 3             readable, period 1 mean = 128.0 degC
```

`SnapshotNotFoundError`. An id that was a permanent handle in 0201 is now a
dangling reference. Anything that recorded `W4W56MZ6` — a model card, a report
footer, a database row — now points at nothing.

The policy advice:

```text
  expiry is irreversible: there is no un-expire, and gc then removes the objects
  the usual policy is a window (keep 90 days of snapshots) plus tags on the
  snapshots that must outlive it, since a tagged snapshot keeps its chunks alive
```

**Traps.**

- **Expiry and GC are two steps and they measure differently.** Expiry moves
  reachable bytes and not disk. GC moves disk and not reachable bytes. If you
  only watch one number you will conclude the other step did nothing.
- **Always `dry_run=True` first.** The `GCSummary` tells you exactly what would
  go, and it costs a walk rather than a deletion.
- **In an append-only history, GC reclaims essentially nothing.** Every chunk is
  still referenced by the tip. The savings come entirely from *superseded* chunks,
  which only corrections and reingests create. If your ingest is pure appends, a
  retention sweep buys you almost no space — and still costs you the history.
- **Tag before you sweep.** A tagged snapshot keeps its chunks alive. That is the
  mechanism for "this version is cited and must survive". Doing it after expiry is
  too late.
- **`garbage_collect(delete_object_older_than=...)` takes an object-age cutoff, not
  a snapshot-age cutoff.** Passing `datetime.now(UTC)` as this example does is
  safe in a single-writer setting, but with a concurrent writer in flight it can
  delete objects a live session just wrote and has not yet committed. Set the
  cutoff behind the longest possible in-flight write.
- **A long-held readonly session pinned to an expiring snapshot will start
  failing.** Retention does not know about your readers.
- **Both operations are irreversible.** There is no un-expire and no undelete.
  Treat a retention sweep like a `DROP`, with the same review.

---

## What is actually on disk

0101 walks the layout and 0501 measures how it grows. This section pulls both
together and explains the mechanism, because "how does chunk sharing work" is the
question that makes everything else make sense.

### The layout

Here is a complete listing of a repository after two commits — a 30-day
`mode="w"` write followed by a 29-day append, on a 64 x 64 grid. Generated
directly against this project's environment:

```text
chunks/
chunks/DDG6PKZ7N88F6RAD3XBG                    223,019 bytes
chunks/EGN952WG90S2P6S3MSXG                    223,030 bytes
chunks/G3ZP68ERFRHCQSAAJSHG                    222,979 bytes
chunks/HTT1TC49XTEGJ93VY580                    223,146 bytes
chunks/K2QPAB3VFC91569TCZ00                    222,993 bytes
chunks/KK110JC0FY0ANCWZG23G                    223,014 bytes
chunks/RD8MNF52JP8657XMN440                    223,254 bytes
chunks/YY6Z7P6E0V2FP9JMBFG0                    223,009 bytes
manifests/
manifests/DR4CMMEDGYTSEG6E9240                     250 bytes
manifests/HX72QQDY9QSETKM6ES7G                     277 bytes
manifests/MBCVASX2C0NPAZPC10F0                     197 bytes
manifests/RPDS38ZHYX7DR2NRS55G                     364 bytes
overwritten/
overwritten/repo.30716691004467.4B6P8WVWHV9ANAP91QS0       419 bytes
overwritten/repo.30716691004478.QAEJRGZ1YZ686YDPJKZ0       272 bytes
repo                                               509 bytes
snapshots/
snapshots/1CECHNKREP0F1RSTCMT0                     192 bytes
snapshots/HQGV9R68CDQVBFFAKMZG                     963 bytes
snapshots/ME5452JV03Y51VKTGW40                     971 bytes
transactions/
transactions/1CECHNKREP0F1RSTCMT0                  118 bytes
transactions/HQGV9R68CDQVBFFAKMZG                  233 bytes
transactions/ME5452JV03Y51VKTGW40                  225 bytes
```

Every id differs per run except `1CECHNKREP0F1RSTCMT0`. Note there is no `refs/`
directory in this listing — with a single unmoved branch and no tags on a
freshly created local repository, the reference state is carried in the objects
above rather than in a separate visible tree. Create a branch or a tag and
reference entries appear.

### What each part holds

**`chunks/`** — the array data. One file per chunk written, under an opaque id,
compressed by the codec pipeline zarr configured. Eight files here: four for the
first 30-day write (chunks `(15, 32, 64)` over a 30 x 64 x 64 array gives a
2 x 2 x 1 grid) and four for the append. About 223 KB each, and they are the
overwhelming majority of the repository — 1.78 MB of 1.79 MB.

These files are **immutable and never modified**. Nothing in icechunk opens an
existing chunk file for writing. A changed chunk is a new file with a new id.

**`manifests/`** — the mapping from chunk coordinate to chunk file, per array,
per snapshot. This is the redirection layer that makes everything else work. A
manifest for `/t2m` says "coordinate `[0, 0, 0]` is in file `G3ZP68ER...`,
coordinate `[0, 1, 0]` is in file `KK110JC0...`". Four manifests here, 250 to 364
bytes each — a couple of hundred bytes of bookkeeping per commit against a
megabyte of data.

**`snapshots/`** — one file per commit, holding the complete zarr hierarchy at
that point: groups, arrays, shapes, dtypes, chunk grids, codecs, dimension names,
attributes, plus references to the manifests. This is where the `zarr.json`
content lives — inside a snapshot object rather than as files in a tree. Three
here: the root (192 bytes, empty) and one per commit (963 and 971 bytes).

**`transactions/`** — the change log entry each commit appends. Keyed by the same
id as the snapshot it corresponds to.

**`repo`** — the repository configuration: format version, default settings,
virtual chunk container definitions. The one thing that gets rewritten rather
than only appended to.

**`overwritten/`** — superseded copies of `repo`. When the config is rewritten,
the old version is moved here rather than deleted, which is what lets a reader
that was mid-read still resolve. Two entries in this listing, and they are
normal, not detritus from a failure.

**References** — the branch and tag pointers. A branch is a name and a
20-character snapshot id; a tag is the same. They are tiny, and they are the only
mutable state in the entire repository. Everything else is write-once.

### How chunk sharing works

Now the mechanism, which follows directly from the layout.

Consider the two commits in that listing. The first wrote 30 days and produced
four chunk files. The second appended 29 days and produced four more. The
repository holds eight chunk files, and snapshot 2 addresses all 59 days.

How? Snapshot 2's manifest for `/t2m` names all eight files. Four of those
entries are byte-identical to the corresponding entries in snapshot 1's manifest —
they point at the same chunk files, because the append never touched those
coordinates. The four new entries point at the new files.

So the cost of snapshot 2 is: four new chunk files, one new manifest, one new
snapshot object, one new transaction record. Not a copy of January.

Generalise that and you get the numbers from 0501:

```text
  append period 1                    snapshots= 2  chunk bytes=  505,856  delta= +505,856  on disk=  508,369
  append period 2                    snapshots= 3  chunk bytes=1,011,699  delta= +505,843  on disk=1,016,351
  append period 3                    snapshots= 4  chunk bytes=1,517,542  delta= +505,843  on disk=1,524,483
  append period 4                    snapshots= 5  chunk bytes=2,023,327  delta= +505,785  on disk=2,032,709
  append period 5                    snapshots= 6  chunk bytes=2,527,033  delta= +503,706  on disk=2,538,994
  append period 6                    snapshots= 7  chunk bytes=3,025,021  delta= +497,988  on disk=3,039,703
```

Flat delta. Seven snapshots, six periods of data, and the total is what six
periods cost. The comparison the example draws:

```text
  the tip is 3,317,760 uncompressed bytes, so the whole history costs 0.91x it
  six full copies would have cost about 18,150,126 bytes -- 19,906,560 uncompressed
```

And the overhead of versioning itself is visible in the gap between chunk bytes
and disk: 3,025,021 versus 3,039,703, so about 14.7 KB for seven snapshots,
their manifests, and the transaction log. Under half a percent.

### The critical qualification: by reference, not by content

Sharing happens when a write **does not touch a coordinate**. It does not happen
because two chunks contain the same bytes.

There is no content hash anywhere in this design. A chunk file's id is not
derived from its contents. When a write targets a coordinate, icechunk writes a
new object under a new id and the new manifest points at it — regardless of
whether the bytes match what was there before.

0501 demonstrates this in the most direct possible way, by writing the identical
dataset back:

```text
  full rewrite, same values          snapshots=10  chunk bytes=6,509,787  delta=+3,001,972  on disk=6,531,943
  the tip's values are unchanged (True) but the store grew by a full dataset
```

`(True)` there is `read_dataset(repo).t2m.equals(rewrite.t2m)`. The data is
identical. The store doubled.

The rule to carry away: **whether a commit is cheap is determined by which chunk
coordinates your code wrote, not by what values it wrote.** Use `append_dim=` to
extend and `region=` to patch. Avoid `mode="w"` on an existing store unless you
genuinely intend a full copy.

### Where the space goes when history piles up

Superseded chunks are the accumulation mechanism. When 0403 corrects February,
the old February chunk is not deleted — a snapshot below the correction still
names it. When 0502 corrects period 1 three times, four versions of period 1's
chunk coexist:

```text
    W4W56MZ6  ingest 4                 readable, period 1 mean = 26.0 degC
    564C31X4  correction 1             readable, period 1 mean = 126.0 degC
    PYT75R94  correction 2             readable, period 1 mean = 127.0 degC
    3MTJFJN8  correction 3             readable, period 1 mean = 128.0 degC
```

All four readable means all four on disk. `expire_snapshots` cuts the references,
`garbage_collect` deletes the objects, and the two steps show up separately in
the numbers:

```text
  after ingest + fixes   snapshots= 8  reachable chunk bytes=3,507,418  on disk=3,523,403
  after expire           snapshots= 3  reachable chunk bytes=2,508,575  on disk=3,524,244
  after gc               snapshots= 3  reachable chunk bytes=2,508,575  on disk=2,519,615
```

Expiry moved the first number. GC moved the second. Neither moved both.

### Practical consequences of the layout

- **Do not inspect it with zarr tooling.** There is no `zarr.json` to read and no
  chunk key to compute. Everything goes through a session.
- **Do not copy it partially.** The refs are the only mutable state; a copy that
  captures a ref pointing at a snapshot whose chunks did not make it is a broken
  repository. Copy whole, or copy immutable objects first and refs last.
- **Do not hand-delete anything from `chunks/`.** A file that looks orphaned may
  be referenced by a snapshot you are not thinking about. `garbage_collect` knows;
  `find -delete` does not.
- **The object count matters on object storage.** Every chunk is one object, and
  object stores charge per request and per object. A repository with tiny chunks
  has a lot of objects. This is the main reason chunks should be larger on S3 than
  on local disk — see [Storage](../storage.md).
- **Metadata overhead is genuinely small.** Under half a percent in these
  measurements. The version history is close to free; it is the *superseded data*
  that costs, and that is under your control.

---

## Storage backends

icechunk ships eight storage constructors. Every example in this project uses the
first one, and the point of this section is that switching to any of the others
changes exactly one line.

The full argument about *which* one you need — the compare-and-swap analysis, the
one-committer rule, the shared-path constraint, the decision table — is in
[Storage](../storage.md). This section covers the code shape and the small
practical differences.

### The one line that changes

```python
storage = icechunk.local_filesystem_storage("/data/temperature.icechunk")
storage = icechunk.s3_storage(bucket="climate", prefix="temperature", region="eu-west-1")
```

Everything after that line is identical:

```python
repo = icechunk.Repository.open_or_create(storage)
session = repo.writable_session("main")
ds.to_zarr(session.store, append_dim="time", consolidated=False, align_chunks=True)
session.commit("ingest 2024-05")
```

Sessions, commits, branches, tags, ancestry, diffs, conflicts, rebase, expiry,
garbage collection, `chunk_storage_stats()` — all behave identically across every
backend. Everything the fourteen examples teach transfers unchanged. That is not
a marketing claim; it is a consequence of the design, where the backend only ever
sees "put this immutable object" and "conditionally swap this reference".

### `local_filesystem_storage`

```python
storage = icechunk.local_filesystem_storage("/data/downloads/era5_t2m.icechunk")
```

One positional argument: the directory. Created if absent.

This is what OCS uses today and what every example here uses. It is correct for a
single committer and it is not a toy — a single-instance service on one box is a
legitimate deployment. It warns on every open that it is unsafe for concurrent
commits, and that warning is accurate: POSIX has no portable conditional write,
so the compare-and-swap at the heart of `commit()` cannot be made atomic across
processes.

### `s3_storage`

```python
storage = icechunk.s3_storage(
    bucket="climate-data",
    prefix="datasets/era5_t2m",
    region="eu-west-1",
)
```

The main object-store target and the one OCS is planning to move to. Credentials
resolve several ways, in rough order of preference for a deployment:

```python
# 1. The ambient environment: IAM role, instance profile, ~/.aws/credentials
icechunk.s3_storage(bucket="climate-data", prefix="era5", region="eu-west-1", from_env=True)

# 2. Explicit static credentials -- fine for a test, poor for a service
icechunk.s3_storage(
    bucket="climate-data", prefix="era5", region="eu-west-1",
    access_key_id="...", secret_access_key="...", session_token="...",
)

# 3. A callable, for credentials that rotate
icechunk.s3_storage(
    bucket="climate-data", prefix="era5", region="eu-west-1",
    get_credentials=fetch_fresh_credentials,
)

# 4. Public data, no credentials at all
icechunk.s3_storage(bucket="public-climate", prefix="era5", region="us-west-2", anonymous=True)
```

For S3-compatible services — MinIO, Ceph, LocalStack — point it at the endpoint:

```python
storage = icechunk.s3_storage(
    bucket="climate-data",
    prefix="era5",
    region="us-east-1",
    endpoint_url="http://localhost:9000",
    allow_http=True,
    force_path_style=True,
    access_key_id="minioadmin",
    secret_access_key="minioadmin",
)
```

`allow_http=True` is required for a plain-HTTP endpoint. `force_path_style=True`
is usually required for MinIO, which does not do virtual-host-style bucket
addressing by default.

The constructor takes a long tail of further options —
`network_stream_timeout_seconds`, `requester_pays`, `checksum_algorithm`,
`expires_after`, `scatter_initial_credentials`, per-operation headers. Reach for
them when a specific deployment needs them; the defaults are sensible.

!!! note
    Never write credentials into a repository or a config file. This project's
    convention is that secrets come from 1Password via the `op` CLI, and in a
    deployment they come from the environment or an instance role. `from_env=True`
    is the right default for a service.

### `gcs_storage`

```python
storage = icechunk.gcs_storage(
    bucket="climate-data",
    prefix="datasets/era5_t2m",
)
```

Google Cloud Storage. Credentials from the ambient environment by default —
application default credentials, or a workload identity on GKE — with explicit
alternatives:

```python
icechunk.gcs_storage(bucket="climate-data", prefix="era5", service_account_file="/secrets/sa.json")
icechunk.gcs_storage(bucket="climate-data", prefix="era5", bearer_token="...")
icechunk.gcs_storage(bucket="public-climate", prefix="era5", anonymous=True)
```

### `azure_storage`

```python
storage = icechunk.azure_storage(
    account="climatestorage",
    container="datasets",
    prefix="era5_t2m",
)
```

Azure Blob Storage. Note the three-part address — account, container, prefix —
where S3 and GCS take two. Credentials via `access_key`, `sas_token`,
`bearer_token`, `from_env=True`, or `anonymous=True`.

### `r2_storage`

```python
storage = icechunk.r2_storage(
    bucket="climate-data",
    prefix="era5_t2m",
    account_id="your-cloudflare-account-id",
)
```

Cloudflare R2, which is S3-compatible but addressed by account id. The signature
otherwise mirrors `s3_storage`, including the credential options. R2's selling
point is no egress charges, which matters for a dataset served publicly.

### `tigris_storage`

```python
storage = icechunk.tigris_storage(
    bucket="climate-data",
    prefix="era5_t2m",
    region="auto",
)
```

Tigris, another S3-compatible store with globally distributed buckets. It takes
one distinctive argument, `use_weak_consistency=False` by default — leave it
alone unless you have read Tigris's consistency documentation and know what you
are trading.

### `http_storage`

```python
storage = icechunk.http_storage("https://data.example.org/stores/era5_t2m/")
repo = icechunk.Repository.open(storage)
ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
```

**Read-only.** Serves a repository over plain HTTP from a static file server or a
CDN, which is a genuinely good way to publish a dataset: no credentials, no
API, and the client gets the full version history including tags and time travel.
There is no conditional PUT over plain HTTP, so no writes.

### `in_memory_storage`

```python
storage = icechunk.in_memory_storage()
repo = icechunk.Repository.create(storage)
```

No arguments, no persistence. Everything vanishes when the process exits. Useful
for tests where you want the full icechunk semantics — commits, branches,
conflicts — without touching a disk. This project's tests use temporary
directories instead, because they also want to assert on on-disk sizes.

### What actually changes when you move

Code: one line. Operations: rather more.

- **Latency per operation rises from microseconds to milliseconds.** Chunks should
  be larger than you would choose on local disk, and the number of small objects
  starts to matter. Every chunk is an object, and every object is a request.
- **Storage costs money and requests cost money.** The chunk-sharing behaviour in
  0501 stops being trivia. A rewrite that duplicates a dataset is a line on a
  bill. Expiry and garbage collection (0502) become routine maintenance rather
  than a curiosity.
- **Conflicts become real.** With concurrent committers actually possible, the
  rebase paths in 0303 turn into code you need rather than a lesson you read.
- **Credentials and endpoints become configuration** that must be right in the API
  process and in every worker image — the same discipline as version pinning, and
  it fails the same way when it is wrong.
- **The concurrent-commit warning goes away**, because the hazard it describes
  goes away. That is the point of moving.

[Storage](../storage.md) has the decision table, the fork/merge analysis, and the
shared-path argument that in practice forces the move before the concurrency
argument does.

---

## Pitfalls and gotchas

Consolidated from all fourteen examples. Each of these has a demonstration
somewhere in the project rather than being received wisdom.

### The local-filesystem concurrent-commit warning

Every open of a local-filesystem repository prints:

```text
WARN icechunk_arrow_object_store: The LocalFileSystem storage is not safe for
concurrent commits. If more than one thread/process will attempt to commit at the
same time, prefer using object stores.
```

**What it means.** `commit()` is a compare-and-swap on a branch reference: write
the new snapshot id, but only if the reference still holds the parent this session
started from. Object stores implement that as a conditional PUT. POSIX has no
portable equivalent, so two processes committing at the same instant can both
believe they won, and one commit is silently lost.

**What it does not mean.** It is not about two *sessions* in one process — 0303
shows those conflicting correctly, with `ConflictError` raised exactly as
designed. It is not about dask, either: the fork/merge model has many writers and
one committer, so a dask job is one committer.

**When it bites.** A scheduled ingest overlapping a manual backfill. Two service
replicas both configured to sync. A retry firing while the original is still
running. Any two independent processes committing to the same branch.

**The examples silence it** with `quiet_icechunk_logs()` because they are
single-writer by construction and the warning would drown the lesson — the
helper's docstring says exactly that. **Do not silence it in production.** If your
service prints it and you have two committers, the warning is describing your bug.

The full argument is in [Storage](../storage.md).

### Conflicts that cannot be rebased

Two appends to the same dimension are unresolvable. Not difficult — unresolvable.
From 0303:

```text
  rebase(ConflictDetector()) raises icechunk.RebaseFailedError:
    Rebase failed on snapshot R88627W73K848QRJQY40: 6 conflicts found
      Zarr metadata double update on /t2m
      Zarr metadata double update on /time
      Chunks updated in updated array on /time
      Chunks updated in updated array on /t2m
      Chunk double update on /time  chunks=[[1]]
      Chunk double update on /t2m  chunks=[[4, 0, 0], [5, 0, 0]]
```

`BasicConflictSolver` produces the identical list. Both writers changed the
array's shape, and there is no policy that decides whether the correct result is
six time steps or eight.

**The fix** is to discard, re-read the tip, and redo the work against what is
actually there:

```text
    fresh session sees time=6; B's 2 steps are already there, so B appends the next 2
    retry commits: BT1YWFCJ
```

That is 0402's resume logic applied to a conflict, and it is why deriving the
to-do list from the store rather than from a plan makes a system robust in more
than one dimension.

**The design rule:** one appender per dataset. If you need multiple writers,
partition so that appends never overlap, or use fork/merge so there is one
committer.

Disjoint chunk edits, by contrast, rebase cleanly — 0303 case 1 shows both
writers' corrections surviving.

### Append chunk alignment

Appending a dask-backed block that straddles two store chunks fails:

```text
  2024-05  naive append -> ValueError: Specified Zarr chunks encoding['chunks']=(30, 32, 32) for variable named 't2m' would overlap multiple Dask chunks
```

That is a real error, from a real write, in 0401. Two parallel write tasks would
have to read-modify-write the same chunk, and there is no ordering between them,
so xarray refuses.

**The fix is `align_chunks=True`:**

```python
month.to_zarr(session.store, append_dim="time", consolidated=False, align_chunks=True)
```

It rechunks the incoming data onto the store's grid before writing, so every
write targets whole chunks.

**Why it is a trap rather than a bug.** 0401 probes every period and four of the
five would have worked unaligned:

```text
  2024-02  naive append -> would have worked unaligned too
  2024-03  naive append -> would have worked unaligned too
  2024-04  naive append -> would have worked unaligned too
  2024-05  naive append -> ValueError: ...
  2024-06  naive append -> would have worked unaligned too
```

An ingest that worked for four months is not evidence it will work for the fifth.
The failure depends on where the cumulative time offset happens to fall relative
to the chunk boundary, which drifts as calendar months of different lengths
accumulate against a fixed chunk. Set `align_chunks=True` unconditionally.

The same error appeared independently in the `climate-pipeline` project, on the
same fifth month. See [climate-pipeline](climate-pipeline.md).

### The first write fixes the chunk shape forever

```python
ds.to_zarr(session.store, mode="w", zarr_format=3, consolidated=False,
           encoding={"t2m": {"chunks": (30, 32, 32)}})
```

Pass that encoding explicitly. If you do not, xarray picks — in 0101 it chose
`(15, 32, 64)` for a 30 x 64 x 64 array — and that choice is permanent for the
life of the array. `encoding=` on a subsequent append is ignored, silently.

Chunk shape determines three separate things beyond read performance: the
granularity of the audit trail in a diff (0203), the granularity and cost of a
correction (0403), and the number of objects on object storage. Choose it on
purpose.

### `mode="w"` on an existing store is a full overwrite

It deletes and recreates the arrays. 0203 shows what the diff looks like:

```text
    new_arrays        ['/t2m', '/time']
    deleted_arrays    ['/t2m', '/time']
```

The same paths under both. And the root attributes are silently gone:

```text
      attrs at S7H26FX4: {'source': 'era5-land', 'ingest_version': '2.1'}
      attrs at AQQ5SYS4: {}
```

On a GeoZarr dataset that is the CRS and grid mapping disappearing. Use
`append_dim=` to extend and `region=` to patch.

### A region write must drop its indexing coordinate

```python
patch.drop_vars("time").to_zarr(session.store, region={"time": slice(0, 1)}, consolidated=False)
```

Not optional. This appears in 0203, 0303, 0403, and 0502 — every region write in
the project.

Also: `region=` writes into an existing extent and cannot grow the array. Growing
is `append_dim=`.

### Expiry is irreversible

```text
The expired snapshots are gone, and gone means gone:
    W4W56MZ6  ingest 4                 unreadable: SnapshotNotFoundError
    564C31X4  correction 1             unreadable: SnapshotNotFoundError
```

There is no un-expire. An id that was a permanent handle is now a dangling
reference, and anything that recorded it — a model card, a report, a database row
— points at nothing.

**Before a sweep:** tag every snapshot that must survive, since a tag keeps its
chunks alive. **During:** run `garbage_collect(..., dry_run=True)` first and read
the `GCSummary`. **Always:** treat it like a `DROP`, with the same review.

One more subtlety: `garbage_collect(delete_object_older_than=...)` takes an
*object-age* cutoff. Passing `datetime.now(UTC)` is safe with a single writer;
with a concurrent writer in flight it can delete objects that a live session has
written but not yet committed. Set the cutoff behind the longest plausible
in-flight write.

### Garbage collection reclaims nothing in an append-only history

Worth stating plainly because it disappoints people: in a pure-append history,
every chunk is still referenced by the tip. Expiring old snapshots removes the
*snapshots*, and the chunks stay, because the current dataset needs them.

The savings come entirely from **superseded** chunks, which only corrections and
reingests create. 0502 has to build three corrections into its history to have
anything worth collecting, and even then GC deleted two chunks out of four
versions of period 1 — the other two were still reachable.

So: if your ingest is append-only, a retention sweep buys almost no space and
still costs you the history. Do not run it out of habit.

### `total_chunks_storage()` is deprecated

```python
repo.total_chunks_storage()                     # DeprecationWarning in icechunk 2.1.2
repo.chunk_storage_stats().native_bytes         # the replacement, same number
```

`chunk_storage_stats()` also gives you `virtual_bytes` and `inlined_bytes`:

```text
  native_bytes=6,509,787  virtual_bytes=0
  inlined_bytes=1,558  (tiny arrays like the time coordinate)
```

Both are relatively expensive — they walk manifests, and they take
`max_snapshots_in_memory`, `max_compressed_manifest_mem_bytes`, and
`max_concurrent_manifest_fetches` for that reason. Do not call either in a
request handler on a large repository.

### Reachable bytes are not disk bytes

`native_bytes` counts what is reachable. It does not count orphans from failed
writes, and it does not count superseded chunks that expiry has cut loose. Only
the filesystem sees those.

0301 shows orphans accumulating from an abandoned write:

```text
  on-disk bytes before=21255, after=39534 (chunk objects exist but are orphaned)
```

0502 shows the two numbers moving independently:

```text
  after expire           snapshots= 3  reachable chunk bytes=2,508,575  on disk=3,524,244
  after gc               snapshots= 3  reachable chunk bytes=2,508,575  on disk=2,519,615
```

Monitor both. A growing gap between them is unreclaimed garbage.

### Sharing is by reference, not by content

Writing byte-identical data is not free:

```text
  full rewrite, same values          snapshots=10  chunk bytes=6,509,787  delta=+3,001,972
  the tip's values are unchanged (True) but the store grew by a full dataset
```

"Is this a no-op?" is a question about which chunk coordinates your code wrote.

### A readonly session never updates

By design, and it is 0302's whole subject. A service that opens a session at
startup serves the startup state forever. Open a new session to pick up commits;
cache on the snapshot id if the open cost matters.

A long-held session also pins its snapshot against expiry, and retention does not
know about your readers.

### A fresh repository has no zarr group

`xr.open_zarr` on a repository where nothing has been committed to `main` raises
`GroupNotFoundError` from the zarr layer. That is correct — a root snapshot is not
a group. 0102's `peek` and 0402's `committed_periods` both handle it.

### `diff` is directional and strict

`from` must be an ancestor of `to`. Backwards raises, and so does comparing a
snapshot with itself:

```text
  backwards, newer -> older  raises InvalidInputError: session error: `to` snapshot ancestry doesn't include `from`
  a snapshot against itself  raises InvalidInputError: session error: `to` snapshot ancestry doesn't include `from`
```

For branch-versus-branch, find the fork point and diff each side from it.

### `reset_branch` is not a merge

It moves a pointer. Commits that were on the branch and are not ancestors of the
new target become unreachable from that branch. Pass `from_snapshot_id=` to make
the move conditional on the branch not having drifted.

### Tags are immutable but deletable

`create_tag` on an existing name raises `AlreadyExistsError`. But `delete_tag`
exists, and `expire_snapshots(delete_expired_tags=True)` removes tags pointing at
expired snapshots. Immutable means "cannot be repointed", not "cannot be removed".

### icechunk ships no type stubs

Every icechunk object in this project is annotated `Any`, deliberately, with a
comment where it first appears. mypy and pyright both run in strict mode and both
must pass; `Any` is how that is achieved without inventing stubs that would go
stale.

### Commit messages are the audit trail

"update" tells you nothing in six months. `ingest era5 t2m 2024-03` tells you
everything. `commit()` also takes `metadata=` for structured, machine-readable
provenance alongside the prose.

### Timestamps come from the writer's clock

`written_at` is UTC and monotonic within one process. Across machines with skewed
clocks it is a hope, not a guarantee — which matters before writing a retention
policy that keys off it.

---

## How this maps to open-climate-service

[OCS](https://github.com/dhis2/open-climate-service) is a climate data platform:
each instance is scoped to one country, ingests from sources like CHIRPS and
ERA5, stores results as GeoZarr in icechunk, and exposes them through STAC, Zarr
over HTTP, and openEO. Its storage layer is icechunk, and this project exists to
learn that layer rather than to reimplement it.

### One repository per dataset

Every dataset lives at:

```text
{data_dir}/downloads/{dataset_id}.icechunk
```

One repository per dataset, not one repository holding many datasets. That has
consequences worth being explicit about:

- **Retention is per-dataset.** A precipitation dataset that gets reissued nightly
  and a temperature dataset that only ever appends need different retention
  policies, and separate repositories let them have them.
- **Conflicts are per-dataset.** Two ingests targeting different datasets cannot
  conflict, because they are compare-and-swapping different branch pointers.
  That is a real reduction in the concurrency surface.
- **A dataset can be deleted by deleting a directory.** No cross-references to
  untangle.
- **Chunks are not shared across datasets.** Two datasets on the same grid store
  their coordinate arrays separately. Given how small coordinate arrays are —
  0501 reports 1,558 inlined bytes for a time coordinate — this is not a cost
  worth worrying about.

The `open_repo` helper mirrors OCS's `open_or_create_repo` closely enough that
reading one explains the other:

```python
def open_repo(path: Path | str) -> Any:
    path = Path(path)
    storage = icechunk.local_filesystem_storage(str(path))
    if path.exists():
        return icechunk.Repository.open(storage)
    path.parent.mkdir(parents=True, exist_ok=True)
    return icechunk.Repository.create(storage)
```

The `.icechunk` suffix is convention. It signals "this directory is a repository,
do not treat it as a zarr tree" to anyone who finds it with `ls`.

### Per-period commits

OCS ingests one period at a time and commits each. That is
[`0401_append_periods.py`](../../icechunk/examples/0401_append_periods.py)
exactly:

```python
session = repo.writable_session("main")
month.to_zarr(session.store, append_dim="time", consolidated=False, align_chunks=True)
session.commit(f"ingest {period}")
```

The properties this buys, each of which has an example behind it:

- **A per-period audit trail.** The history is one row per period, with the
  message and timestamp (0103). "Which periods arrived, when" is answered by the
  store rather than by a log.
- **A failure loses only the period in flight.** Everything before it is
  committed and everything after it has not started (0301). There is no partial
  period.
- **A reader always sees whole periods.** Never half of March (0102, 0302).
- **The commit cost is one period.** Not one dataset (0501).

The alternative — one commit for a whole run — loses all four. It is not
obviously worse until an ingest fails on the eleventh of twelve months.

### Resume from committed time steps

OCS restarts ingests, and the restart derives its work from the data:

```python
def committed_periods(repo: Any) -> set[str]:
    try:
        ds = read_dataset(repo)
    except Exception:
        return set()
    times = pd.to_datetime(np.asarray(ds.time.values))
    return {f"{ts.year:04d}-{ts.month:02d}" for ts in times}
```

That is [`0402_resume.py`](../../icechunk/examples/0402_resume.py), and the
reasoning is worth restating because it is a design principle rather than a
trick.

Any bookkeeping stored beside the data — a status row, a marker file, a log line
— is a second write. A crash can land between the data write and the bookkeeping
write, in either order, and afterwards the two disagree. Nothing can tell you
which is right.

The committed time coordinate cannot disagree with the data, because it *is* the
data. A period appears in that set if and only if the commit that wrote it
landed. There is no window.

The consequences OCS gets for free:

- **Restarts are safe** — a killed job restarts and picks up exactly where the
  commits stopped.
- **Reruns are no-ops** — a scheduler that double-fires, an operator who reruns
  to be sure, a retry after a network blip. All harmless.
- **No status table to maintain**, and therefore none to get out of sync.

The same pattern appears in `climate-pipeline/examples/0103_resume.py`, applied
inside the full pipeline.

### Corrections when upstream reissues

Providers reissue data, and OCS republishes the period.
[`0403_rewriting_history.py`](../../icechunk/examples/0403_rewriting_history.py)
is that workflow: locate the period on the time axis, region-write the corrected
values, commit. The old snapshot stays readable, so "what did the service return
last Tuesday" keeps its exact answer.

Which for a health service is not an academic question. A report that drove a
decision was generated against some state of the data; being able to reproduce
that state is the difference between explaining an anomaly and arguing about it.

The cost is on the other side of the same coin: every reissue keeps a full copy
of that period's chunks alive, and a nightly-reissuing feed accumulates one copy
per night. That is what makes
[`0502_expiry_and_gc.py`](../../icechunk/examples/0502_expiry_and_gc.py) an
operational necessity rather than a curiosity, and it is why a retention window
plus tags on the snapshots that must outlive it is the policy shape to reach for.

### The planned S3 migration

OCS calls `icechunk.local_filesystem_storage` and nothing else today. That is
correct for its current deployment — a single instance, one ingest process, one
API process, one committer.

It stops being adequate at a specific point, and [Storage](../storage.md) works
through exactly where. The summary:

**The concurrency argument.** A commit is compare-and-swap on a branch pointer.
Object stores provide the conditional write it needs; POSIX does not, portably.
One committer at a time makes local storage correct; a second committer makes it
a race. Note that this is about *committers*, not writers — icechunk's fork/merge
model (<https://icechunk.io/en/latest/parallel/>) has many writers and one
committer, so a dask cluster is one committer.

**The shared-path argument, which bites first.** Every participant must reach the
storage under the same identifier. A dask graph carries one path string, used by
the client that built it and every worker that executes it. A local path only
satisfies that when everything runs on one machine.
`dask-distributed/examples/0303_distributed_xarray.py` demonstrates the failure —
`FileNotFoundError` on a path the workers can see perfectly well — and
`0302_shared_storage.py` shows a file the client just wrote reporting `False` on
all three workers. An `s3://bucket/store.zarr` URL dissolves this, because it
resolves identically everywhere while staying one string in the graph.

**What the migration costs in code.** One line:

```python
storage = icechunk.local_filesystem_storage(f"{data_dir}/downloads/{dataset_id}.icechunk")
storage = icechunk.s3_storage(bucket=bucket, prefix=f"downloads/{dataset_id}", region=region)
```

**What it costs operationally.** More, and it is worth planning for:

- Latency per operation goes from microseconds to milliseconds, so chunk sizes
  should go up and the object count starts to matter.
- Requests and storage cost money, which turns 0501's chunk-sharing behaviour from
  trivia into a budget line and 0502's retention from a curiosity into routine
  maintenance.
- Conflicts become genuinely possible, so 0303's rebase paths become code the
  ingest needs rather than a lesson it read.
- Credentials and endpoints become configuration that must be right in the API
  process and in every worker image.

Three findings from this project bear directly on the move, and all three are
measured rather than assumed:

1. **The warning is the argument.** Local storage warns on every open that it is
   unsafe for concurrent commits. That is not noise to silence; it is the
   migration case stated by the library itself.
2. **Chunks are shared by reference, not by content hash** (0501). Appends cost
   one period each. A rewrite with identical values costs a full copy. On metered
   storage that distinction is a bill.
3. **Two appends to one dimension cannot be rebased** (0303). If the deployment
   ever has two ingest processes, that is the failure they will hit, and the fix
   is architectural — one appender, or fork/merge — not a solver argument.

---

## Where to go next

- **[climate-pipeline](climate-pipeline.md)** — the capstone. It puts this
  storage layer inside a full miniature climate service: a messy source
  normalized, ingested one period per commit, derived into climatologies and
  indices, and published with GeoZarr attributes and a STAC collection whose
  extents are read back off the store. `0102_streaming_ingest` and `0103_resume`
  are 0401 and 0402 in their natural habitat.
- **[Storage](../storage.md)** — the local-versus-object-store argument in full:
  why a commit needs a conditional write, why one committer makes local storage
  correct, why fork/merge means a dask cluster is one committer, why the
  shared-path constraint forces the move before the concurrency one does, and a
  decision table you can read your own deployment off.
- **[reference/icechunk.md](../reference/icechunk.md)** — the generated API
  reference for `playground_icechunk.helpers`, with signatures and full
  docstrings for `open_repo`, `write_dataset`, `read_dataset`,
  `describe_history`, `climate_dataset`, and `quiet_icechunk_logs`.
- **[Open Climate Service](../open-climate-service.md)** — the mapping across all
  five projects, and the groundwork for the two planned extensions.
- **[The stack](../stack.md)** — what each layer does and where it stops, if you
  arrived here without reading xarray and dask first.

---

## Further reading

**icechunk**

- <https://icechunk.io/> — the project site. The version-control guide, the
  configuration reference, and the full Python API.
- <https://icechunk.io/en/latest/parallel/> — parallel and distributed writes: the
  fork/merge model, with the dask and cubed integrations. Read this before
  planning any multi-worker ingest.
- <https://icechunk.io/en/latest/spec/> — the on-disk format specification. Worth
  reading if you want the manifest and snapshot structure precisely rather than
  by the summary in [What is actually on disk](#what-is-actually-on-disk).
- <https://icechunk.io/en/latest/virtual/> — virtual chunks: referencing existing
  NetCDF and HDF5 files from a repository without copying them. This is the
  `virtual_bytes` field that reads zero in 0501's output.
- <https://github.com/earth-mover/icechunk> — the source, in Rust with Python
  bindings. The issue tracker is the fastest way to find out whether a behaviour
  is intended.

**Zarr**

- <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html> — the Zarr v3
  core specification: the store interface, the chunk grid, metadata documents, and
  the codec pipeline. This is the contract icechunk implements.
- <https://zarr.readthedocs.io/> — zarr-python. Array creation, encoding, codecs,
  and the built-in store implementations that `IcechunkStore` sits beside.
- <https://zarr.dev/> — the Zarr community, the specification process, and the
  implementations in other languages.

**xarray**

- <https://docs.xarray.dev/> — the library reference.
- <https://docs.xarray.dev/en/stable/user-guide/io.html#zarr> — the Zarr I/O guide,
  which is where `append_dim`, `region`, `align_chunks`, `encoding`, and
  `consolidated` are documented properly. Every one of those appears in this
  project's examples.

**Related projects here**

- `xarray/` — the data model these datasets are expressed in.
- `dask/` — chunking, blocked algorithms, and `0601_zarr_legal_chunks.py`, which
  covers the other half of the chunk-alignment family of problems.
- `dask-distributed/` — a real cluster in containers, and the shared-storage
  failures that make the object-store argument concrete.
- `climate-pipeline/` — all of it assembled into one pipeline.








