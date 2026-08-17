# Storage: do you actually need S3?

This is the question worth understanding properly, because the answer is not
"yes" and it is not "no" — it depends on a specific property, and knowing which
one tells you exactly when the local filesystem stops being adequate.

## What a commit actually does

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
`icechunk/` do silence it, via `quiet_icechunk_logs()`, because they are
single-writer by construction and the warning would drown the lesson — the
helper's docstring says so.)

## The rule

**One committer at a time makes local filesystem storage correct.** More than
one makes it a race.

Note the emphasis on *committer*, not *writer*. That distinction is what makes
distributed writes possible without an object store, and it is worth
understanding before deciding anything.

## How distributed writes actually work

The natural worry is that a dask cluster means many machines writing to one
store, and therefore many committers. That is not how icechunk does it.

The model is **fork/merge**, and it looks like this:

```python
session = repo.writable_session("main")      # coordinator opens one session
forks = [session.fork() for _ in workers]    # one serializable child each
# ... each ForkSession is pickled to a worker, which does all its writes ...
session.merge(*returned_forks)               # coordinator merges the change sets
session.commit("ingest 2024-05")             # exactly ONE commit
```

Workers write chunk objects directly to storage — that part is genuinely
distributed and parallel. But every worker returns its change set to the
coordinator, which merges them and performs a single compare-and-swap. Many
writers, one committer.

So the concurrency hazard is not "distributed dask", it is "two independent
jobs committing to the same branch at once" — a second ingest, a manual fix
running while a scheduled sync is in flight, two replicas of the same service.

## The constraint that actually forces object storage

There is a second, more mundane reason to move, and in practice it bites first.

**Every participant must reach the same storage under the same identifier.**

A dask graph carries one path string, used by the client that built it and by
every worker that executes it. A local path satisfies that only when every
participant runs on the same machine — or on the same network mount, which
brings back the conditional-write problem and adds latency.

This is not theoretical. Building `dask-distributed`, a lazy zarr pipeline
driven from the host failed with `FileNotFoundError: /data/source.zarr` — the
workers could open it happily, the client could not see it at all, and one path
string cannot mean two things.
`dask-distributed/examples/0303_distributed_xarray.py` demonstrates the failure
and the two shapes that do work: push the whole job to one worker (correct, but
you have thrown away the cluster), or keep the data in the graph and never
touch storage.

An `s3://bucket/store.zarr` URL dissolves the problem, because it resolves
identically everywhere — each side configuring its own endpoint and
credentials, while the identifier in the graph stays the same string.

## The decision table

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
single-instance service on one box is a legitimate, correct deployment. It
stops being adequate the moment compute is spread across machines or a second
writer appears — and both of those arrive together the day you deploy a real
cluster.

## What changes when you migrate, and what does not

Reassuringly little changes in code:

```python
storage = icechunk.local_filesystem_storage("/data/temperature.icechunk")
storage = icechunk.s3_storage(bucket="climate", prefix="temperature", region="eu-west-1")
```

The `Repository` API is identical across every backend icechunk ships —
`s3_storage`, `gcs_storage`, `azure_storage`, `r2_storage`, `tigris_storage`,
`http_storage`, `in_memory_storage`. Sessions, commits, branches, tags,
ancestry, expiry, and garbage collection all behave the same. Everything the
`icechunk` project teaches against the local backend transfers unchanged.

What does change is the operational envelope:

- **Latency per operation rises**, from microseconds to milliseconds. Chunks
  should be larger than you would choose on local disk, and the number of tiny
  objects starts to matter.
- **Storage costs money and requests cost money.** The chunk-sharing behaviour
  in `icechunk/examples/0501_storage_growth.py` stops being trivia: a rewrite
  that duplicates a dataset is now a line on a bill. Expiry and garbage
  collection (`0502_expiry_and_gc.py`) become routine maintenance rather than a
  curiosity.
- **Conflicts become real.** With concurrent committers actually possible, the
  rebase paths in `icechunk/examples/0303_conflicts.py` turn into code you
  need: disjoint chunk edits rebase cleanly, two appends to the same dimension
  do not, and nothing resolves a doubly-updated array shape automatically.
- **Credentials and endpoints become configuration** that must be right in the
  API process and in every worker image — the same discipline as version
  pinning, and it fails the same way when it is wrong.
