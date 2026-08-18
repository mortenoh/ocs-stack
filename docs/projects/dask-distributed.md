# dask-distributed

**A real cluster.** A dask scheduler and three worker containers run with
Docker Compose, driven by fifteen examples that execute on the host and talk to
the containers over TCP. This is the project where dask stops being a library
you import and becomes a system you operate: work crosses a process boundary
and a network, so every argument must serialize, every file path must be valid
on the worker rather than on the client, workers die and get replaced
mid-computation, and the libraries installed in the container must match the
ones on your laptop. None of those constraints exist on a `LocalCluster`, which
is exactly why a `LocalCluster` is a poor rehearsal for a deployment.

```bash
cd dask-distributed
make install
make up                       # build images, start scheduler + 3 workers
make run EXAMPLE=0101_connect
make run-all                  # every example, in order
make dashboard                # http://127.0.0.1:8787/status
```

!!! note "Where the numbers on this page came from"

    Every output block below was produced by running the example against the
    Compose cluster described here — one scheduler and three worker containers,
    two threads and 1.5 GiB each, on a single macOS host on 2026-08-17. Timings
    are machine-dependent and will not reproduce exactly; the shapes, the
    ratios, and the ordering are the parts worth reading. Where an output is
    long it is trimmed, and the trim is marked. Nothing here is invented: if a
    number appears in a fenced block, it came out of a program.

---

## Introduction to dask.distributed

This section assumes you already know basic dask — that a dask array is a grid
of chunks plus a task graph, that `.compute()` is when the work happens, and
that chunk size is the tuning dial that matters most. It assumes you have never
run a cluster. Everything below is about what changes when the thing executing
your graph stops being a thread pool inside your own process.

Upstream references worth having open alongside this page:

- [distributed.dask.org](https://distributed.dask.org/) — the `dask.distributed`
  documentation proper: client API, scheduling policies, worker internals,
  memory management, and the API reference for everything named here.
- [docs.dask.org/en/stable/deploying.html](https://docs.dask.org/en/stable/deploying.html)
  — the deployment landscape: `LocalCluster`, SSH, Kubernetes, HPC job queues,
  cloud, and the manual `dask scheduler` / `dask worker` command-line route this
  project uses.
- [docs.dask.org/en/stable/dashboard.html](https://docs.dask.org/en/stable/dashboard.html)
  — panel-by-panel guide to the diagnostic dashboard, which phase 5 of this
  project reads programmatically instead of by squinting.

### What changes when the scheduler is a separate process

With the threaded or the multiprocessing scheduler, `.compute()` is a function
call. It walks your graph, hands ready tasks to a pool, collects results, and
returns. The graph, the intermediate values, and the final answer all live in
one process's memory. Nothing is copied unless the multiprocessing scheduler
forces it, nothing is sent anywhere, and nothing can fail in a way that is not
an ordinary Python exception in an ordinary Python stack.

`dask.distributed` replaces that function call with a conversation between
processes. When you call `.compute()` against a distributed client:

1. Your process serializes the graph and sends it to the scheduler, over a
   socket.
2. The scheduler — a separate program, possibly on a separate machine — decides
   which worker should run which task, in what order, and tells each worker
   about the tasks it has been assigned.
3. Each worker deserializes the task, runs it in a thread, keeps the result in
   its own memory, and reports back to the scheduler that the key is now
   available and how many bytes it took.
4. When a task needs an input that lives on a different worker, the two workers
   talk to each other directly and copy the bytes across.
5. When the final result is ready, the scheduler tells your client where it is,
   and your client pulls it back over the socket.

Every one of those five steps is a place where something new can go wrong, and
each corresponds to a lesson in this project. Step 1 fails when your function
or your arguments cannot be pickled (`0201_serialization`). Step 2 is where
placement decisions get made, well or badly (`0203_locality`). Step 3 is where
a worker's fixed memory limit turns into spilling, pausing, and death
(`0301_worker_memory`), and where a missing library on the worker becomes a
`ModuleNotFoundError` that surfaces on the client as a confusing task error
(`0102_versions`). Step 4 is the transfer time that shows up as a colour in the
task stream (`0503_task_stream`). Step 5 is where a result you thought was on
the cluster turns out to be 8 GB that will not fit in your laptop.

There is a sixth thing that changes, and it is the one that reorients how you
think: **the cluster outlives your call.** A `LocalCluster` inside a
`with` block is scoped to the block. A real cluster is a service. It was
running before your script started and it will be running after your script
exits, holding whatever data you left on it, with whatever workers happen to be
alive at that moment. Your script is a client of a system, not the owner of a
thread pool.

### The architecture: client, scheduler, worker, nanny

Four kinds of process participate. Naming them precisely is worth doing once,
because the documentation and the error messages assume you know which is
which.

```text
    your Python process                      the cluster
   +--------------------+
   |  Client            |  tcp://...:8786   +--------------------+
   |   - holds graphs   | <---------------> |  Scheduler         |
   |   - holds Futures  |                   |   - task states    |
   |   - gathers results|                   |   - who has what   |
   +--------------------+                   |   - dashboard :8787|
                                            +--------------------+
                                              ^        ^        ^
                                              |        |        |
                        +---------------------+        |        +---------------------+
                        |                              |                              |
             +---------------------+       +---------------------+       +---------------------+
             | Nanny (container 1) |       | Nanny (container 2) |       | Nanny (container 3) |
             |  +---------------+  |       |  +---------------+  |       |  +---------------+  |
             |  | Worker        |  |       |  | Worker        |  |       |  | Worker        |  |
             |  |  2 threads    |<--------->|  2 threads    |<--------->|  2 threads    |  |
             |  |  1.5 GiB      |  |       |  |  1.5 GiB      |  |       |  |  1.5 GiB      |  |
             |  +---------------+  |       |  +---------------+  |       |  +---------------+  |
             +---------------------+       +---------------------+       +---------------------+
                    worker-to-worker transfers go directly, not through the scheduler
```

**The client** is the object you get from `Client("tcp://127.0.0.1:8786")`. It
lives in your process. It owns the task graphs you build, it owns the `Future`
objects you hold, and it is the reference-counting authority for anything you
asked the cluster to keep. When your client disconnects, the scheduler releases
everything only that client was holding. Importantly, the client is *not* a
passive handle: calling `Client(...)` also installs itself as the default
scheduler for the whole process, so a bare `dask_array.compute()` anywhere in
your code now goes to the cluster whether or not you pass the client
explicitly.

**The scheduler** is a single process holding the entire state of the cluster:
every task, its dependencies, its state (`released`, `waiting`, `processing`,
`memory`, `erred`), which worker holds which key, how much memory each worker
is using, and a rolling history of what has happened. It makes every placement
decision. It is single-threaded and event-loop driven, which is why the
scheduler can become a bottleneck for graphs with very many very small tasks —
its per-task overhead is small but not zero, on the order of a few hundred
microseconds, and a million-task graph makes that number matter. The dashboard
is served by the scheduler, from the same process, which is why "the dashboard
answers" is a good proxy for "the scheduler event loop is alive" — and exactly
what this project's healthcheck uses.

**A worker** is a process that runs tasks in a thread pool and stores results
in a local dictionary. It has a thread count (concurrency) and a memory limit
(capacity). It talks to the scheduler over one persistent connection, and to
other workers directly when it needs data they hold. Two things about workers
routinely surprise people. First, the threads share one Python process, so
GIL-bound work does not parallelize inside a worker even though it parallelizes
across workers — this is why "more workers with fewer threads each" is
sometimes the right shape. Second, a worker's memory is *its own*: two workers
holding the same array hold two copies, and the sum of worker memory is not a
shared pool you can draw against.

**A nanny** is a supervisor process that owns exactly one worker process as a
child. It starts the worker, watches it, and if the worker exits — crash, OOM
kill, `os._exit`, segfault in a C extension — the nanny starts a fresh one. The
`dask worker` command starts a nanny by default; `--no-nanny` turns that off.
The nanny is why a worker crash is usually a hiccup rather than permanent
capacity loss, and it is directly visible in this project's container logs:

```text
worker-2  | 2026-08-17 17:48:42,748 - distributed.nanny - INFO - Worker process 120 exited with status 1
worker-2  | 2026-08-17 17:48:42,748 - distributed.nanny - INFO - Unregistering worker (status=Status.running)
worker-2  | 2026-08-17 17:48:42,749 - distributed.nanny - WARNING - Restarting worker (status=Status.running)
worker-2  | 2026-08-17 17:48:43,094 - distributed.worker - INFO -       Start worker at:     tcp://172.19.0.3:34041
```

Note that the nanny is a *process* supervisor, not a container supervisor. When
`0401_worker_failure` kills a worker, `docker compose ps` still reports the
container as having been up for hours. Nothing restarted at the Docker level;
the nanny replaced a child process inside a container that never noticed.

One more participant deserves a mention because it explains a class of
confusing behaviour: **the scheduler is not in the data path.** When worker A
needs a chunk that worker B holds, A opens a connection to B and pulls it. The
scheduler only says "B has it". This is why a cluster can be moving gigabytes
while the scheduler looks idle, and why the dashboard's bandwidth panel is
per-worker-pair rather than a single number.

### What a Future is

A `Future` is a receipt for a computation that the cluster owns.

```python
future = client.submit(expensive, 42)   # returns immediately
future.status                           # "pending"
future.result()                         # blocks, returns the value
```

Three properties make it different from `concurrent.futures.Future`, and each
has consequences.

**It is a remote reference, not a container.** The result lives in a worker's
memory. `future.result()` copies it to your process; until you call that, the
value never touches your machine. This is what lets you chain work on the
cluster without round-tripping data through the client: passing a future as an
argument to another `submit` tells the scheduler "this task depends on that
key", and the scheduler will prefer to run the new task on the worker that
already holds it.

**It is reference-counted, and your variable is the reference.** When the last
`Future` pointing at a key goes out of scope on the client, the client tells
the scheduler, and the scheduler tells the workers to drop the data. This is
the mechanism behind `del persisted` freeing cluster memory in
`0301_worker_memory`, and behind the corresponding leak: a persisted result you
stash in a module-level list is a cluster-wide memory leak that outlives the
computation that created it.

**It has a key, and the key is content-addressed by default.** `submit` hashes
the function and its arguments to produce a deterministic key, so submitting
the same call twice returns the same future and runs the work once. That is
usually what you want and occasionally exactly what you do not: a function with
side effects, or a deliberately flaky one, needs `pure=False` to be re-run
rather than deduplicated. Several examples here pass `pure=False` for precisely
that reason.

The lifecycle is worth memorizing because `future.status` is the cheapest
non-blocking diagnostic in the whole system:

| status | meaning |
|---|---|
| `pending` | submitted; queued, running, or waiting on dependencies |
| `finished` | the result is in some worker's memory |
| `error` | the task raised; `result()` will re-raise, `traceback()` has the worker's stack |
| `cancelled` | the future was cancelled, or the cluster lost it without a recipe |

`0402_errors_and_retries` prints exactly this transition:

```text
  submit() returned immediately; future.status is now 'pending'
  after the task ran, status is 'error'
  result() re-raises it here: ValueError: this task was never going to work (value=42)
```

### Collections versus futures

There are two ways to give a distributed cluster work, and they meet in the
middle.

**Collections** are the dask API you already know: `dask.array`,
`dask.dataframe`, `dask.bag`, `dask.delayed`, and everything xarray builds on
top of them. You build a lazy object, call `.compute()`, and the client ships
the graph to the scheduler. The graph is the unit of submission — thousands of
tasks in one message. This is the right shape for data-parallel work with
structure: a climatology, a rechunk, a reduction over a grid.

**Futures** are the imperative API: `submit`, `map`, `gather`, `scatter`. You
control each task individually, results come back as they finish, and you can
make scheduling decisions in Python based on what has already completed. This
is the right shape for work that is not a neat array operation: fan out over
1,000 files, run a parameter sweep, drive a pipeline where each step's shape
depends on the last step's answer.

They interoperate in both directions, which is the part worth internalizing:

- `client.futures_of(persisted_array)` gives you the `Future` for every chunk of
  a persisted collection. `0203_locality` and `0301_worker_memory` both use this
  to block until every chunk is genuinely resident before inspecting the
  cluster.
- `client.scatter(numpy_array)` gives you a `Future`, and `dask.array.from_delayed`
  or `client.submit` can build collections on top of futures.
- `persist()` on a collection is, in effect, "submit this graph and hand me back
  a collection whose chunks are futures". `compute()` is `persist()` plus
  `gather()`.

The distinction that matters operationally: **`compute()` brings the answer to
your process, `persist()` leaves it on the cluster.** Calling `.compute()` on a
100 GB dask array against a real cluster does not fail with a nice message
about chunk sizes; it tries to assemble 100 GB in your client process. Reach
for `persist()` when you want the data to stay out there and `compute()` only
for things that are small by construction — a scalar, a reduction, a plot's
worth of numbers.

### Why a LocalCluster hides the interesting problems

`LocalCluster` is genuinely useful. It gives you the distributed scheduler's
work-stealing, the dashboard, the task stream, and the futures API on one
machine, and for many workloads it is the correct production answer. It is a
poor *teacher*, though, and the reason is structural rather than incidental.

A `LocalCluster` with `processes=False` — which is what this project's fallback
uses — runs its "workers" as threads inside your own process. That means:

- **Arguments are not serialized.** Passing a 500 MB DataFrame to a task costs a
  pointer copy. The single most common distributed performance bug is invisible.
- **The filesystem is shared, trivially.** `to_zarr("/tmp/out.zarr")` works. The
  same line against a real cluster writes into three different container
  filesystems, or nowhere useful.
- **Library versions are identical by construction.** There is exactly one
  Python process, so client and worker cannot disagree.
- **Worker death is not survivable and not simulatable.** Killing a thread-worker
  takes your script with it, so you cannot rehearse the failure at all.
- **Memory limits are soft or absent.** There is no separate address space to
  bound; the "limit" reported is your machine's RAM.

A `LocalCluster` with `processes=True` recovers some of this — real
serialization, real memory limits, real worker death — but keeps the shared
filesystem and the shared environment, which are two of the four things that
actually break deployments. That is why this project ships containers.

The consolidated section [The four things a LocalCluster hides](#the-four-things-a-localcluster-hides)
below works through each with the evidence from the examples.

### What "distributed" actually costs

Distribution is not free, and the bill arrives in four currencies. Knowing the
rough size of each is what lets you predict whether a cluster will help before
you build one.

**Latency per task.** A trivial task with an integer argument round-trips in
about 8 ms against this cluster (`0201_serialization`, machine-dependent). On
the threaded scheduler the same call is microseconds. That floor is scheduling
overhead: client-to-scheduler, scheduler-to-worker, execute, report back,
gather. It means a graph of a million 1 ms tasks is not a distributed workload
— it is a scheduler stress test. Tasks should be big enough that the
scheduler-side bookkeeping and the network hop are noise, which in practice
means aiming for tasks of at least tens of milliseconds and preferably
hundreds.

**Bytes on the wire.** Every argument is pickled and pushed through a socket.
An 8 MB numpy array shipped as an argument turned a 20 ms task into a 77 ms one
— about 3.9x — and that is on loopback TCP between containers on one machine,
the friendliest network that exists. Across a real network it is worse.
`0202_scatter_gather` shows the multiplier version of the same bug: the same
array needed by 12 tasks took 400 ms shipped per-task versus 82 ms scattered
once.

**Memory that is not pooled.** Three workers with 1.5 GiB each is not 4.5 GiB
of usable space for one array chunk. It is three separate 1.5 GiB budgets, and
a single chunk must fit comfortably inside one of them alongside everything
else that worker is holding. The practical planning number is roughly
`limit / threads` per concurrently-executing task, minus room for inputs,
outputs, and unmanaged memory.

**Operational surface.** A cluster is software you now run. It has versions
that must match your client's, images that must be built and pushed, a
scheduler that is a single point of failure, ports, logs, and a dashboard
somebody has to look at. `0102_versions` exists because the base image being
one patch version behind on numpy produced a warning on every single connect —
a small thing that, left alone, trains people to ignore warnings.

Set against those costs is the one benefit: work that genuinely parallelizes
finishes in wall time proportional to `total_work / total_threads` instead of
`total_work`. `0403_scaling` measures the ladder — 24 tasks of 0.25 s took
1.04 s on 6 slots, 1.54 s on 4, and 3.06 s on 2. That is very nearly the
inverse of the slot count, which is the best case, and it only holds because
the tasks were independent and equal-sized. The moment they are not,
`0503_task_stream` shows what happens: a batch can never finish faster than its
slowest single task, no matter how many workers you add.

The honest summary is that a cluster buys you throughput and costs you latency,
memory locality, and operational complexity. It is worth it when the work is
big, splittable, and roughly balanced — and a liability when it is not.

---

## Setup

The project is self-contained: its own `pyproject.toml`, its own `.venv`, its
own `uv.lock`. Nothing is installed at the repository level. What is different
from every other project here is that this one also has containers, so there
are two things to start rather than one.

```bash
cd dask-distributed
make install                                 # uv sync
make up                                      # build images, start scheduler + 3 workers
make run EXAMPLE=0101_connect                # run one example
make run-all                                 # all 15, in order
make down                                    # stop the cluster
```

`make up` does not return when the containers start — it returns when the
scheduler actually answers on port 8786, by polling it through
`wait_for_scheduler`. A target that returns on container start is a target that
hands the next command a cluster that is not ready yet.

The rest of the targets, in the order you tend to need them:

| Target | What it does |
|---|---|
| `make ps` | container status |
| `make logs` | follow scheduler and worker logs |
| `make dashboard` | open <http://127.0.0.1:8787/status> |
| `make scale N=5` | change the worker count while running |
| `make restart` | recreate the containers, keeping images |
| `make test` / `make lint` / `make ci` | the same as every other project |

### Running without Docker

**Every example runs with the cluster down.** `connect()` probes the scheduler,
falls back to an in-process `LocalCluster`, and says so rather than pretending:

```text
Compose cluster not reachable at tcp://127.0.0.1:8799 -- fell back to an
in-process LocalCluster (inproc://<host>/42246/1).
  Start the real thing with: make up
```

(Captured with `DASK_SCHEDULER_ADDRESS` pointed at a port nothing was listening
on, since the cluster was up at the time; the host in the `inproc://` address
is elided. Everything else is verbatim.)

That fallback is a genuine substitute for perhaps half of what is on this page
and a poor one for the rest — a `LocalCluster` cannot show you a serialization
failure across a process boundary, a worker filesystem that differs from the
client's, or a container being killed mid-computation. The four things it
hides get their own section [below](#the-four-things-a-localcluster-hides), and
[the fallback design](#the-fallback-design-in-clusterpy) explains how it works.

Point the examples at a cluster somewhere else by setting the address:

```bash
DASK_SCHEDULER_ADDRESS=tcp://10.0.0.4:8786 make run EXAMPLE=0101_connect
```

### Pinning the image to the lockfile

The container image installs from the same `uv.lock` the host does. This is not
tidiness: a client and a worker running different library versions is a whole
class of confusing failures, and the base image was one patch behind on numpy
until this was fixed. [`0102_versions.py`](../../dask-distributed/examples/0102_versions.py)
checks it at runtime and is the example to run first when something behaves
inexplicably.

---

## The cluster in this project

The whole cluster is two files — [`compose.yml`](../../dask-distributed/compose.yml)
and [`Dockerfile`](../../dask-distributed/Dockerfile) — plus a Makefile that
wraps `docker compose` and a Python helper that decides whether to use it. This
section walks all of them, because the decisions encoded there are the same
decisions a deployment makes, only smaller.

### compose.yml, line by line

```yaml
services:
  scheduler:
    build: .
    command: ["dask", "scheduler", "--host", "0.0.0.0", "--port", "8786", "--dashboard-address", ":8787"]
```

`build: .` means the scheduler runs the *same image* as the workers. That is
deliberate and it is worth stating as a rule: the scheduler deserializes task
keys and, in some code paths, graph fragments, so it needs the same libraries.
A scheduler on a thinner image than its workers is a version-skew bug waiting
for the right graph.

`--host 0.0.0.0` binds all interfaces inside the container, which is what makes
the published port usable from the host. Binding `127.0.0.1` inside a container
is a classic way to produce a service that is up, healthy, and unreachable.

`--port 8786` is the scheduler's TCP port for clients and workers. `8786` is
dask's convention; there is nothing magic about it beyond that.

`--dashboard-address :8787` starts the bokeh diagnostic server. Note this is a
*separate* HTTP port from the scheduler's own TCP protocol port. The dashboard
is served by the scheduler process itself, which is what makes it a valid
liveness probe.

```yaml
    ports:
      - "${DASK_SCHEDULER_PORT:-8786}:8786" # client connections
      - "${DASK_DASHBOARD_PORT:-8787}:8787" # bokeh dashboard
```

Only the scheduler publishes ports. Workers do not need to be reachable from
the host at all — they connect *out* to the scheduler, and they talk to each
other over the compose network. This asymmetry is the same one you get in
Kubernetes: the scheduler is a Service, the workers are just pods.

The `${VAR:-default}` form makes both ports overridable from the environment
without editing the file, which matters if you already have something on 8787 —
and something on 8787 is common, because every `LocalCluster` on the machine
wants it.

```yaml
    volumes:
      - shared:/data
```

The scheduler gets the shared volume too, even though it never computes. That
is convenience rather than necessity: it means you can exec into the scheduler
container to inspect what the workers wrote.

```yaml
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8787/health')"]
      interval: 3s
      timeout: 5s
      retries: 20
      start_period: 5s
```

This is the most quietly important block in the file. `/health` is an endpoint
the dashboard serves; it answers `ok`:

```console
$ curl -s http://127.0.0.1:8787/health
ok
```

Because the dashboard runs on the scheduler's own event loop, a successful
`/health` means the event loop is turning — not merely that the process exists
and the port is bound. That distinction is what lets `make up` wait for genuine
readiness instead of racing the first `Client(...)` call. With
`interval: 3s` and `retries: 20`, compose will wait up to about a minute for a
cold start, and `start_period: 5s` stops early failures from counting against
the retry budget.

The reason this matters is the worker block immediately below:

```yaml
  worker:
    build: .
    command:
      ["dask", "worker", "tcp://scheduler:8786",
       "--nworkers", "1", "--nthreads", "2",
       "--memory-limit", "1.5GiB",
       "--local-directory", "/tmp/dask-worker-space"]
    depends_on:
      scheduler:
        condition: service_healthy
```

`tcp://scheduler:8786` uses compose's DNS: the service name resolves to the
scheduler container's address on the compose network. Workers dial out; the
scheduler never dials in.

`depends_on: condition: service_healthy` is the payoff for writing a real
healthcheck. Without it, `depends_on` only waits for the container to *start*,
and three workers race a scheduler that has not finished binding. Workers do
retry their connection, so the naive version usually works — but "usually
works" is the failure mode you least want in infrastructure that exists to
teach you about failure.

`--nworkers 1 --nthreads 2` is one worker process per container, with two
threads. The alternative — one container running `--nworkers 3` — would put
three worker processes behind one nanny in one container, which is a fine
deployment shape but a worse teaching one: you would not get three distinct
container hostnames, and "which container ran this" would stop being a
meaningful question.

The threads-per-worker choice is the classic tradeoff. Two threads means two
tasks run concurrently in one Python process and share its memory, so numpy
work (which releases the GIL) parallelizes and pure-Python work does not. Six
single-threaded workers would give more real parallelism for GIL-bound code and
worse data locality, since each worker's memory would be a separate island.

`--memory-limit 1.5GiB` is the number that makes phase 3 possible. It is a
*per-worker process* limit, enforced by distributed itself by watching its own
RSS, not by the container runtime. That means it is the number that triggers
spilling and pausing (see [Worker memory thresholds](#worker-memory-thresholds-target-spill-pause-terminate)),
and it should be set comfortably below whatever the container's actual cgroup
limit is — otherwise the kernel OOM-killer wins the race and distributed never
gets the chance to spill.

`--local-directory /tmp/dask-worker-space` is where a worker spills data to
disk and writes its scratch space. It shows up in the startup banner:

```text
worker-2  | 2026-08-17 14:08:41,494 - distributed.worker - INFO -       Local Directory: /tmp/dask-worker-space/dask-scratch-space/worker-unttkjo5
```

Note the per-worker subdirectory with a random suffix. Each worker process gets
its own, which is why several workers can share a directory without colliding —
and why a worker restarted by its nanny gets a *new* scratch directory rather
than inheriting the dead one's.

```yaml
    deploy:
      replicas: ${DASK_WORKERS:-3}
```

Three workers by default, overridable by environment variable. This is also
what `make scale N=5` manipulates, via `--scale worker=5`.

```yaml
volumes:
  shared:
```

A *named* volume, not a bind mount. This is the single most instructive choice
in the file, and the comment in the source says why: the examples write through
the workers, so the host never needs to see these files. The consequence is
that `/data` exists in every container and does not exist on the host at all:

```text
    client sees /data: False
```

That single `False` is the entire lesson of `0303_distributed_xarray`. A bind
mount would have made `/data` visible on the host too, which would have hidden
the constraint — and hiding constraints is what a `LocalCluster` already does
well enough.

### Dockerfile, line by line

```dockerfile
FROM ghcr.io/dask/dask:2026.7.1-py3.13
```

The upstream dask image, pinned to an exact dask version *and* an exact Python
version. Both halves matter. A different Python minor version between client
and worker changes pickle behaviour and can change what is importable; a
different dask version changes the wire protocol and the graph representation.

The tag is pinned rather than floating on `latest` for the obvious reason and
one less obvious one: `latest` means the cluster silently changes underneath
you on the next `make up --build`, and version skew that arrives without a code
change is the hardest kind to diagnose.

```dockerfile
RUN pip install --no-cache-dir \
    "xarray==2026.7.0" \
    "zarr==3.3.0" \
    "pandas==3.0.5" \
    "numpy==2.5.2" \
    "tornado==6.5.8" \
    "msgpack==1.2.1" \
    "cloudpickle==3.1.2" \
    "toolz==1.1.0"
```

Two different reasons are mixed in this list, and it is worth separating them.

**Libraries the tasks need.** `xarray` and `zarr` are here because
`0302_shared_storage` and `0303_distributed_xarray` send functions that import
them. A worker unpickles whatever the client sent; if the module is missing,
the failure happens at unpickle time and arrives on the client as a task error
with a `ModuleNotFoundError` buried in it. The general rule is blunt: **every
library used inside a submitted function must exist on the worker.** The client
importing it successfully proves nothing.

**Libraries pinned to silence version skew.** `numpy`, `tornado`, `msgpack`,
`cloudpickle`, and `toolz` all ship in the base image already. They are pinned
anyway because the base image's versions were a patch behind the client's, and
distributed compares versions on connect and warns when they differ. The
comment in the Dockerfile is explicit that this was an observed problem, not a
precaution.

Why `uv.lock` is the source of truth for these pins: the client environment is
resolved by `uv` from `pyproject.toml`, and `uv.lock` records the exact
versions that resolution produced. Those are the versions your client will
actually pickle with. Copying them into the Dockerfile makes the cluster match
the client by construction rather than by hope, and it turns "bump the client"
into a two-line change that fails loudly if you only do one half. The result is
a clean comparison table from `0102_versions`:

```text
  package        client           scheduler        workers          match
  -------------- ---------------- ---------------- ---------------- -----
  python         3.13.14.final.0  3.13.14.final.0  3.13.14.final.0  yes
  dask           2026.7.1         2026.7.1         2026.7.1         yes
  distributed    2026.7.1         2026.7.1         2026.7.1         yes
  numpy          2.5.2            2.5.2            2.5.2            yes
  pandas         3.0.5            3.0.5            3.0.5            yes
  cloudpickle    3.1.2            3.1.2            3.1.2            yes
  msgpack        1.2.1            1.2.1            1.2.1            yes
  toolz          1.1.0            1.1.0            1.1.0            yes
```

A quiet connect is a feature somebody built. It is not what you get by default.

```dockerfile
RUN mkdir -p /data
```

Creating the mount point in the image rather than relying on Docker to create
it means the directory exists with sensible ownership even if the volume is
never mounted — a small robustness detail that also documents the contract.

Finally, [`.dockerignore`](../../dask-distributed/.dockerignore) excludes
`.venv/`, `examples/`, `tests/`, and the various caches. The image needs none
of them: examples run on the *host*, not in the containers. Sending a host
`.venv` into a Linux image would be both large and useless.

### The Makefile targets

```console
$ make up
>>> Building and starting the cluster
[+] Running 5/5 ...
>>> Waiting for the scheduler to accept connections
scheduler ready at tcp://127.0.0.1:8786
```

`make up` is `docker compose up -d --build` followed by `make wait`, and `wait`
runs the project's own `wait_for_scheduler()` helper. That second step is not
redundant with the healthcheck: the healthcheck proves the scheduler is healthy
*inside the compose network*, while `wait_for_scheduler()` proves it is
reachable *from the host*, through the published port. Those are different
claims, and on Docker Desktop the port publishing genuinely lags container
health by a moment.

| target | what it does | when you want it |
|---|---|---|
| `make up` | build images, start scheduler + workers, block until reachable from the host | start of a session |
| `make down` | `docker compose down -v` — stops everything *and deletes the volume* | end of a session, or to reset `/data` |
| `make ps` | `docker compose ps` — container status and published ports | "is it running?" |
| `make logs` | `docker compose logs -f` — follow scheduler and worker logs together | anything unexplained |
| `make scale N=5` | `docker compose up -d --no-recreate --scale worker=5` | more or fewer workers, live |
| `make dashboard` | opens `http://127.0.0.1:8787/status` in a browser | always, while a job runs |
| `make wait` | block until the scheduler answers on the host | inside scripts and CI |

Two of these have sharp edges worth naming.

**`make down` removes the volume.** The `-v` is deliberate — the shared volume
is scratch space for examples, and leaving stale zarr stores around makes
reruns non-deterministic. But it does mean `down` is destructive, and anything
you wrote to `/data` and care about needs copying out first.

**`make scale N=5` is not symmetric with the client.** Scaling *up* adds
containers, which start nannies, which start workers, which register with the
scheduler; nothing else is required. Scaling *down* with `--scale worker=2`
stops containers, which is a hard stop, not a drain: whatever data those
workers held is gone. If it was computed from a graph, the scheduler will
rebuild it; if it was `scatter`ed, it is lost. The graceful version is
`client.retire_workers()`, which moves data off first — covered in
`0403_scaling` below. `--no-recreate` is there so scaling does not restart the
workers that are already running and healthy.

For running examples:

```console
$ make run EXAMPLE=0101_connect
$ make run-all
```

`run-all` loops over `examples/*.py` in sorted order and stops at the first
failure. Because every example is self-contained and cleans up after itself, it
is safe to run repeatedly against a long-lived cluster — with the caveat that
`0401_worker_failure` genuinely kills a worker process each time, so a `run-all`
leaves you with one worker whose process is a few minutes younger than the
others.

### The fallback design in cluster.py

[`src/ocs_stack_dask_distributed/cluster.py`](../../dask-distributed/src/ocs_stack_dask_distributed/cluster.py)
is 200 lines whose entire job is to answer one question — "is there a cluster?"
— and to make the answer not matter for whether an example runs.

The repository convention is that an example depending on a service must still
run, and explain itself, when the service is not configured. For this project
that means every example must work on a machine with no Docker. The mechanism
is `connect()`:

```python
def connect(address: str = SCHEDULER_ADDRESS, *, allow_fallback: bool = True) -> ClusterSession:
    from distributed import Client, LocalCluster

    if scheduler_reachable(address):
        return ClusterSession(client=Client(address), mode="compose", address=address)

    if not allow_fallback:
        raise ConnectionError(f"no scheduler at {address}; start the cluster with: make up")

    cluster = LocalCluster(n_workers=FALLBACK_WORKERS, threads_per_worker=2, processes=False, ...)
    client = Client(cluster)
    return ClusterSession(client=client, mode="local", address=str(cluster.scheduler_address))
```

The probe is the interesting part, because the obvious implementation is wrong:

```python
def scheduler_reachable(address: str = SCHEDULER_ADDRESS, timeout: float = 1.0) -> bool:
    try:
        host, port = _split_address(address)
    except ValueError:
        return False
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False
```

A plain TCP socket, not a dask `Client(..., timeout=...)`. The reason is
responsiveness: constructing a `Client` against a dead address goes through
distributed's connection machinery, with its own retry and backoff, and takes
seconds before it gives up. A raw socket connect to a port nobody is listening
on fails in milliseconds — instantly on loopback, where the kernel returns
`ECONNREFUSED` without any network involved. The difference between "the
example starts immediately" and "the example hangs for ten seconds before
printing anything" is the difference between a fallback people accept and one
they work around.

`_split_address()` handles both `tcp://host:port` and bare `host:port`, and
raises `ValueError` on anything without a numeric port. It uses `rpartition`
rather than `split` so that IPv6-ish and scheme-prefixed forms do not confuse
it, and `scheduler_reachable` turns that `ValueError` into `False` — a
malformed address is, for its purposes, simply not reachable. The tests in
[`tests/test_cluster.py`](../../dask-distributed/tests/test_cluster.py) cover
the unhappy paths explicitly, including a closed port and a socket bound to an
ephemeral port to prove the positive case.

`ClusterSession` is a small dataclass carrying the client, the mode
(`"compose"` or `"local"`), and the address actually in use. It is a context
manager, so examples read as:

```python
with connect() as session:
    client = session.client
    print(session.banner())
```

and `close()` does the right thing in both modes — closing the client always,
and additionally closing the cluster object when the session created one. Not
closing the fallback cluster would leave a scheduler and two worker threads
running until interpreter exit, which is harmless in a script and a real leak
in anything longer-lived.

`banner()` is the honesty mechanism. Against the containers:

```text
Connected to the Compose cluster at tcp://127.0.0.1:8786
```

Without them:

```text
Compose cluster not reachable at tcp://127.0.0.1:9999 -- fell back to an in-process LocalCluster (inproc://192.168.1.8/91305/1).
  Start the real thing with: make up
```

The `inproc://` scheme in that address is the tell: there is no TCP at all, and
"sending" an argument to a worker is a dictionary lookup. Every example then
branches on `session.is_compose` to say what the fallback cannot show. That
branch is not decoration — it is what stops the fallback from teaching the
wrong lesson. Compare the two runs of `0101_connect`:

```text
Each worker is its own container: 3 distinct host addresses.
Nothing is shared between them -- not memory, not open files, not imported modules.
```

```text
The fallback runs 2 workers as threads in ONE process (1 host).
They share memory, so transfers are free and the distributed lessons stay invisible.
```

The scheduler address comes from the `DASK_SCHEDULER_ADDRESS` environment
variable, defaulting to `tcp://127.0.0.1:8786`. That one line is what makes the
same examples work against a remote cluster without edits — point it at a
deployed scheduler and nothing else changes. It is also how the fallback output
above was produced, by pointing at a port with nothing on it:

```console
$ DASK_SCHEDULER_ADDRESS=tcp://127.0.0.1:9999 uv run python examples/0101_connect.py
```

!!! note "One observed wrinkle in the fallback"

    `connect()` passes `dashboard_address=None` to `LocalCluster`, intending to
    start no dashboard — the comment explains that binding a port would clash
    with the real cluster's 8787. On the version pinned here (distributed
    2026.7.1) that did not fully take: running the fallback while the Compose
    cluster was up printed

    ```text
    UserWarning: Port 8787 is already in use.
    Perhaps you already have a cluster running?
    Hosting the HTTP server on port 55304 instead
    ```

    and then served a dashboard on the fallback port anyway. It is a warning,
    not a failure, and it only appears in the unusual case of running the
    fallback on a machine where the real cluster is also running. Worth knowing
    if the warning surprises you; not worth working around.

`wait_for_scheduler()` is the same probe in a loop with a deadline, used by
`make up` and `make wait`. It prints `scheduler ready at ...` on success and
raises `TimeoutError` with an actionable message on failure:

```python
raise TimeoutError(f"no scheduler answered at {address} within {timeout:.0f}s (try: make logs)")
```

Pointing at `make logs` in the error text is a small thing that saves a real
minute, because the answer to "why did the scheduler not come up" is always in
those logs.

Finally, `describe_workers(client)` normalizes `client.scheduler_info()` into a
list of dicts with address, host, threads, and memory limit in GiB. It exists
because `scheduler_info()` returns a large nested structure whose exact shape
is an implementation detail, and because several examples want the same four
fields. The docstring names the point directly: in a container cluster the
hosts are distinct, which is the whole reason to look.


---

## Core concepts

Six mechanisms carry almost everything in this project: submitting work,
publishing data, the future lifecycle, worker memory thresholds, nannies, and
retries. Each is introduced here with runnable code and the output it actually
produced, so the per-example sections below can lean on them rather than
re-explaining.

### submit, map, and gather

`client.submit(fn, *args, **kwargs)` sends one function call to the cluster and
returns a `Future` immediately. The call has not run yet — it may not even have
been assigned to a worker.

```python
from ocs_stack_dask_distributed import connect

def where_am_i(task_id: int) -> str:
    import socket
    return f"task {task_id} ran on {socket.gethostname()} (pid {os.getpid()})"

with connect() as session:
    client = session.client
    print(client.submit(where_am_i, 1).result())
```

```text
  task 1 ran on 66de7753702c (pid 24)
```

That hostname is a container ID and that pid is 24 — the worker process inside
the container, not anything in your shell. It is the shortest possible proof
that the work left your machine.

`client.map(fn, iterable)` is `submit` over an iterable and returns a list of
futures, one per input. It is the normal way to fan out:

```python
mapped = client.gather(client.map(square, range(10)))
```

```text
  squares of 0..9 -> [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]
```

`client.gather(futures)` collects results back into your process. It blocks
until everything is ready and preserves order. Prefer it over a list
comprehension of `.result()` calls: `gather` fetches in one coordinated round
trip, while `[f.result() for f in futures]` serializes the waiting and can be
markedly slower for large lists.

Three details are worth knowing before you rely on this API.

**Concurrency is bounded by total threads, not by worker count.** This cluster
is 3 workers x 2 threads = 6 slots. Submitting 12 tasks of 0.4 s gives:

```text
3 workers x 2 threads = 6 concurrent slots.
Submitting 12 tasks of ~0.4s each into 6 slots.

Wall time 0.83s versus 4.8s if run one after another
  speedup: 5.8x (ideal would be 6x)
```

Two waves of six, 0.4 s each, plus overhead. The 5.8x against a theoretical 6x
is about as close as scheduling overhead allows. (Machine-dependent.)

**Keys are content-addressed unless you say otherwise.** `submit(f, 3)` twice
gives you the same future and runs `f(3)` once. When you want a genuine re-run
— for a side-effecting task, a randomized one, or a deliberately flaky one —
pass `pure=False`. `0402` and `0403` both do.

**`gather` raises on the first error, unless told not to.** `errors="skip"`
returns the successes and drops the failures, which is what you want when a
batch is a best-effort fan-out rather than an all-or-nothing transaction:

```python
skipped = client.gather(futures, errors="skip")
```

```text
  gather(errors='skip') -> [0, 2, 4, 6, 8]
```

There is also `client.run(fn, ...)`, which is *not* task submission: it executes
a function once on every worker (or on the workers you name) outside the task
graph, and returns a dict keyed by worker address. It is the cluster's
introspection primitive — "what does every worker see?" — and `0302` uses it to
ask all three workers whether a path exists.

### scatter: publish data once

`client.scatter(obj)` pushes data to the cluster up front and returns a
`Future` standing for it. Passing that future as an argument tells the
scheduler the data is already out there, so it is not re-serialized per task.

```python
remote = client.scatter(array, broadcast=True)
futures = [client.submit(column_mean, remote, i) for i in range(N_TASKS)]
results = client.gather(futures)
```

The measured difference for a 5.1 MB array needed by 12 tasks:

```text
Version 1 -- pass the array to every task:
  400 ms; up to 61 MB of serialization work

Version 2 -- scatter the array once, then pass the future:
  scatter itself: 54 ms (5.1 MB sent once)
  total:          82 ms

  identical results: True
  scatter version was 4.9x the speed of the naive one
```

`broadcast=True` copies the object to every worker immediately. Without it, the
data lands on one worker and is moved to others on demand — better when only
some tasks need it, worse when all of them do.

The thing to internalize about scatter is that it creates data **with no
recipe**. A chunk of a dask array can be recomputed from the graph if its
worker dies; a scattered object cannot, because the scheduler has no idea how
it was made. That asymmetry runs through the failure sections below.

Scatter is also easy to over-apply. Do not scatter small data (the round trip
costs more than the send), data used by exactly one task (nothing to amortize),
or data a worker could load itself from shared storage (skip the client
entirely — that last case is the one that matters most in a real service).

### The future lifecycle

`future.status` is a string you can read without blocking:

```python
future = client.submit(always_fails, 42)
print(future.status)     # "pending"
time.sleep(0.5)
print(future.status)     # "error"
```

```text
  submit() returned immediately; future.status is now 'pending'
  after the task ran, status is 'error'
  result() re-raises it here: ValueError: this task was never going to work (value=42)
  future.traceback() gives the WORKER's traceback: traceback
```

`future.result()` blocks and either returns the value or re-raises the worker's
exception. `future.exception()` returns the exception object without raising,
and `future.traceback()` returns the traceback object *from the worker* — the
stack of the machine that actually failed, which is the only reason distributed
debugging is bearable.

Futures resolve asynchronously, and the examples make a point of showing it:

```text
  5 finished, 1 errored out of 6
  the successful results are still available: [0, 2, 4, 6, 8]
  (done-before-wait was 0; futures resolve asynchronously)
```

That last line is checking `status` immediately after `submit`, before any wait:
zero futures were finished, because nothing had had time to run. Polling
`status` in a tight loop is a way to write a busy-wait bug; `client.gather`,
`distributed.wait`, and `as_completed` are the intended tools.

The lifecycle also has a cancellation edge: if the cluster loses a key that has
no recipe — scattered data on a worker that died — dependent futures move to
`cancelled` rather than silently recomputing, because there is nothing to
recompute from.

### Worker memory thresholds: target, spill, pause, terminate

Each worker has a hard memory limit; here it is 1.5 GiB, set by
`--memory-limit 1.5GiB` in `compose.yml`. distributed watches its own memory use
against that limit and escalates through four stages, each configured as a
fraction in dask config:

```python
import dask.config
dask.config.get("distributed.worker.memory.target")     # 0.60
dask.config.get("distributed.worker.memory.spill")      # 0.70
dask.config.get("distributed.worker.memory.pause")      # 0.80
dask.config.get("distributed.worker.memory.terminate")  # 0.95
```

`0301_worker_memory` prints them with the absolute figure for this cluster:

```text
As a worker's memory fills, distributed escalates through four stages
(fractions of that worker's own limit):
  target     0.60  (~0.9 GiB here) -- start spilling the least-recently-used data to disk
  spill      0.70  (~1.05 GiB here) -- spill aggressively; the worker is now doing disk I/O, not work
  pause      0.80  (~1.2 GiB here) -- stop accepting new tasks; the worker goes quiet
  terminate  0.95  (~1.42 GiB here) -- kill and restart the worker; its in-memory data is lost
```

Read as a progression, these describe a worker getting into trouble:

- **target (60%)** — the worker starts writing least-recently-used data to its
  local directory. Still healthy; the cost is that touching spilled data now
  needs a disk read, which shows up as `disk-read` in the task stream.
- **spill (70%)** — spilling becomes aggressive. The worker is now doing I/O
  instead of your work, and throughput drops without anything failing.
- **pause (80%)** — the worker stops accepting new tasks. On the dashboard it
  goes quiet while its memory bar stays red. This is the stage people misread as
  "the cluster hung": it has not hung, it has run out of room.
- **terminate (95%)** — the worker is killed and restarted by its nanny.
  Everything it held in memory is lost, and anything with a recipe gets
  recomputed elsewhere.

A distinction that matters for reading any of this: **managed** memory is data
distributed knows about (task results it is holding), while **unmanaged** memory
is everything else in the process — numpy scratch, interpreter overhead, a
library's cache, a leak. Only managed memory can be spilled. A worker dying with
low managed memory is a worker whose unmanaged memory grew, and no amount of
spilling will save it.

Watching managed memory rise and fall is a two-line trick — persist, then drop
the reference:

```python
persisted = array.persist()
client.gather(client.futures_of(persisted))   # block until really resident
...
del persisted                                  # the last reference frees it
```

```text
  before: 0 MB managed across the cluster
  the array is 392 MB in 16 chunks
  after persist: 392 MB managed
    tcp://172.19.0.3:36739           122 MB  (8% of its limit)
    tcp://172.19.0.4:41379           147 MB  (10% of its limit)
    tcp://172.19.0.5:36231           122 MB  (8% of its limit)

  after releasing the reference: 0 MB managed
```

The practical rule that falls out: memory pressure is a chunking problem, not a
hardware problem. A worker needs room for several chunks at once — inputs,
outputs, and scratch for every concurrently-running task — so the planning
number is roughly `limit / threads`, and the rule of thumb of ~100 MB chunks
exists because it sits comfortably inside typical worker budgets.

### Nannies and restart

Every worker container here runs `dask worker`, which starts a nanny that
supervises one worker process. You can see both in the startup banner:

```text
worker-2  | + exec dask worker tcp://scheduler:8786 --nworkers 1 --nthreads 2 --memory-limit 1.5GiB --local-directory /tmp/dask-worker-space
worker-2  | 2026-08-17 14:08:40,999 - distributed.nanny - INFO -         Start Nanny at: 'tcp://172.19.0.3:42421'
worker-2  | 2026-08-17 14:08:41,494 - distributed.worker - INFO -       Start worker at:     tcp://172.19.0.3:45543
worker-2  | 2026-08-17 14:08:41,494 - distributed.worker - INFO -               Threads:                          2
worker-2  | 2026-08-17 14:08:41,494 - distributed.worker - INFO -                Memory:                   1.50 GiB
worker-2  | 2026-08-17 14:08:41,494 - distributed.worker - INFO -       Local Directory: /tmp/dask-worker-space/dask-scratch-space/worker-unttkjo5
worker-2  | 2026-08-17 14:08:41,505 - distributed.worker - INFO -         Registered to:       tcp://scheduler:8786
```

The nanny listens on its own port (`42421` above) and the worker on another
(`45543`). When the worker process exits for any reason, the nanny notices,
unregisters it, and starts a replacement:

```text
worker-2  | 2026-08-17 17:48:42,748 - distributed.nanny - INFO - Worker process 120 exited with status 1
worker-2  | 2026-08-17 17:48:42,748 - distributed.nanny - INFO - Unregistering worker (status=Status.running)
worker-2  | 2026-08-17 17:48:42,749 - distributed.nanny - WARNING - Restarting worker (status=Status.running)
worker-2  | 2026-08-17 17:48:43,094 - distributed.worker - INFO -       Start worker at:     tcp://172.19.0.3:34041
```

Three consequences follow from that log, and all three matter.

**The replacement has a new address.** `172.19.0.3:36739` became
`172.19.0.3:34041` — same container IP, new ephemeral port, because it is a new
process. Worker address is therefore an identifier for a *process*, not for a
machine or a container. Any code that pins work with `workers=[address]` and
holds the address across a restart is pinning to something that no longer
exists.

**The replacement has fresh memory and fresh globals.** Nothing survives. Data
the old process held is gone, and any state a task stashed on the worker object
is gone with it.

**The container never restarted.** After a run of `0401_worker_failure`:

```console
$ docker compose ps --format "table {{.Name}}\t{{.Status}}"
NAME                           STATUS
dask-distributed-scheduler-1   Up 4 hours (healthy)
dask-distributed-worker-1      Up 4 hours
dask-distributed-worker-2      Up 2 hours
dask-distributed-worker-3      Up 4 hours
```

No container uptime reset, because Docker never noticed. This is worth
remembering when debugging a deployment: if you are looking for evidence of a
worker crash in `kubectl get pods` restart counts, you will not find it. The
nanny handled it a layer below.

The client-side counterpart is `client.restart()`, which restarts every worker
and clears all cluster state — a blunt instrument, occasionally the right one
after a memory leak, and never something to run against a cluster somebody else
is using.

### Retries

`client.submit(fn, *args, retries=N)` tells the scheduler to re-run a failed
task up to N times before giving up and marking the future as errored.

```python
client.submit(flaky, "with-retry", 2, retries=3, pure=False, workers=[pinned_to]).result()
```

```text
  Without retries:
    ConnectionError: transient failure on attempt 1
  With retries=3:
    succeeded on attempt 3 on 72c10137d285
  dask re-ran the task in place; the client never saw the failures.
```

The rule for when retries help is one sentence: **retries fix transient faults
and nothing else.** A rate-limited API, a dropped connection, a flaky mount, a
worker that died mid-task — all worth retrying. A bug in your code is not:

```text
  a real bug retried 3 times is still a bug -> ValueError: this task was never going to work (value=7)
  Three retries here bought nothing but latency.
```

There is a subtlety that `0402` builds an entire pin around, and it is the kind
of thing that produces a bug report rather than a lesson if you meet it in
production: **a retry is free to land on a different worker.** The container
logs show it plainly — the same task key retried on two different containers:

```text
worker-1  | 2026-08-17 17:48:44,666 - distributed.worker - ERROR - Compute Failed
worker-1  | Key:       always_fails-81300c72-a02a-408b-a1ce-a905a02e85a4
worker-2  | 2026-08-17 17:48:44,668 - distributed.worker - ERROR - Compute Failed
worker-2  | Key:       always_fails-81300c72-a02a-408b-a1ce-a905a02e85a4
```

If your "flaky" task counts its own attempts in worker-local state, the counter
resets every time the retry moves, and the task fails forever. That is why the
example pins its flaky task with `workers=[pinned_to]`, and it is a good
illustration of the general principle: retry logic must be idempotent with
respect to *which* worker runs it.

Retries are also available on collections through `dask.compute(..., retries=N)`
and can be set globally in dask config, which is the right lever when the
transient fault is in a storage backend that every task touches.

---

## Phase 1 — Connecting to a real cluster

Three examples that establish the ground truth: there is a cluster, it is made
of separate processes, those processes have their own libraries, and work you
submit lands on them.

### 0101_connect — what is on the other end of the socket

Source: [`examples/0101_connect.py`](../../dask-distributed/examples/0101_connect.py)

**What it teaches.** How to open a client against a real scheduler, how to read
the authoritative worker inventory, what "capacity" means numerically, and the
first proof that your code ran somewhere else.

**The code.** The example's only computation is a function whose entire purpose
is to report where it ran:

```python
def where_am_i(task_id: int) -> str:
    """Report the container and process that executed this task."""
    import socket

    return f"task {task_id} ran on {socket.gethostname()} (pid {os.getpid()})"
```

Two details in those four lines are load-bearing. First, the function is
defined **at module level**, which its docstring calls out: a closure or a
lambda capturing local state is a much harder thing to ship, and while
cloudpickle can often manage it, module-level functions are the shape that
always works. Second, `import socket` happens *inside* the function. The import
therefore runs on the worker, at call time, in the worker's environment — which
is the correct instinct for anything that might not exist on both sides.

The inventory comes from `describe_workers()`, a thin wrapper over
`client.scheduler_info()`:

```python
info = client.scheduler_info()
for address, meta in sorted(info.get("workers", {}).items()):
    workers.append({
        "address": address,
        "host": meta.get("host", "?"),
        "threads": meta.get("nthreads", 0),
        "memory_limit_gib": round(meta.get("memory_limit", 0) / 2**30, 2),
    })
```

`scheduler_info()` is the authoritative answer to "what is out there". It is a
snapshot of the scheduler's own state, not a guess from configuration, so it
reflects workers that have actually registered — including replacements after a
crash and additions after `make scale`.

**Real output**, against the Compose cluster:

```text
Connected to the Compose cluster at tcp://127.0.0.1:8786

The scheduler reports 3 workers:
  tcp://172.19.0.3:36739       host=172.19.0.3     threads=2  memory=1.5 GiB
  tcp://172.19.0.4:41379       host=172.19.0.4     threads=2  memory=1.5 GiB
  tcp://172.19.0.5:36231       host=172.19.0.5     threads=2  memory=1.5 GiB

Cluster capacity: 6 threads, 4.5 GiB across 3 workers.
That is 6 tasks running at once before work starts queueing.

Each worker is its own container: 3 distinct host addresses.
Nothing is shared between them -- not memory, not open files, not imported modules.

Dashboard: http://127.0.0.1:8787/status
Open it while a job runs -- the task stream is the fastest way to see what a cluster is doing.

Submitting one task to confirm the round trip:
  task 1 ran on 66de7753702c (pid 24)
```

**Why it matters.** Four facts are established here that everything else builds
on.

The addresses are `172.19.0.x` — the compose network, three distinct IPs. Each
worker is a separate host as far as anything on the cluster is concerned. When
`0302_shared_storage` reports that a path is invisible to all three, this is
why.

The capacity is 6 threads and 4.5 GiB, but those two numbers behave completely
differently. Threads pool: 6 tasks run at once regardless of which worker they
land on. Memory does not pool: it is three separate 1.5 GiB budgets, and there
is no arrangement of tasks that lets one chunk use 4.5 GiB.

The hostname in the result — `66de7753702c` — is a container ID, and the pid is
24. Both are meaningless on your machine. That is the point.

The dashboard link is printed because it is genuinely the first thing to open
when a job misbehaves, and the URL is not obvious from the scheduler address
(different port, HTTP not TCP).

**Contrast with the fallback.** Running with `DASK_SCHEDULER_ADDRESS` pointed at
a dead port produces:

```text
Compose cluster not reachable at tcp://127.0.0.1:9999 -- fell back to an in-process LocalCluster (inproc://192.168.1.8/91305/1).
  Start the real thing with: make up

The scheduler reports 2 workers:
  inproc://192.168.1.8/91305/4 host=192.168.1.8    threads=2  memory=32.0 GiB
  inproc://192.168.1.8/91305/6 host=192.168.1.8    threads=2  memory=32.0 GiB

Cluster capacity: 4 threads, 64.0 GiB across 2 workers.

The fallback runs 2 workers as threads in ONE process (1 host).
They share memory, so transfers are free and the distributed lessons stay invisible.

Submitting one task to confirm the round trip:
  task 1 ran on mlaptop.local (pid 91305)
```

Look at the differences. The scheme is `inproc://` rather than `tcp://`. Both
"workers" report the same host and the same pid as the client. The memory limit
is 32 GiB per worker, which is the machine's RAM counted twice — a number that
tells you it is not a real budget. This is the honest version of a fallback:
still runnable, and explicitly labelled as not teaching the lesson.

**The traps.**

- **`Client(address)` changes global state.** It registers itself as the default
  scheduler for the process, so every subsequent `.compute()` goes to the
  cluster. That is usually what you want, and it is a surprise the first time a
  library's internal `.compute()` starts making network calls.
- **`scheduler_info()` is a snapshot, not a subscription.** Call it again after
  anything that changes the cluster. `0401` relies on exactly this by polling it
  to watch a worker get replaced.
- **`client.dashboard_link` can be wrong behind port mapping.** Here it resolves
  to `http://127.0.0.1:8787/status` because the port is published one-to-one. In
  Kubernetes, behind an ingress, or with a remapped host port, the link the
  scheduler advertises is the address *it* knows, not the one you can reach.
- **A connect that hangs is almost always a firewall or a bind address**, not a
  dask problem. The scheduler binding `127.0.0.1` inside a container is the
  classic version, which is why `--host 0.0.0.0` is in the compose command.

### 0102_versions — why client and workers must match

Source: [`examples/0102_versions.py`](../../dask-distributed/examples/0102_versions.py)

**What it teaches.** That the cluster is an environment, not just capacity; how
to interrogate every participant's package versions in one call; and the
ordered list of what drift actually costs.

**The code.** One call does the work:

```python
versions: dict[str, Any] = client.get_versions(check=False)
```

`check=False` collects the data without raising on a mismatch. With
`check=True`, `get_versions` raises when it finds disagreement — which is the
right setting for a startup assertion in production code and exactly wrong
here, where the entire point is to look at the differences rather than trip
over them.

The example then compares a fixed watchlist across client, scheduler, and every
worker:

```python
WATCHED = ("python", "dask", "distributed", "numpy", "pandas", "cloudpickle", "msgpack", "toolz")
```

That list is not arbitrary. It is the framework itself (`dask`, `distributed`),
the serializers that decide what "the same bytes" means (`cloudpickle`,
`msgpack`, `toolz`), the runtime that unpickles them (`python`), and the two
libraries whose objects actually travel (`numpy`, `pandas`). A version
disagreement in any of these can change behaviour on the wire; a disagreement in
some unrelated leaf package usually cannot.

Worker versions are collapsed with a set, so that three workers running the same
version print once and three workers running different versions print `MIXED`:

```python
worker_versions = {pkgs.get(name, "-") for pkgs in worker_pkgs}
worker_display = worker_versions.pop() if len(worker_versions) == 1 else "MIXED"
```

`MIXED` is worth flagging loudly, because a heterogeneous cluster is strictly
worse than a uniformly-wrong one: the same task produces different results
depending on where it lands, and the failure is not reproducible.

**Real output:**

```text
client.get_versions() asks every participant what it is running.
  client, scheduler, and 3 worker(s) reported in.

  package        client           scheduler        workers          match
  -------------- ---------------- ---------------- ---------------- -----
  python         3.13.14.final.0  3.13.14.final.0  3.13.14.final.0  yes
  dask           2026.7.1         2026.7.1         2026.7.1         yes
  distributed    2026.7.1         2026.7.1         2026.7.1         yes
  numpy          2.5.2            2.5.2            2.5.2            yes
  pandas         3.0.5            3.0.5            3.0.5            yes
  cloudpickle    3.1.2            3.1.2            3.1.2            yes
  msgpack        1.2.1            1.2.1            1.2.1            yes
  toolz          1.1.0            1.1.0            1.1.0            yes

Everything agrees, so no VersionMismatchWarning on connect.
That is not luck: the Dockerfile pins numpy, pandas, tornado and friends
to the versions in uv.lock. The base image alone was a patch behind and
warned on every single connect.

What drift causes, in rough order of how nasty it is:
  1. a warning on connect -- easy to ignore, and people do
  2. an unpickling error deep in a task, surfacing as a confusing traceback
  3. a missing module on the worker: the client imports it fine, the worker cannot
  4. silently different numerics between library versions -- the one that bites hardest
```

**Why it matters.** The all-`yes` column is the least interesting thing on the
screen and the most expensive to produce. It exists because someone noticed a
`VersionMismatchWarning` on every connect, traced it to the base image shipping
numpy and tornado a patch behind what `uv.lock` resolved, and pinned them. That
is a small amount of work that prevents a specific bad habit: a warning that
fires on every single run trains everyone to ignore warnings, including the one
that matters.

The four-item severity list is the part to remember. Read it in reverse. The
worst outcome is not a crash — a crash is a gift, because it tells you
something is wrong. The worst outcome is two library versions that both work and
disagree slightly about a numerical result, so your cluster produces answers
that are subtly wrong and perfectly reproducible on the machine you test on.

**The traps.**

- **Client-side import success proves nothing.** Your laptop having `scipy` is
  irrelevant if the worker image does not. The failure arrives as a task error
  containing a `ModuleNotFoundError`, several frames deep, and reads like a
  dask bug rather than a packaging one.
- **`get_versions()` reports what is installed, not what is imported.** A worker
  with two environments on its path, or a `PYTHONPATH` surprise, can report one
  version and use another.
- **Pinning half the pair is worse than pinning neither.** Bumping
  `pyproject.toml` without rebuilding the image, or rebuilding the image from a
  new base tag without re-locking, leaves you with skew that appears only on the
  next connect. The Dockerfile comment says it directly: bump this tag and the
  pyproject floors together, never one alone.
- **`check=True` in a long-running service is a good idea and a sharp one.** It
  turns skew into a startup failure, which is right — as long as somebody is
  reading the startup logs.

### 0103_submit_across_workers — where tasks land

Source: [`examples/0103_submit_across_workers.py`](../../dask-distributed/examples/0103_submit_across_workers.py)

**What it teaches.** That submitting a batch spreads it over the cluster, that
the spread is decided task by task, that speedup is bounded by total threads,
and that `map` is the ergonomic form of `submit`.

**The code.** The task holds a slot without doing arithmetic, which keeps the
measurement about scheduling rather than about CPU:

```python
def busy_task(task_id: int, seconds: float = 0.4) -> tuple[int, str, int]:
    """Occupy a worker thread briefly and report where it ran."""
    time.sleep(seconds)
    return task_id, socket.gethostname(), os.getpid()
```

Sleeping rather than computing is a deliberate choice the docstring explains: it
holds the task slot long enough for the scheduler to spread the batch, without
making the example slow or making the result depend on the host's CPU.

The batch itself is a list comprehension over `submit`, then one `gather`:

```python
futures = [client.submit(busy_task, i) for i in range(N_TASKS)]
results = client.gather(futures)
```

Placement is then tallied from the returned hostnames with a `Counter` — the
results *are* the evidence, which is a nice property to design for.

**Real output:**

```text
3 workers x 2 threads = 6 concurrent slots.
Submitting 12 tasks of ~0.4s each into 6 slots.

Wall time 0.83s versus 4.8s if run one after another
  speedup: 5.8x (ideal would be 6x)

Tasks per worker container:
  66de7753702c      4  ####
  72c10137d285      4  ####
  8027be35ef47      4  ####

3 distinct containers shared the batch.
The scheduler assigns each task to whichever worker has a free slot.

client.map fans one function over many inputs (no manual loop):
  squares of 0..9 -> [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]
```

**Why it matters.** The 4/4/4 split is a perfectly balanced batch, and it is
worth understanding that this is *not* round-robin. The scheduler assigns each
task to whichever worker has capacity, weighing what data that worker already
holds and how long its queue is. With twelve identical no-input tasks and six
identical slots, "balanced" and "greedy" produce the same answer. With
unbalanced tasks or tasks that depend on data, they do not — which is what
`0203_locality` and `0503_task_stream` explore.

The 5.8x speedup against an ideal 6x is the honest measure of overhead. Twelve
0.4 s tasks is 4.8 s of work; six slots means two waves; two waves is 0.8 s;
observed was 0.83 s. About 30 ms went to scheduling and transport for the whole
batch. That is small, and it is small *because* the tasks are 400 ms each. The
same 30 ms against tasks of 1 ms would dominate completely.

The `map` line at the end is not filler. `client.map(square, range(10))` is the
form you will actually write, and it differs from a comprehension in one useful
way: it batches the submission into fewer messages to the scheduler, which
matters once the iterable is thousands of items long.

**The traps.**

- **`speedup` here is a best case and knows it.** Independent, equal-sized,
  input-free tasks are the friendliest possible workload. Real work has
  dependencies (which serialize parts of the graph), unequal durations (which
  leave slots idle at the tail), and inputs (which have to get to the worker).
- **More workers does not mean more concurrency if threads stay constant.** Six
  single-threaded workers and three two-threaded workers both give six slots.
  They differ in memory isolation and GIL behaviour, not in slot count.
- **A `Counter` over hostnames counts containers, not workers.** They coincide
  here because compose runs one worker per container. Under `--nworkers 3` you
  would see one hostname with three pids, and the tally would mislead.
- **Do not read placement from a single small batch.** The scheduler makes
  probabilistic and load-dependent decisions; `0501` shows a much more lopsided
  split (9 / 78 / 77) for a different workload on the same cluster, and that is
  not a bug.

---

## Phase 2 — Moving data across the wire

Everything sent to a worker is serialized. These three examples make that cost
visible, then show the two ways to avoid paying it repeatedly.

### 0201_serialization — what crosses the wire

Source: [`examples/0201_serialization.py`](../../dask-distributed/examples/0201_serialization.py)

**What it teaches.** That arguments are pickled and pushed through a socket,
what that costs relative to the work itself, that building data on the worker is
usually better than sending it, and that some objects cannot cross at all.

**The code.** Three tasks, deliberately measuring three different things:

```python
def sum_array(array: np.ndarray) -> float:
    """Sum an array that was shipped from the client as an argument."""
    return float(array.sum())


def build_and_sum(rows: int, cols: int, seed: int) -> float:
    """Build an array on the worker, then sum it.

    Only three integers cross the wire; the megabytes never leave the worker.
    """
    rng = np.random.default_rng(seed)
    return float(rng.random((rows, cols)).sum())


def add_one(value: int) -> int:
    """Add one to a value, as the cheapest possible payload."""
    return value + 1
```

`add_one` measures the floor — scheduling overhead with nothing to carry.
`sum_array` measures floor plus 8 MB of transfer. `build_and_sum` measures floor
plus the same arithmetic, with three integers on the wire instead of 8 MB. Same
seed, so the results are identical and the comparison is honest.

The payload is weighed before anything runs, which is a habit worth copying:

```python
payload_mb = array.nbytes / 1e6
pickled_mb = len(pickle.dumps(array)) / 1e6
```

**Real output:**

```text
The array is 1000x1000 float64 = 8.0 MB in memory,
and 8.0 MB once pickled -- that is what travels per task.

A task with an int argument round-trips in 7.9 ms.
That is the floor: scheduling overhead with nothing to carry.

Sending the 8.0 MB array as an argument: 77 ms
Building the same array on the worker:      20 ms
  identical result: True (500159.2565)

  Shipping cost about 3.9x the worker-side build.
  The work is the same; the difference is bytes on a socket.
```

(All timings machine-dependent.)

**Why it matters.** Three numbers, three lessons.

**7.9 ms is the floor.** That is what a task costs when it carries nothing. It
sets the minimum useful task size: anything under about 50 ms of work is mostly
overhead, and a graph of a million such tasks is a scheduler benchmark rather
than a computation.

**77 ms versus 20 ms is the transfer tax.** The arithmetic is identical; the
only difference is 8 MB going through a socket. Note that this is loopback TCP
between containers on one host — the best possible case. The equivalent number
across a real network, or between availability zones, is worse by a large
factor.

**`pickled_mb == payload_mb` is a reassurance and a warning.** A dense float64
numpy array pickles to essentially its own size, because there is nothing to
compress and no Python object overhead per element. A list of a million Python
floats does not: it pickles to several times its `sys.getsizeof` because every
element is an object. The general lesson is that "how big is my payload" is a
question about the object's representation, not about how big it looks.

The example ends by trying to send something unsendable:

```text
Not everything can cross the wire. dask uses cloudpickle, which handles
lambdas and locally-defined functions, but OS resources cannot be pickled:
  sending an open file handle -> TypeError
    ('Could not serialize object of type LLGExpr', "LLGExpr(dsk={'sum_arra...
    the root cause: cannot pickle 'BufferedReader' instances
  Send the path instead and let the worker open it -- if it can see it.
```

The error is worth reading carefully, because it is representative of a whole
genre. The top-level message says `Could not serialize object of type LLGExpr`
— an internal dask graph-expression class, which tells you nothing about your
code. The useful part is buried: `cannot pickle 'BufferedReader' instances`.
When a distributed job fails at submit time with a serialization error, the
first move is always to scroll past the framework's wrapper to the innermost
`cannot pickle` line.

There is also a piece of test hygiene worth noting, because it is a good
pattern: distributed logs the full pickle traceback at `ERROR` before raising,
which is right in production and pure noise in a lesson, so the example
temporarily silences exactly one logger:

```python
protocol_logger = logging.getLogger("distributed.protocol")
previous_level = protocol_logger.level
protocol_logger.setLevel(logging.CRITICAL)
try:
    ...
finally:
    protocol_logger.setLevel(previous_level)
```

Silencing a named logger around a deliberately-provoked failure, and restoring
it in `finally`, is much better than a blanket `logging.disable()`.

**The traps.**

- **The trailing clause of the last line is the real trap.** "Send the path
  instead and let the worker open it — *if it can see it*." Sending a path
  removes the serialization problem and introduces the filesystem problem, which
  is `0302`'s entire subject.
- **`cloudpickle` handling lambdas encourages a bad habit.** It can serialize a
  closure over a 500 MB DataFrame just fine — and then it ships 500 MB with
  every task, invisibly, because you never wrote the array as an argument.
  Accidental capture is the sneakiest version of this bug.
- **Big graphs get the same warning as big arguments.** `0303` triggers
  `UserWarning: Sending large graph of size 182.54 MiB`, which is the same
  problem wearing a different hat: data embedded in a graph rather than passed
  as an argument.
- **Compression is automatic but not magic.** distributed will compress some
  payloads on the wire, which helps for compressible data and costs CPU for
  data that is already dense and random.

### 0202_scatter_gather — publish once, reuse many times

Source: [`examples/0202_scatter_gather.py`](../../dask-distributed/examples/0202_scatter_gather.py)

**What it teaches.** The classic distributed performance bug — passing one big
object to N tasks — and the one-line fix.

**The code.** The two versions differ by exactly one line:

```python
# Version 1: the array itself, N times
naive_futures = [client.submit(column_mean, array, i) for i in range(N_TASKS)]

# Version 2: scatter once, then pass the Future
remote = client.scatter(array, broadcast=True)
scattered_futures = [client.submit(column_mean, remote, i) for i in range(N_TASKS)]
```

The task is the same in both cases and does almost no work, so the measurement
is nearly pure data movement:

```python
def column_mean(array: np.ndarray, index: int) -> float:
    """Return the mean of one column of a shared array."""
    return float(array[:, index % array.shape[1]].mean())
```

**Real output:**

```text
A 5.1 MB array, needed by 12 separate tasks.

Version 1 -- pass the array to every task:
  400 ms; up to 61 MB of serialization work
  dask deduplicates identical arguments to a degree, but it still hashes
  and tracks the payload once per submit call.

Version 2 -- scatter the array once, then pass the future:
  scatter itself: 54 ms (5.1 MB sent once)
  total:          82 ms

  identical results: True
  scatter version was 4.9x the speed of the naive one

scatter returns a Future: Future, status=finished
```

(Timings machine-dependent.)

**Why it matters.** 400 ms to 82 ms for a one-line change, on a 5 MB array. The
shape of the improvement is what to remember: the naive version's cost grows
with `N x payload`, while the scatter version's cost is `payload + N x epsilon`.
At 12 tasks it is a 5x difference. At 1,000 tasks it is the difference between a
job that finishes and a job that does not.

The parenthetical in the output is a nuance worth keeping: dask does deduplicate
identical arguments to a degree, so the naive version does not literally send 61
MB. It still hashes and tracks the payload once per `submit` call, and that
per-call work is what the measurement is picking up. The lesson survives the
nuance — do not rely on the deduplication, because it depends on the objects
being recognized as identical, which is not something you control.

`status=finished` immediately after `scatter` is a useful detail: unlike
`submit`, `scatter` blocks until the data is actually on the workers. There is
nothing to wait for afterwards.

The example is careful to say when *not* to reach for this:

```text
When NOT to scatter:
  - small data: the round trip costs more than just sending it
  - data used by exactly one task: there is nothing to amortize
  - data a worker could load itself from shared storage: skip the client entirely
```

That third bullet is the one that matters for a service. Scatter is a fix for
"the client has data the workers need". The better architecture is usually that
the client never had the data — the workers read it from a store, and the client
sends a URL. `0303` follows exactly that thread.

**The traps.**

- **Scattered data has no recipe.** This is the single most important
  consequence and it recurs in `0401` and `0403`. A chunk of a dask array can be
  recomputed if its worker dies; a scattered object cannot, because the
  scheduler does not know how it was made. Losing that worker loses the data,
  and dependent futures go to `cancelled`.
- **`broadcast=True` costs memory on every worker.** A 1 GB broadcast across 10
  workers consumes 10 GB of cluster memory. Without `broadcast`, the data lands
  on one worker and moves on demand — usually right when only some tasks need
  it, and worse when all of them do.
- **The returned future is a reference you must keep.** Drop it and the
  scheduler frees the data, so the next `submit` that "reuses" it silently
  re-sends. Holding a scattered future in a long-lived variable is the intended
  pattern, and it is also a way to pin cluster memory indefinitely.
- **Scattering inside a loop defeats the purpose entirely.** `scatter` in the
  body of the same loop that submits is the naive version with extra steps.

### 0203_locality — moving compute to data

Source: [`examples/0203_locality.py`](../../dask-distributed/examples/0203_locality.py)

**What it teaches.** That the scheduler prefers to run a task where its input
already lives, how to see where data is, and what overriding that preference
costs.

**The code.** First, put data on the cluster and make sure it is really there:

```python
array = da.random.random((4000, 4000), chunks=(1000, 1000))
persisted = array.persist()
# Block until every chunk is really resident before asking who has what.
client.gather(client.futures_of(persisted))
```

That second line is essential and easy to forget. `persist()` returns
immediately with a collection whose chunks are futures; the chunks may still be
computing. Asking `who_has` before they land gives you a partial and confusing
answer. `client.futures_of(collection)` extracts those futures, and `gather`
blocks until all of them are `finished`.

Then two complementary views of placement:

```python
has_what = client.has_what()          # worker address -> keys it holds
who_has = client.who_has(persisted)   # key -> worker addresses holding it
```

`has_what` answers "what does this worker have?" and is the one you want when
diagnosing an unbalanced cluster. `who_has` answers "where is this key?" and is
the one you want when diagnosing a specific slow task. Note that `who_has`
returns a *list* of addresses per key — data can be replicated, deliberately via
`client.replicate()` or incidentally after a transfer.

Finally, the deliberate anti-pattern:

```python
pinned = [client.submit(chunk_sum, client.futures_of(persisted)[i], workers=[victim]) for i in range(8)]
```

`workers=[victim]` forces every one of those tasks onto one named worker,
regardless of where its input lives.

**Real output:**

```text
persist() computes chunks and LEAVES them in worker memory.
  a 128 MB array in 16 chunks is now resident

client.has_what() -- keys held per worker:
  172.19.0.3:36739          5 chunks
  172.19.0.4:41379          5 chunks
  172.19.0.5:36231          6 chunks

client.who_has() maps each chunk key to the worker(s) holding it:
  16 chunks spread over 3 worker(s)

Now submit work on those chunks and see where it runs.
  sum of the persisted array = 7,999,350 in 31 ms
  Each partial sum ran on the worker already holding its chunk:
  no chunk had to move, so the only traffic was the tiny partial results.

Forcing the opposite: pin every task to ONE worker with workers=.
  8 chunks summed on 172.19.0.3:36739: 38 ms
  (8 chunks, most of which had to be shipped to that worker first)
  Same arithmetic, extra network. Pinning is a tool for correctness
  (a worker with a GPU, a licence, a mounted disk), not for speed.
```

**Why it matters.** The 5/5/6 split of 16 chunks is the scheduler distributing
work as it computes. The 31 ms sum over 128 MB is what locality buys: sixteen
partial sums ran next to their data, and the only thing that crossed the network
was sixteen float64 scalars. Summing 128 MB in 31 ms across the cluster is not
impressive arithmetic — it is impressive *absence of I/O*.

The pinned version is the important half. Eight chunks summed on one worker took
38 ms against 31 ms for all sixteen unpinned, and most of those chunks had to be
shipped to the victim first. The absolute numbers are small because the data is
small and the network is loopback; the direction is what generalizes. Pinning
turned a free local read into a transfer, and it did so silently — no warning,
no error, just a slower job.

**Why `persist` rather than `compute`.** This is the example that makes the
distinction concrete. `compute()` would have brought 128 MB into the client
process and left nothing on the cluster to inspect. `persist()` leaves it out
there, and everything downstream — `has_what`, `who_has`, locality-aware
scheduling — is only possible because the data stayed.

And the release at the end is not tidy-up theatre:

```python
del persisted
```

```text
Dropping the reference lets the scheduler free those chunks;
worker memory is a resource you manage, not one that manages itself.
```

**The traps.**

- **`workers=` is a correctness tool, not a performance tool.** Legitimate uses
  are a worker with a GPU, a licensed library, a mounted disk, or credentials.
  Using it to "keep things together" almost always makes things slower.
- **A pinned worker that dies takes its tasks with it.** By default `workers=`
  is a hard constraint, and the scheduler will not reassign the task elsewhere.
  `allow_other_workers=True` makes it a preference instead, which is usually
  what people actually mean.
- **Worker addresses are process identities.** Pin to `tcp://172.19.0.3:36739`,
  let the nanny restart that worker as `tcp://172.19.0.3:34041`, and your
  constraint now names a worker that does not exist.
- **`has_what()` returns keys, not bytes.** Five chunks and six chunks look
  balanced; if those chunks were different sizes, they would not be. Use
  `scheduler_info()` metrics for the memory view.
- **Persisted data is a standing memory reservation.** It stays until the last
  reference is dropped, including across unrelated computations, including if
  you forget.

---

## Phase 3 — Worker memory and shared storage

A worker is a box with a fixed size and a filesystem that is not yours. These
three examples measure the first and prove the second.

### 0301_worker_memory — limits, thresholds, and watching usage

Source: [`examples/0301_worker_memory.py`](../../dask-distributed/examples/0301_worker_memory.py)

**What it teaches.** That each worker has a hard memory limit, what distributed
does as that limit approaches, how to read managed memory per worker, and that
releasing a reference is what frees cluster memory.

**The code.** The four thresholds are read from dask config rather than
hardcoded, which means the printed numbers are the ones actually in force:

```python
THRESHOLDS = (
    ("target", "distributed.worker.memory.target", "start spilling the least-recently-used data to disk"),
    ("spill", "distributed.worker.memory.spill", "spill aggressively; the worker is now doing disk I/O, not work"),
    ("pause", "distributed.worker.memory.pause", "stop accepting new tasks; the worker goes quiet"),
    ("terminate", "distributed.worker.memory.terminate", "kill and restart the worker; its in-memory data is lost"),
)

for name, key, effect in THRESHOLDS:
    fraction = dask.config.get(key, default=None)
```

Usage comes from the metrics the workers push to the scheduler:

```python
def worker_memory_mb(client: Any) -> dict[str, float]:
    info = client.scheduler_info()
    usage: dict[str, float] = {}
    for address, meta in info.get("workers", {}).items():
        metrics = meta.get("metrics", {})
        usage[address] = float(metrics.get("managed_bytes", 0)) / 1e6
    return usage
```

`managed_bytes` is the key field: bytes of *task results* the worker is
deliberately holding. It is not RSS. The gap between the two is unmanaged
memory, and the gap is where OOM kills come from.

Two `time.sleep` calls in this example look like flakiness and are not:

```python
persisted = array.persist()
client.gather(client.futures_of(persisted))
time.sleep(1.0)  # let the workers report fresh metrics to the scheduler
...
del persisted
time.sleep(1.5)  # release propagates asynchronously
```

Metrics flow from worker to scheduler on a heartbeat, so `scheduler_info()`
immediately after a change reports the state from a moment ago. Likewise
releasing is a message to the scheduler which forwards it to the workers.
Anything that polls `scheduler_info()` for a decision needs to be tolerant of
this lag — it is a monitoring surface, not a synchronous API.

**Real output:**

```text
3 workers, 4.5 GiB of memory in total:
  tcp://172.19.0.3:36739       limit 1.5 GiB
  tcp://172.19.0.4:41379       limit 1.5 GiB
  tcp://172.19.0.5:36231       limit 1.5 GiB

As a worker's memory fills, distributed escalates through four stages
(fractions of that worker's own limit):
  target     0.60  (~0.9 GiB here) -- start spilling the least-recently-used data to disk
  spill      0.70  (~1.05 GiB here) -- spill aggressively; the worker is now doing disk I/O, not work
  pause      0.80  (~1.2 GiB here) -- stop accepting new tasks; the worker goes quiet
  terminate  0.95  (~1.42 GiB here) -- kill and restart the worker; its in-memory data is lost

Persisting a ~400 MB array and watching managed memory rise.
  before: 0 MB managed across the cluster
  the array is 392 MB in 16 chunks
  after persist: 392 MB managed
    tcp://172.19.0.3:36739           122 MB  (8% of its limit)
    tcp://172.19.0.4:41379           147 MB  (10% of its limit)
    tcp://172.19.0.5:36231           122 MB  (8% of its limit)

  after releasing the reference: 0 MB managed
```

**Why it matters.** The accounting closes exactly: 122 + 147 + 122 = 391 MB
against a 392 MB array. Every byte is on a worker, none of it is on the client,
and the split is uneven (6 chunks on one worker, 5 on the others) because
chunks are the unit of placement.

The percentages are the useful reframing. 392 MB of array is nothing in
absolute terms; per worker it is 8-10% of a 1.5 GiB budget. Multiply the array
by ten and the same computation puts each worker at 80-100% — past `pause`,
into `terminate`. The array that "fits in the cluster" (3.9 GB against 4.5 GiB
total) does not, because memory does not pool and because a worker also needs
room for the inputs and outputs of whatever it is currently computing.

Then the release, which is the part people skip:

```text
  after releasing the reference: 0 MB managed
  Dropping the last reference is what frees cluster memory. A persisted
  result you never release is a leak that outlives the computation.
```

`del persisted` is not a hint. It is the mechanism. Reference counting on the
client is the authority for what the cluster keeps, so a persisted collection
kept in a module-level cache, a notebook variable, or a closure holds worker
memory for as long as that reference lives — long after the computation that
made it finished.

**The advice the example ends on** is the actionable part:

```text
Staying under the limit is a chunking problem, not a hardware problem:
  - a worker needs room for several chunks at once, not just one
  - rule of thumb: chunk size around 100 MB, and well under limit/threads
  - unmanaged memory (numpy scratch, leaked references) counts too
  - persist() only what you will reuse; the rest should stay lazy
```

`limit / threads` for this cluster is 1.5 GiB / 2 = 768 MB per concurrent task,
covering its input chunk, its output chunk, and any scratch. A 100 MB chunk
sits comfortably inside that; a 700 MB chunk does not, even though it is
"under the limit".

**The traps.**

- **The `--memory-limit` is enforced by distributed, not the kernel.** Set it
  above the container's real cgroup limit and the OOM killer wins first — the
  worker dies with no spill, no pause, and no useful log line, just a nanny
  reporting a dead child.
- **Spilling is not free and not silent in the task stream.** A worker past
  `target` starts doing disk I/O; the time shows up as `disk-read` and
  `disk-write` phases, and the job gets slower for reasons that look like
  nothing.
- **Unmanaged memory is the usual cause of surprise deaths.** numpy temporaries
  inside a single task, a C library's arena, a cached DataFrame — none of it is
  spillable, and the worker's memory can be nearly all unmanaged while
  `managed_bytes` looks calm.
- **`pause` looks exactly like a hang.** No error, no progress, workers idle.
  The dashboard's memory bars are the fastest way to tell the difference.
- **Restarting a paused worker "fixes" it and loses your data.** The real fix is
  smaller chunks or fewer concurrent tasks.

### 0302_shared_storage — the client's filesystem is not the worker's

Source: [`examples/0302_shared_storage.py`](../../dask-distributed/examples/0302_shared_storage.py)

**What it teaches.** The single most common surprise when moving code from a
`LocalCluster` to a real deployment, demonstrated rather than asserted, and the
shape of the fix.

**The code.** The demonstration is three lines of setup and one `client.run`:

```python
with tempfile.TemporaryDirectory() as tmp:
    host_file = os.path.join(tmp, "client-only.txt")
    Path(host_file).write_text("written by the client\n")
    answers = client.run(path_exists_on_worker, host_file)
```

`client.run(fn, *args)` executes on every worker outside the task graph and
returns a dict keyed by worker address. It is the right tool for exactly this
kind of question — it is not a computation, it is an inspection.

```python
def path_exists_on_worker(path: str) -> tuple[str, bool]:
    """Report whether a path exists from the worker's point of view."""
    return socket.gethostname(), os.path.exists(path)
```

Then the shared volume, used the way a deployment would: one worker writes a
zarr store, every worker lists the directory, a different worker reads it back.

```python
def write_store(path: str, days: int, size: int, seed: int) -> tuple[str, int]:
    """Build a small climate-shaped dataset and write it as zarr.

    Runs on a worker, so the path must be valid inside that container.
    """
    ...
    ds.to_zarr(path, mode="w", consolidated=False)
    return socket.gethostname(), sum(1 for _ in Path(path).rglob("*") if _.is_file())
```

(`consolidated=False` because consolidated metadata is not part of the zarr v3
spec and writing it emits a warning — a detail shared with the `xarray` and
`icechunk` projects.)

**Real output:**

```text
First: write a file the CLIENT can see, and ask the workers about it.
  client wrote /var/folders/7t/m0y6vhq508n4fsfg85vhgjkh0000gp/T/tmp3kwt_eea/client-only.txt
  client sees it: True
  worker 72c10137d285 (172.19.0.3:36739) sees it: False
  worker 8027be35ef47 (172.19.0.4:41379) sees it: False
  worker 66de7753702c (172.19.0.5:36231) sees it: False

  False everywhere. The workers are separate containers with their own
  filesystems -- the path is meaningless to them. Passing it to to_zarr()
  would not error; it would write somewhere useless.

Now the shared volume: compose mounts one docker volume at /data
in the scheduler and every worker, so all containers see the same files.

  worker 72c10137d285 wrote /data/demo.zarr (8 files)

  every container now lists the same directory:
    172.19.0.3:36739       ['demo.zarr']
    172.19.0.4:41379       ['demo.zarr']
    172.19.0.5:36231       ['demo.zarr']

  worker 8027be35ef47 read it back: shape=(30, 64, 64), mean t2m=20.00 degC
  A DIFFERENT container than the one that wrote it -- the volume is genuinely shared.

  cleaned up the store: True
```

**Why it matters.** Three `False`s. That is the whole lesson, and it is the one
a `LocalCluster` will never show you, because on a `LocalCluster` all three
would say `True` and your code would work perfectly right up until you deployed
it.

The sentence in the middle is the dangerous part: **it would not error.** A
worker handed `/var/folders/.../out.zarr` will happily create that directory
inside its own container and write a complete, valid, correct zarr store into a
filesystem that disappears when the container does. Three workers writing
"the same" store means three partial stores in three containers. No exception,
no warning, no output. This is a data-loss bug with no error message, which is
the worst kind.

The second half shows the fix working. One container writes, all three list the
same directory, a *different* container reads it back and gets the right answer
— `mean t2m=20.00 degC` against a dataset built around 20.0. The volume is
genuinely shared.

And `client.run` deserves its own note. It is how you interrogate a cluster.
"What does every worker see?" is answerable in one line, and the same trick
covers environment variables (`client.run(os.environ.get, "AWS_REGION")`),
module versions, mount points, and DNS resolution. When something works on your
laptop and not on the cluster, `client.run` is the first debugging move.

**The traps.**

- **The bug is silent, and that is the whole problem.** Nothing raises. Design
  for it: assert on the worker side that a path exists before writing, or use a
  storage layer that cannot be ambiguous.
- **A shared volume is not the same as a shared store.** It solves
  worker-to-worker sharing and does nothing for the client, which `0303` makes
  painfully clear.
- **`client.run` returns per-worker results, so a partial truth is visible.** If
  two workers can see a mount and one cannot — a failed mount on one node — you
  will see `True, True, False`, which is the highest-value output in this whole
  example. Aggregating it to a single boolean would hide the interesting case.
- **Relative paths are worse than absolute ones.** A worker's working directory
  is not yours; `to_zarr("out.zarr")` writes somewhere determined by how the
  worker was launched.
- **The scheduler having the volume mounted proves nothing about the workers.**
  They are separate containers with separate mounts; check the workers.

### 0303_distributed_xarray — one graph, one path, two sides

Source: [`examples/0303_distributed_xarray.py`](../../dask-distributed/examples/0303_distributed_xarray.py)

**What it teaches.** Why a lazy zarr pipeline cannot be driven from a client
that cannot resolve the store's path, and the two pipeline shapes that do work.
This is the example that most directly justifies object storage, and it is the
one [Storage](../storage.md) cites.

**The constraint, stated.** A dask graph carries **one path string**. The client
builds the graph, which means the client opens the store to learn its shape,
chunking, and dtypes. The workers then execute tasks that open the same store
by the same string. Both sides use the same identifier, so it must mean the same
thing on both sides.

Against this cluster it does not:

```text
A dask graph carries ONE path string. Client and workers both use it,
so it has to mean the same thing on both sides. Watch it fail:
  the workers can write /data (a docker volume), but the client cannot:
    client sees /data: False
  so xr.open_zarr('/data/source.zarr') on the CLIENT raises FileNotFoundError,
  even though every worker could open it happily.

  In production the answer is a URL both sides resolve -- s3://bucket/store.zarr
  with each side pointing at its own endpoint. That is what open-climate-service
  does, and why its stores live in object storage rather than on a mounted disk.
```

That `False` is a direct consequence of the named volume in `compose.yml`. A
bind mount would have hidden it. The lesson is only visible because the setup
refuses to make it comfortable.

**Shape one: push the whole job to a worker.**

```python
def run_pipeline_on_worker(base: str, days: int, grid: int, time_chunk: int, space_chunk: int) -> dict[str, Any]:
    """Run the whole ingest-and-derive pipeline inside one worker.

    Everything -- writing the source store, reopening it lazily, deriving the
    climatology, writing the result -- happens on the worker's own filesystem,
    so no path ever has to be valid anywhere else.
    """
    ds = build_dataset(days, grid).chunk({"time": time_chunk, "y": space_chunk, "x": space_chunk})
    ds.to_zarr(source, mode="w", consolidated=False)

    opened = xr.open_zarr(source, consolidated=False, chunks={})
    climatology = opened["t2m"].groupby("time.month").mean()
    anomaly = (opened["t2m"].groupby("time.month") - climatology).chunk({"time": time_chunk})
    anomaly.to_dataset(name="t2m_anomaly").to_zarr(result, mode="w", consolidated=False)
```

The whole pipeline is one task. Write, reopen lazily, derive, write again — all
inside one container, so `/data/source.zarr` only ever has to be valid there.

```text
Option 1 -- send the whole pipeline to a worker, using /data:
  worker 8027be35ef47 ran write -> open -> climatology -> write in 4.69s
  source 174.4 MB, anomaly 178.1 MB
  climatology shape (12, 256, 256), anomaly mean +0.000000
  One worker did everything, so nothing needed a shared path -- but only
  one worker's cores were used. Fine for a per-dataset ingest job.
```

This is correct and it is a real deployment pattern — a per-dataset ingest job
farmed out one dataset per worker parallelizes perfectly at the job level. What
it is not is a cluster computation: one worker's two threads did all the work
while four other threads sat idle.

**Shape two: build the graph on the client, let the cluster run it.**

```python
ds = build_dataset(DAYS, GRID).chunk({"time": TIME_CHUNK, "y": SPACE_CHUNK, "x": SPACE_CHUNK})
climatology = ds["t2m"].groupby("time.month").mean()
anomaly = ds["t2m"].groupby("time.month") - climatology
```

```text
Option 2 -- build the graph on the client and let the CLUSTER run it.
Data generated in the graph itself needs no storage at all:
  the array is 191 MB in 52 chunks
  climatology graph:   345 tasks
  anomaly graph:      6185 tasks
  Nothing has run yet -- these are just graphs.

  compute() executed across the cluster in 1.72s
  climatology shape (12, 256, 256)
  monthly means (degC): 26.76 28.07 28.85 28.88 28.13 26.81 25.27 23.91 23.14 23.13 23.90 25.21
  anomaly mean, should be ~0: +0.000000

  Every chunk of that computation ran inside a worker container, in
  parallel across all three. The client held only the graph and the result.
```

1.72 s against 4.69 s for the same class of work, using all three workers.
(Machine-dependent, and the two are not doing identical work — option 1 also
writes two zarr stores — so read the ratio as illustrative rather than as a
benchmark.)

The graph sizes are worth pausing on. The climatology is 345 tasks; the anomaly
is 6,185. A `groupby("time.month")` subtraction over 52 chunks explodes into
thousands of tasks because every chunk must be matched against its month's mean.
Building all of them took milliseconds and touched no data — the lazy property
holding at cluster scale.

**The warning this example provokes**, which is genuinely instructive:

```text
UserWarning: Sending large graph of size 182.54 MiB.
This may cause some slowdown.
Consider loading the data with Dask directly
 or using futures or delayed objects to embed the data into the graph without repetition.
See also https://docs.dask.org/en/stable/best-practices.html#load-data-with-dask for more information.
```

The dataset was built **in client memory** with numpy and then chunked, so the
actual bytes are embedded in the graph, and shipping the graph ships 182 MiB.
This is `0201`'s lesson wearing a different hat: it is the same mistake as
passing a big array to a task, hidden inside a collection. Production code
avoids it by having the workers read the data (`xr.open_zarr` on a path they can
resolve) or generate it (`dask.array.random`), so the graph carries a recipe
rather than a payload.

**Why it matters.** The two working shapes bracket the design space, and
neither is a compromise you have to accept forever:

- **Push the job to the data.** Correct, simple, and single-worker. Right for
  per-dataset ingest.
- **Keep the data in the graph.** Fully parallel, and only possible when the
  data is generated or already distributed — no store involved.
- **The third shape, which needs object storage.** A lazy graph over a store both
  sides can open, computed across the cluster. That is what a service actually
  wants, and an `s3://bucket/store.zarr` URL is what makes it possible, because
  the string resolves identically on both sides while each configures its own
  endpoint and credentials.

That third shape is exactly the argument [Storage](../storage.md) develops at
length, and it is why open-climate-service's planned move to S3-backed icechunk
and its planned move to distributed dask are the same project.

**The traps.**

- **`FileNotFoundError` from the client on a path the workers can see is the
  signature failure.** Recognize it: it means the graph-building side and the
  executing side disagree about the filesystem.
- **A bind mount papers over it on one machine and fails on two.** If host and
  containers share `/data` via a bind mount, everything works until the cluster
  spans machines. Deferred, not fixed.
- **NFS-style shared mounts bring their own problems.** They restore the shared
  path and add latency, plus the conditional-write weakness that makes icechunk
  commits unsafe (see [Storage](../storage.md)).
- **`chunks={}` on `open_zarr` means "use the store's own chunking"**, which is
  usually right and is not the same as `chunks="auto"`.
- **Watch for the large-graph warning in your own code.** It is a reliable
  detector of accidentally embedding data in a graph.

---

## Phase 4 — Failure and elasticity

The half a `LocalCluster` cannot teach: workers die, tasks raise, and capacity
is a dial.

### 0401_worker_failure — killing a worker mid-flight

Source: [`examples/0401_worker_failure.py`](../../dask-distributed/examples/0401_worker_failure.py)

**What it teaches.** That worker death is survivable, how the nanny replaces a
worker, that the graph is the source of truth for recovery, and precisely what
recovery does not cover.

**The code.** The kill is as brutal as possible on purpose:

```python
def kill_this_worker() -> None:
    """Terminate the worker process immediately, with no cleanup.

    ``os._exit`` skips atexit handlers and shutdown hooks, which is exactly
    what an OOM kill or a yanked machine looks like from the outside.
    """
    os._exit(1)
```

`os._exit` rather than `sys.exit` or `Worker.close()` matters. A graceful
shutdown gives distributed a chance to hand off data and deregister cleanly,
which is a completely different scenario — that is retirement, covered in
`0403`. `os._exit` is what an OOM kill or a preempted spot instance looks like:
the process simply stops mid-instruction.

The example is also honest about the fallback, and refuses to run the
destructive part there:

```python
if not session.is_compose:
    print("\nThis example needs real worker processes to kill.")
    print("The fallback runs workers as threads inside THIS process, so killing")
    print("one would take the example down with it. Skipping the destructive part.")
```

Recovery is detected by polling the worker set:

```python
def worker_addresses(client: Any) -> set[str]:
    return set(client.scheduler_info().get("workers", {}))
```

Set difference then tells you what went and what arrived.

**Real output:**

```text
Starting with 3 workers:
  tcp://172.19.0.3:36739
  tcp://172.19.0.4:41379
  tcp://172.19.0.5:36231

Persisted 288 MB in 16 chunks.
  reference answer, computed while healthy: 18,001,543.63
  chunks per worker: 172.19.0.3:36739=6, 172.19.0.4:41379=6, 172.19.0.5:36231=5

Killing tcp://172.19.0.3:36739 with os._exit -- it holds 6 chunks.
Those chunks exist nowhere else. They are simply gone.
  the RPC died with the worker: CommClosedError (expected)

After 1.0s the scheduler reports 3 workers.
  gone:        tcp://172.19.0.3:36739
  replacement: tcp://172.19.0.3:34041
  A new address means a NEW process: the nanny supervising that container
  noticed the exit and started a fresh worker. The container never restarted.

Recomputing the same sum on the healed cluster:
  18,001,543.63 in 0.04s
  matches the reference: True
  The lost chunks were rebuilt from the graph. Same answer, extra work.
```

The nanny's side of the same second, from the container logs:

```text
worker-2  | 2026-08-17 17:48:42,748 - distributed.nanny - INFO - Worker process 120 exited with status 1
worker-2  | 2026-08-17 17:48:42,748 - distributed.nanny - INFO - Unregistering worker (status=Status.running)
worker-2  | 2026-08-17 17:48:42,749 - distributed.nanny - WARNING - Restarting worker (status=Status.running)
worker-2  | 2026-08-17 17:48:43,094 - distributed.worker - INFO -       Start worker at:     tcp://172.19.0.3:34041
worker-2  | 2026-08-17 17:48:43,101 - distributed.worker - INFO -         Registered to:       tcp://scheduler:8786
```

Under 350 ms from "process exited" to "registered", and about 1.0 s before the
client's poll noticed.

**Why it matters.** Four things are proven here that you cannot prove any other
way.

**The answer survived.** `18,001,543.63` before the kill and `18,001,543.63`
after, with six of sixteen chunks destroyed in between. dask's recovery model is
that the graph is the source of truth: every chunk is a recipe, and a recipe can
be re-run. Correctness survives worker loss; latency does not.

**`CommClosedError` is the expected outcome, not a bug.** The example even says
so. `client.run(kill_this_worker, workers=[victim])` cannot get a reply from a
process that just called `os._exit`. Code that kills or restarts workers must
expect the RPC itself to fail, which is unusual enough to be worth writing down.

**A new address means a new process.** `36739` became `34041` on the same
container IP. This is the observable signature of a nanny restart and the reason
worker addresses are unstable identifiers.

**The container never restarted.** `docker compose ps` shows the same uptime as
before. If you go looking for evidence of this failure in your orchestrator's
restart counts, you will find none.

**What recovery does not cover**, which the example states plainly:

```text
What this does NOT save you from:
  - scattered data: client.scatter puts data on workers with no recipe to
    rebuild it, so losing that worker loses the data for good
  - the client dying: the graph lives in the client process
  - a task that kills every worker it touches (a poison pill), which dask
    eventually gives up on rather than retrying forever
```

All three are worth internalizing. Scattered data has no recipe — that is the
price of the performance win in `0202`. The client is a single point of failure
for the graph, which is why long-running jobs want a durable checkpoint (a
written store) rather than a long-lived in-memory result. And a poison-pill task
— one that OOMs whatever worker touches it — will take out workers in sequence
until dask gives up, which is why `terminate` at 95% and a task that allocates
10 GB are a bad combination.

**The traps.**

- **Recovery is not free, it is just correct.** Recomputation costs whatever the
  lost work cost. A chunk that took ten minutes to compute takes ten minutes
  again.
- **Repeated failures blacklist a task.** dask tracks how often a task has been
  implicated in worker deaths and eventually errors it rather than retrying
  forever. That is a feature, and it looks like an inexplicable task error.
- **Pinned tasks may not recover at all.** `workers=[dead_address]` is a hard
  constraint; the scheduler will not move the task elsewhere.
- **A run of this example leaves the cluster with one young worker.** Harmless,
  and visible as a different port in later examples' output — several of the
  outputs quoted on this page show `172.19.0.3:34041` for exactly this reason.
- **Nanny restart is fast but not instant.** Under 400 ms here for a small
  worker; a worker whose startup imports a large library stack takes
  proportionally longer, and that is dead capacity.

### 0402_errors_and_retries — exceptions on workers, tracebacks on your screen

Source: [`examples/0402_errors_and_retries.py`](../../dask-distributed/examples/0402_errors_and_retries.py)

**What it teaches.** How a remote exception comes back, what `future.status`
and `future.traceback()` give you, when `retries=` helps and when it is a waste,
how a failure affects the rest of a batch — and, incidentally, one of the
sharpest gotchas in the whole project.

**The code.** Three tasks: one that always fails, one that fails a fixed number
of times, one that always works.

```python
def always_fails(value: int) -> int:
    raise ValueError(f"this task was never going to work (value={value})")
```

The flaky one is where it gets interesting, and its comment is the lesson:

```python
def flaky(key: str, fail_times: int) -> str:
    # State has to live on the Worker OBJECT, not in a module global. A
    # function defined in __main__ is pickled by value together with its
    # globals, so each task execution unpickles a fresh namespace and a
    # module-level counter would read zero forever.
    from distributed import get_worker

    try:
        worker = get_worker()
    except ValueError:
        # Not running on a worker (the synchronous fallback path).
        store = _ATTEMPTS
    else:
        existing: dict[str, int] | None = getattr(worker, "_ocs_stack_attempts", None)
        if existing is None:
            existing = {}
            setattr(worker, "_ocs_stack_attempts", existing)
        store = existing

    seen = store.get(key, 0) + 1
    store[key] = seen
    if seen <= fail_times:
        raise ConnectionError(f"transient failure on attempt {seen}")
    return f"succeeded on attempt {seen} on {socket.gethostname()}"
```

Read that comment twice, because it describes a trap that costs people hours.
The obvious way to write a "fails twice then succeeds" function is a module-level
`_ATTEMPTS` dict. It does not work when the function is defined in `__main__`
and shipped to a worker: cloudpickle serializes such a function **by value**,
carrying a copy of the globals it references, so each execution unpickles a
fresh namespace with the counter back at zero. The function fails forever.

The fix is `distributed.get_worker()`, which returns the `Worker` object of the
process you are running in, and stashing state as an attribute on it. That
object is genuinely per-worker and genuinely persistent for the life of the
process. The `getattr`/`setattr` form rather than plain attribute syntax is a
concession to strict type checkers — `Worker` declares no such attribute —
and stashing state on the worker is the documented way to keep per-worker state.

Even that is not enough on its own, which is the second half of the trap:

```python
pinned_to = sorted(client.scheduler_info()["workers"])[0]
client.submit(flaky, "with-retry", 2, retries=3, pure=False, workers=[pinned_to]).result()
```

A retry may land on a *different* worker, whose counter starts at zero, so the
task would fail forever anyway. Pinning keeps retries in the process doing the
counting. `pure=False` stops dask from deduplicating the "no-retry" and
"with-retry" calls into one.

**Real output:**

```text
A task that raises on a worker:
  submit() returned immediately; future.status is now 'pending'
  after the task ran, status is 'error'
  result() re-raises it here: ValueError: this task was never going to work (value=42)
  future.traceback() gives the WORKER's traceback: traceback
  You debug with the stack from the machine that actually failed.

A flaky task that fails twice, then works.
  (pinned to 172.19.0.3:34041: the attempt counter is worker-local,
   and retries can otherwise land on a worker that has never seen it)
  Without retries:
    ConnectionError: transient failure on attempt 1
  With retries=3:
    succeeded on attempt 3 on 72c10137d285
  dask re-ran the task in place; the client never saw the failures.

Retries only help when the fault is transient:
  a real bug retried 3 times is still a bug -> ValueError: this task was never going to work (value=7)
  Three retries here bought nothing but latency.

One bad task among many does not cancel the good ones:
  5 finished, 1 errored out of 6
  the successful results are still available: [0, 2, 4, 6, 8]
  (done-before-wait was 0; futures resolve asynchronously)

  gather() on the whole batch raises on the first failure, so pass
  errors='skip' when you would rather keep the partial results.
  gather(errors='skip') -> [0, 2, 4, 6, 8]
```

**Why it matters.** The exception arrived on the client as a normal Python
exception, with the worker's traceback attached. That is not a small
convenience: without it, debugging a distributed job would mean correlating
container logs by timestamp. It is the single feature that makes distributed
debugging feel like ordinary debugging.

The worker side of that same failure, from the container logs, shows what dask
is packaging up for you:

```text
worker-1  | 2026-08-17 17:48:44,666 - distributed.worker - ERROR - Compute Failed
worker-1  | Key:       always_fails-81300c72-a02a-408b-a1ce-a905a02e85a4
worker-1  | State:     executing
worker-1  | Task:  <Task 'always_fails-81300c72-a02a-408b-a1ce-a905a02e85a4' always_fails(...)>
worker-1  | Exception: "ValueError('this task was never going to work (value=7)')"
worker-1  | Traceback: '  File "/Users/morteoh/.../dask-distributed/examples/0402_errors_and_retries.py", line 38, in always_fails\n'
```

(The user-specific middle of the path is elided. Everything else is verbatim.)

Note the file path in that traceback: `/Users/morteoh/...`. That path does not
exist in the container. The worker is reporting the source location the function
carried with it when it was pickled from the client's `__main__`. This is the
same by-value pickling that breaks the module-global counter, showing up as a
harmless-but-confusing artifact in a log.

That log also shows the retry landing on two different containers — the same key
appears under `worker-1` and `worker-2` — which is the mechanism the pin exists
to defeat.

The batch behaviour is the last piece. One failure among six did not cancel the
others; five finished and are still gathering-able. `gather()` on the whole list
raises on the first failure, and `errors="skip"` returns what succeeded. Which
you want is a real design decision: a transactional batch should raise, a
best-effort fan-out over a thousand files should skip and report.

**The traps.**

- **`retries=` on a deterministic bug is pure latency.** Three retries of a
  `ValueError` cost three executions and produce the same `ValueError`.
- **Retries move between workers.** Any per-worker state your retry depends on
  is not there. Pin, or make the task stateless.
- **Functions defined in `__main__` are pickled by value with a copy of their
  globals.** Module-level mutable state does not persist between executions.
  Use `get_worker()` for genuinely worker-local state, and remember it dies with
  the process.
- **Polling `future.status` in a loop is a busy-wait.** Use `gather`,
  `distributed.wait`, or `as_completed`.
- **`errors="skip"` silently drops failures.** Convenient and dangerous: log
  what was skipped, or you will ship a pipeline that quietly produces partial
  output.

### 0403_scaling — what capacity buys

Source: [`examples/0403_scaling.py`](../../dask-distributed/examples/0403_scaling.py)

**What it teaches.** That throughput tracks total threads rather than worker
count, measured rather than asserted; the difference between retiring a worker
and killing it; and that adding capacity is an infrastructure action rather
than a client call.

**The code.** The measurement is careful in a way worth copying. Rather than
actually scaling the shared cluster down — which would be rude, and permanent —
it restricts each batch to a subset of workers:

```python
addresses = sorted(client.scheduler_info()["workers"])
for count in range(len(addresses), 0, -1):
    subset = addresses[:count]
    client.gather([client.submit(unit_of_work, i, pure=False, workers=subset) for i in range(N_TASKS)])
```

The comment in the source explains the choice: restricting the batch measures
the effect of a smaller cluster without shrinking the shared one, because a
graceful retirement is permanent and the nanny does not undo it. That is both
good manners toward a shared resource and a more honest measurement — the
workers are identical, so N of them at 2 threads is a faithful stand-in for a
cluster of N.

`pure=False` is required here for a subtle reason: without it, the identical
`unit_of_work(i)` calls across the three rounds would be deduplicated by key and
the later rounds would return instantly from cache, measuring nothing.

**Real output:**

```text
Current capacity: 3 workers, 6 threads.
Running 24 tasks of 0.25s each.
  wall time 1.29s versus 6.0s serial (4.6x)
  with 6 slots the floor is about 1.00s

The same batch, restricted to fewer workers with workers=:
  3 worker(s), 6 slots:  1.04s  ########
  2 worker(s), 4 slots:  1.54s  ############
  1 worker(s), 2 slots:  3.06s  ########################
  Halving the slots roughly doubles the wall time: throughput tracks
  total threads, and nothing failed when capacity got tight.
```

(Machine-dependent. The same ladder is quoted in [Scaling](../scaling.md) from a
different run, at 1.05 / 1.56 / 3.08 — close enough to show the effect is stable
and the exact figures are not.)

**Why it matters.** The ladder is as close to a clean inverse relationship as
you will see in practice. 6 slots, 4 slots, 2 slots against 1.04 s, 1.54 s,
3.06 s. Six slots into 24 tasks of 0.25 s is a 1.00 s floor and 1.04 s was
achieved; two slots is a 3.00 s floor and 3.06 s was achieved. Overhead is
roughly constant and small.

The first measurement, 1.29 s, is higher than the 1.04 s of the second round on
the same six slots. That is warm-up: the first batch pays for the client
connecting, the scheduler seeing these task functions for the first time, and
the workers importing what they need. It is a real effect and a good reason to
distrust the first number in any benchmark.

The critical qualifier is that this workload is the friendliest possible one —
24 independent, identical, input-free tasks. Read the ladder as an upper bound
on what capacity buys. `0503_task_stream` immediately provides the counterexample.

**Retire versus kill**, which the example explains rather than runs:

```text
To genuinely remove a worker, client.retire_workers() is the safe way.
Unlike killing it, retiring MOVES its data to the survivors first, then
stops it -- the drain a rolling deploy needs. Two things to know:
  - it is permanent: a retired worker is not restarted by its nanny,
    unlike a crashed one, so capacity has to be added back explicitly
  - data with no recipe (anything scattered) is preserved by the drain,
    which is exactly what killing the worker would have destroyed
This example does not run it, so the cluster it shares stays intact.
```

This is the operational distinction that matters most in a deployment:

| | kill (`os._exit`, `docker stop`, OOM) | retire (`client.retire_workers()`) |
|---|---|---|
| data with a recipe | lost, recomputed later | moved to survivors |
| scattered data | **lost permanently** | moved to survivors |
| the nanny | restarts the worker | does not |
| in-flight tasks | rescheduled elsewhere | allowed to finish first |
| right for | nothing; it is what failure looks like | rolling deploys, scale-down, node drain |

The "permanent" point is the one that surprises people. A crashed worker comes
back automatically; a retired one does not. Retirement is a statement of intent,
and capacity has to be added back explicitly.

**Scaling up is not a client call:**

```text
Growing the cluster happens outside Python:
  make scale N=5        add containers to this compose cluster
  docker compose up -d --scale worker=5
In Kubernetes it is a replica count; with dask-kubernetes or
adaptive clusters, cluster.adapt(minimum=2, maximum=10) lets the
scheduler request capacity based on the queue itself.
```

The asymmetry is inherent. Removing a worker is something the cluster can do to
itself, because the worker is already there. Adding one requires somebody to
provide a machine, and only the layer that owns machines can do that. Adaptive
clusters close the loop by letting the scheduler *ask* an infrastructure
provider for capacity — which is a different thing from having it.

**The traps.**

- **Independent equal-sized tasks are the best case.** Dependencies serialize
  parts of the graph, unequal durations leave slots idle at the tail, and inputs
  cost transfer.
- **Doubling workers cannot beat the longest single task.** See `0503`.
- **`docker compose --scale worker=2` is a kill, not a drain.** Data on those
  containers is gone. Retire first if it matters.
- **Retirement is permanent.** Plan for how capacity comes back.
- **Adding workers can make a memory-bound job worse.** More workers means more
  concurrent tasks means more peak memory, and can also mean more transfer if
  the data is now spread thinner.
- **The first batch is always slower.** Warm up before you measure.

---

## Phase 5 — Observability

The dashboard is the first thing to open when a cluster misbehaves, but "it
looked busy" is not a diagnosis. These three examples turn every panel into a
number you can assert on, freeze a run into a shareable file, and locate a
bottleneck from the records.

### 0501_dashboard_tour — every panel, as data

Source: [`examples/0501_dashboard_tour.py`](../../dask-distributed/examples/0501_dashboard_tour.py)

**What it teaches.** That each dashboard panel is backed by a scheduler endpoint
you can query, what the task stream's colours actually mean, and how to compute
parallel efficiency as a single number.

**The code.** The worker table comes straight from `scheduler_info()`:

```python
info = client.scheduler_info()
workers: dict[str, Any] = info.get("workers", {})
for address, meta in sorted(workers.items()):
    metrics = meta.get("metrics", {})
    print(f"  {address.rsplit('/', 1)[-1]:<24} "
          f"{meta.get('nthreads', 0):>7} "
          f"{meta.get('memory_limit', 0) / 2**30:>10.2f} "
          f"{metrics.get('managed_bytes', 0) / 1e6:>11.1f} "
          f"{metrics.get('cpu', 0):>6.1f}")
```

The task stream comes from a context manager that records while a computation
runs:

```python
with get_task_stream() as stream:
    started = time.perf_counter()
    result = float(sample_workload().compute())
    elapsed = time.perf_counter() - started

records: list[dict[str, Any]] = list(stream.data)
```

Each record is a dict describing one task execution: its key, the worker that
ran it, the number of bytes it produced, and — the useful part — a list of
`startstops` spans:

```python
totals: defaultdict[str, float] = defaultdict(float)
for record in records:
    for span in record.get("startstops", []):
        totals[str(span["action"])] += float(span["stop"]) - float(span["start"])
```

Each span is `{"action": ..., "start": ..., "stop": ...}`, one per phase the
worker went through for that task: `compute`, `transfer`, `disk-read`,
`disk-write`, `deserialize`. These are exactly the colours the dashboard's task
stream draws with, so summing them tells you where the cluster's time went, by
category.

The workload is deliberately sized so arithmetic rather than scheduling
dominates:

```python
def sample_workload() -> Any:
    array = da.random.random((8000, 8000), chunks=(1000, 1000))
    return ((array**2 + array).mean(axis=0) ** 0.5).sum()
```

The `mean(axis=0)` is what makes it interesting: reducing along an axis forces
cross-chunk combination, so there is genuine transfer to observe rather than
embarrassing parallelism.

**Real output:**

```text
Dashboard: http://127.0.0.1:8787/status
Everything below is what those panels are showing, fetched over the API.

--- Panel: Workers ---
Per-worker threads, memory, and how hard each one is working.
  worker                   threads  limit GiB  managed MB  cpu %
  172.19.0.3:34041               2       1.50         0.0    6.0
  172.19.0.4:41379               2       1.50         0.0    4.0
  172.19.0.5:36231               2       1.50         0.0    2.0

--- Panels: Task Stream and Progress ---
Running a workload and capturing what the task stream would draw.
  result 7,302.72 computed in 0.36s from 164 tasks
  task types (the colored bars in the stream):
    random_sample                  64
    mean_chunk                     64
    mean_combine                   16
    sum                            11
    mean_agg                        8
  tasks per worker (the stream's rows):
    172.19.0.3:34041            9
    172.19.0.4:41379           78
    172.19.0.5:36231           77

--- Panel: Task Stream colors ---
Each bar is colored by what the worker was doing. The categories:
    compute            0.533s  ( 95.6%)
    transfer           0.025s  (  4.4%)

--- Reading it as a health check ---
  0.53s of compute across 6 slots in 0.36s wall
  parallel efficiency: 25% of available slot-time was real work
```

(Trimmed: the explanatory prose between panels is omitted. Timings
machine-dependent.)

**Why it matters.** Three readings come out of that output, and two of them are
the interesting kind — the kind where the numbers disagree with the story you
expected.

**The task-type breakdown is the graph, made legible.** 64 `random_sample`
(one per chunk of the 8x8 chunk grid), 64 `mean_chunk` (one per chunk),
16 `mean_combine`, 11 `sum`, 8 `mean_agg`. That is the shape of a tree
reduction: wide at the leaves, narrowing as partial results combine. When a task
type you did not expect dominates the count, that is a graph problem, and this
is where you see it.

**95.6% compute against 4.4% transfer is a healthy ratio.** Transfer is
overhead — data in the wrong place. A stream where transfer approaches or
exceeds compute means the scheduler is spending its time moving bytes, which is
a locality problem (`0203`) or a chunking problem, not a capacity problem.

**25% parallel efficiency is the number that looks alarming and mostly is not.**
0.53 s of compute across 6 slots in 0.36 s of wall time. Six slots for 0.36 s is
2.16 slot-seconds available; 0.53 s of it was compute. The reason is simply that
the job is too short: 164 tasks in 360 ms means each task averaged about 3 ms,
which is well under the ~8 ms per-task floor measured in `0201`. Scheduling
overhead dominates, and no amount of hardware fixes it. The lesson is not "this
cluster is broken" but "this workload is too fine-grained to be worth
distributing" — larger chunks, fewer tasks.

**The 9 / 78 / 77 split deserves a straight answer**, because it is the most
striking thing in the output and it would be easy to write a confident wrong
explanation. What can be said with certainty: the worker that got 9 tasks
(`172.19.0.3:34041`) was the one restarted by the nanny during `0401`, which ran
minutes earlier in the same `run-all` pass, so it was the newest process in the
cluster. What is plausible but not proven from this output alone: a
freshly-registered worker has no measured task-duration history, and over a
360 ms job the scheduler simply never rebalanced toward it. What is actionable
regardless: **a sub-second job is far too short to draw conclusions about
balance from**, and if you see a split like this on a job that matters, the next
step is `0503`'s per-worker analysis over a longer run rather than a theory.

**The traps.**

- **Efficiency is only meaningful for jobs long enough to amortize overhead.**
  Computing it for a 360 ms job produces a scary number that means "your tasks
  are too small", not "your cluster is broken".
- **`get_task_stream()` is typed as async and returns a recorder in a sync
  context.** The example reaches for `stream.data` with a pyright suppression;
  that is the documented shape, not a hack.
- **The task stream has a bounded buffer.** It keeps a rolling window of recent
  tasks, so a very long job's early history is gone. Use
  `performance_report()` when you need the whole run.
- **`cpu` in the worker metrics is a sampled instantaneous value.** Six percent
  and two percent in an idle moment mean nothing; watch it during work.
- **`managed_bytes` reading 0.0 does not mean the worker is empty.** It means
  nothing is *held* — data flowing through tasks and released immediately never
  shows up.

### 0502_performance_report — freezing a run into one file

Source: [`examples/0502_performance_report.py`](../../dask-distributed/examples/0502_performance_report.py)

**What it teaches.** How to capture a whole computation into a self-contained
HTML file, what is inside it, and the order to read the panels in.

**The code.** One context manager:

```python
from distributed import performance_report

with performance_report(filename=str(target)):
    result = float(mixed_workload().compute())
```

Everything the dashboard would have shown during that block is recorded and
written out when the block exits. The workload is chosen to have both phases
worth looking at:

```python
def mixed_workload() -> Any:
    """Return a lazy computation with both compute and transfer phases.

    A reduction along one axis forces cross-chunk combination, so the report
    has bandwidth to show rather than pure embarrassing parallelism.
    """
    array = da.random.random((6000, 6000), chunks=(750, 750))
    scaled = (array**2 + array) ** 0.5
    return (scaled.mean(axis=0) ** 2).sum() + scaled.mean(axis=1).max()
```

Reducing along both axes guarantees cross-chunk traffic in both directions, so
the bandwidth panel has something in it.

The report is written into the project rather than a temp directory, and the
source comment says why: the point of a report is that it outlives the run.
`make clean` removes the directory.

The example then verifies its own output rather than trusting it:

```python
text = target.read_text(errors="ignore")
for name, description in SECTIONS:
    present = "yes" if name.lower().replace(" ", "") in text.lower().replace(" ", "") else " - "
```

**Real output:**

```text
Running a workload inside performance_report(), writing to:
  /Users/morteoh/.../dask-distributed/reports/performance-report.html

  computed 4,237.57 in 0.53s
  wrote 141 KB of self-contained HTML
  Self-contained means no CDN and no running cluster: open it anywhere.

Sections in the report:
  [yes] Task Stream        every task as a colored bar, per worker row
  [yes] Bandwidth          bytes moved between each pair of workers
  [yes] Memory             managed and unmanaged memory over the run
  [yes] Summary            totals: compute time, transfer time, task counts
  [yes] Worker Profile     aggregated stack samples -- which lines burned CPU
  [yes] Scheduler Profile  the same for the scheduler's own event loop

How to read one, in order:
  1. Summary first -- is the time going to compute, or to transfer?
  2. Task Stream -- are there gaps (starved workers) or long red bars?
  3. Bandwidth -- is one worker pair moving most of the data?
  4. Worker Profile -- if compute dominates, which lines are hot?
```

(The user-specific middle of the path is elided. Everything else is verbatim.)

**Why it matters.** 141 KB of self-contained HTML for a half-second job. No CDN,
no JavaScript fetched at open time, no running cluster required. You can attach
it to an issue, commit it next to a benchmark, mail it to whoever asked why the
nightly job got slower, or diff two of them from different weeks.

The contrast with the dashboard is the point. The dashboard is live and
ephemeral: when the cluster stops, the evidence stops with it. Most performance
questions arrive *after* the job — "why was last night's ingest slow?" — and by
then the dashboard has nothing to say. A performance report is the artifact that
answers that question, and it costs essentially nothing to record.

The six panels map onto the six questions you would ask in order, and the
reading order the example prints is genuinely the right one:

1. **Summary** — compute versus transfer totals, task counts. This is the
   triage: a transfer-dominated run is a locality problem and a
   compute-dominated one is an algorithm problem, and they have nothing in
   common.
2. **Task Stream** — the per-worker rows over time. Gaps mean starved workers
   (not enough parallelism, or a dependency bottleneck). Long bars of one colour
   mean a straggler. A staircase pattern means unnecessary serialization.
3. **Bandwidth** — bytes per worker pair. One hot pair means data is
   concentrated where it should not be.
4. **Worker Profile** — statistical stack samples aggregated across workers,
   which tells you which *lines of Python* burned CPU. This is where a
   compute-bound job's real answer lives.
5. **Memory** — managed versus unmanaged over time. Rising unmanaged memory is
   the signature of a leak.
6. **Scheduler Profile** — the scheduler's own event loop. Relevant when the
   graph has so many tasks that the scheduler itself is the bottleneck.

**The traps.**

- **`performance_report()` needs bokeh on the client.** The example checks and
  says so if the file is missing. It is a client-side dependency, not a cluster
  one.
- **The report covers the block, not the process.** Anything outside the `with`
  is invisible, which is easy to get wrong when the interesting part is a
  warm-up or a write that happens after.
- **Reports get large for long runs.** 141 KB here for half a second; a
  multi-hour job with millions of tasks produces something you will not want to
  open in a browser tab casually.
- **Profiles are sampled, not exhaustive.** A function that runs rarely but
  expensively can be under-represented.
- **Writing into the repo means remembering to clean up.** `make clean` removes
  `reports/`; the alternative is committing a binary-ish artifact by accident.

### 0503_task_stream — finding the bottleneck from the records

Source: [`examples/0503_task_stream.py`](../../dask-distributed/examples/0503_task_stream.py)

**What it teaches.** How to turn task-stream records into a diagnosis: time by
task type, the straggler ranking, per-worker balance, and the single most
useful derived number in the whole project — the parallel floor.

**The code.** The workload is a deliberate trap: a few long tasks among many
short ones.

```python
N_FAST = 40
N_SLOW = 3
FAST_SECONDS = 0.05
SLOW_SECONDS = 1.5
```

40 x 0.05 s + 3 x 1.5 s = 6.5 s of work into 6 slots. Naively that is a 1.08 s
job. The point is that it is not.

The analysis primitive is one function:

```python
def span_seconds(record: dict[str, Any], action: str) -> float:
    """Total the time a task spent in one phase."""
    return sum(
        float(span["stop"]) - float(span["start"]) for span in record.get("startstops", []) if span["action"] == action
    )
```

Everything else is aggregation over that: group by task type to find which
*operation* is expensive, sort descending to find stragglers, group by worker to
find imbalance.

```python
ranked = sorted(records, key=lambda r: span_seconds(r, "compute"), reverse=True)
```

**Real output:**

```text
Submitting 40 tasks of 0.05s and 3 of 1.5s
into 6 slots. Total work: 6.5s.

Wall time 1.54s across 43 recorded tasks.

Compute time by task type:
  slow_task        3 tasks    4.51s  ( 68.4% of compute)
  fast_task       40 tasks    2.08s  ( 31.6% of compute)

Slowest individual tasks:
  slow_task        1.50s on 172.19.0.5:36231
  slow_task        1.50s on 172.19.0.4:41379
  slow_task        1.50s on 172.19.0.3:34041
  fast_task        0.06s on 172.19.0.5:36231
  fast_task        0.06s on 172.19.0.3:34041

Load per worker:
  172.19.0.3:34041        14 tasks   2.19s ########
  172.19.0.4:41379        15 tasks   2.23s ########
  172.19.0.5:36231        14 tasks   2.18s ########
  busiest minus idlest: 0.05s of compute

--- Diagnosis ---
  total compute 6.59s over 6 slots -> a 1.10s floor
  actual wall time 1.54s, parallel efficiency 71%
  the single longest task took 1.50s
  That one task alone exceeds the 1.10s floor: no amount of extra
  workers can finish this batch faster than its slowest single task.
  The fix is splitting that task, not growing the cluster.
```

**Why it matters.** This output is a complete diagnosis, and it is worth walking
through as a template for diagnosing anything.

**Three tasks out of 43 hold 68.4% of the compute time.** The first question of
any performance investigation — where does the time actually go? — is answered
in one table. Optimizing `fast_task` by 50% would save about 1 second of
compute; eliminating one `slow_task` saves 1.5 s.

**Load is almost perfectly balanced.** 2.19 / 2.23 / 2.18 seconds, a spread of
0.05 s. This rules out the entire class of placement explanations. Nobody is
idle, nobody is overloaded, and yet the job is 40% slower than its floor. That
elimination is exactly what a per-worker view is for.

**The floor is 1.10 s and the wall time is 1.54 s.** 6.59 s of compute over 6
slots cannot possibly finish faster than 1.10 s. Efficiency is 71%, which is
respectable.

**The longest single task is 1.50 s, which exceeds the floor.** This is the
punchline, and it is a hard constraint, not a heuristic. Some worker has to run
that 1.5 s task from start to finish, so the batch cannot finish in less than
1.5 s no matter how many workers you add. Doubling the cluster to 12 slots
would drop the floor to 0.55 s and leave the wall time essentially unchanged at
about 1.5 s. **The fix is splitting the task, not growing the cluster.**

That single comparison — longest task versus `total_compute / slots` — is the
cheapest useful thing you can compute about any batch, and it decides whether
"add hardware" is even a candidate answer.

**Why sleep-based tasks are the right choice here.** `time.sleep` releases the
GIL and consumes no CPU, so the wall time is governed purely by slot occupancy.
That makes the measurement about *scheduling* rather than about the host's CPU,
which is what the example is teaching. If the tasks burned CPU, the results
would be entangled with how many real cores the machine has and what else was
running.

**The traps.**

- **A perfectly balanced cluster can still be badly bottlenecked.** Balance and
  efficiency are different questions; check both.
- **`total_compute / slots` is a floor, not a target.** It ignores dependencies,
  transfer, and the tail of a batch.
- **The straggler ranking finds slow tasks, not slow *causes*.** A 1.5 s task
  might be slow because of the work, because it waited on a lock, or because it
  read spilled data from disk. The `startstops` breakdown by action separates
  those.
- **Records only cover what the stream captured.** Tasks completing after the
  context manager exits are missing, and the buffer is bounded.
- **Recorded compute totals slightly exceed the nominal work** — 6.59 s measured
  against 6.5 s submitted — because measured spans include per-task entry and
  exit overhead. Do not treat these as exact.

---

## The four things a LocalCluster hides

This project exists because of this section. A `LocalCluster` gives you the
distributed scheduler, the futures API, work stealing, and the dashboard — and
it is a genuinely good production answer for single-machine work. What it
cannot give you is any of the four constraints below, and all four are things
that break on the day you deploy. Each is stated here with the concrete
evidence from the examples.

### 1. Serialization cost

**What a LocalCluster shows you.** Nothing. With `processes=False` — the shape
this project's fallback uses — arguments are passed by reference within one
process. Passing a 500 MB DataFrame to a task costs a pointer copy. The
`inproc://` scheme in the fallback's address is the honest label for this: there
is no protocol, no socket, and no serialization step at all.

**What the real cluster shows you.** Every argument is pickled, pushed through a
socket, and unpickled on the other side.

The floor, from `0201_serialization`:

```text
A task with an int argument round-trips in 7.9 ms.
```

The tax, same example:

```text
Sending the 8.0 MB array as an argument: 77 ms
Building the same array on the worker:      20 ms
  Shipping cost about 3.9x the worker-side build.
```

The multiplier, from `0202_scatter_gather` — one 5.1 MB array needed by twelve
tasks:

```text
Version 1 -- pass the array to every task:
  400 ms
Version 2 -- scatter the array once, then pass the future:
  total:          82 ms
  scatter version was 4.9x the speed of the naive one
```

And the same mistake wearing a collection's clothes, from `0303`:

```text
UserWarning: Sending large graph of size 182.54 MiB.
```

That warning fired because the dataset was built in client memory with numpy
and then chunked, so 182 MiB of actual data was embedded in the graph and
shipped to the scheduler. On a `LocalCluster` this is free. On a real cluster it
is 182 MiB on a socket before any work starts.

**Why it matters.** Code written and tuned against a `LocalCluster` is
systematically biased toward shipping data, because shipping data was free while
you were writing it. The bias is invisible in tests, invisible in review, and
shows up as a job that is inexplicably slower on the cluster than on a laptop —
a sentence that gets said about real systems more often than it should.

**What to do instead.** Send parameters rather than payloads: a path, a URL, a
size, a seed, a date range. Let workers load or generate what they need.
`scatter` when the client genuinely holds data many tasks need. Watch for the
large-graph warning as a reliable detector, and prefer `dask.array` /
`xr.open_zarr` sources over `from_array` on something you built locally.

### 2. Filesystem isolation

**What a LocalCluster shows you.** Nothing, and worse than nothing — it actively
confirms the wrong belief. Workers are threads in your process, so they share
your filesystem, your working directory, your temp directory, and your
environment variables. `ds.to_zarr("/tmp/out.zarr")` works perfectly.

**What the real cluster shows you**, from `0302_shared_storage`:

```text
  client wrote /var/folders/7t/m0y6vhq508n4fsfg85vhgjkh0000gp/T/tmp3kwt_eea/client-only.txt
  client sees it: True
  worker 72c10137d285 (172.19.0.3:36739) sees it: False
  worker 8027be35ef47 (172.19.0.4:41379) sees it: False
  worker 66de7753702c (172.19.0.5:36231) sees it: False
```

And the sentence that follows, which is the whole hazard:

```text
  False everywhere. The workers are separate containers with their own
  filesystems -- the path is meaningless to them. Passing it to to_zarr()
  would not error; it would write somewhere useless.
```

The second half of the constraint, from `0303_distributed_xarray` — a shared
volume fixes worker-to-worker sharing and does not fix the client:

```text
    client sees /data: False
  so xr.open_zarr('/data/source.zarr') on the CLIENT raises FileNotFoundError,
  even though every worker could open it happily.
```

**Why it matters.** This is a data-loss bug with no error message. Three workers
handed a client-local path will each create that directory inside their own
container and write a valid, complete, wrong store into a filesystem that
vanishes with the container. Nothing raises. Nothing warns. The job reports
success.

And a shared volume is only half a fix. It makes the *workers* agree with each
other, which is enough for "push the whole job to a worker" pipelines and not
enough for lazy graphs, because a lazy graph is built by the client — which must
open the store to learn its shape — and executed by workers, using one identical
path string.

**What to do instead.** Give workers an identifier they can resolve: a mount
present in every container, or better, an object-store URL. `s3://bucket/store.zarr`
resolves identically on both sides while each configures its own endpoint and
credentials, which is precisely why [Storage](../storage.md) concludes that
distributed compute forces object storage. Use `client.run` to verify rather
than assume — three booleans is a cheap assertion. And be suspicious of any
absolute path in a graph that came from `tempfile`, `Path.cwd()`, or `~`.

### 3. Worker death

**What a LocalCluster shows you.** Nothing, and it cannot. With
`processes=False`, killing a worker means killing a thread in your own process,
which takes your script with it. `0401_worker_failure` says so and refuses to
run the destructive part:

```text
This example needs real worker processes to kill.
The fallback runs workers as threads inside THIS process, so killing
one would take the example down with it. Skipping the destructive part.
```

**What the real cluster shows you.** A worker killed with `os._exit(1)` while
holding six of sixteen chunks:

```text
Killing tcp://172.19.0.3:36739 with os._exit -- it holds 6 chunks.
Those chunks exist nowhere else. They are simply gone.
  the RPC died with the worker: CommClosedError (expected)

After 1.0s the scheduler reports 3 workers.
  gone:        tcp://172.19.0.3:36739
  replacement: tcp://172.19.0.3:34041

Recomputing the same sum on the healed cluster:
  18,001,543.63 in 0.04s
  matches the reference: True
```

The nanny's own account, from the container logs:

```text
worker-2  | distributed.nanny - INFO - Worker process 120 exited with status 1
worker-2  | distributed.nanny - INFO - Unregistering worker (status=Status.running)
worker-2  | distributed.nanny - WARNING - Restarting worker (status=Status.running)
worker-2  | distributed.worker - INFO -       Start worker at:     tcp://172.19.0.3:34041
```

And the fact that surprises everyone the first time:

```console
$ docker compose ps --format "table {{.Name}}\t{{.Status}}"
dask-distributed-worker-1      Up 4 hours
dask-distributed-worker-2      Up 2 hours
dask-distributed-worker-3      Up 4 hours
```

No container restarted. The nanny replaced a child process one layer below
Docker's notice.

**Why it matters.** Workers die routinely in production — OOM kills, preempted
spot instances, node drains, rolling deploys. dask's answer is that the graph is
the source of truth and anything lost can be rebuilt, and that answer is
excellent. But it has holes, and you want to know where they are *before* you
meet them:

- **Scattered data has no recipe** and is lost permanently.
- **The client is a single point of failure** — the graph lives in its memory.
- **Poison-pill tasks** that kill every worker they touch eventually get errored
  rather than retried forever, which looks inexplicable if you do not know it.
- **Pinned tasks may not recover**, because `workers=` is a hard constraint by
  default.
- **Retries move between workers**, so worker-local state does not survive them.

**What to do instead.** Assume workers die. Prefer recomputable data over
scattered data for anything long-lived. Checkpoint long jobs to durable storage
rather than holding a multi-hour in-memory result. Use `allow_other_workers=True`
alongside `workers=` when the pin is a preference. And use
`client.retire_workers()` rather than a hard stop whenever the removal is
planned — retirement drains data first, and it is the only way scattered data
survives a scale-down.

### 4. Version skew

**What a LocalCluster shows you.** Nothing, by construction. There is one Python
process, so client and worker cannot disagree about anything.

**What the real cluster shows you**, from `0102_versions`:

```text
  package        client           scheduler        workers          match
  -------------- ---------------- ---------------- ---------------- -----
  python         3.13.14.final.0  3.13.14.final.0  3.13.14.final.0  yes
  dask           2026.7.1         2026.7.1         2026.7.1         yes
  distributed    2026.7.1         2026.7.1         2026.7.1         yes
  numpy          2.5.2            2.5.2            2.5.2            yes
```

The interesting part is why that column is all `yes`. From the Dockerfile's own
comment: numpy and tornado are pinned even though the base image ships them,
because its versions were a patch behind the client's — enough for distributed
to raise a `VersionMismatchWarning` on every connect.

**Why it matters.** The escalation ladder from the example is ordered by
nastiness, and the ordering is the lesson:

```text
  1. a warning on connect -- easy to ignore, and people do
  2. an unpickling error deep in a task, surfacing as a confusing traceback
  3. a missing module on the worker: the client imports it fine, the worker cannot
  4. silently different numerics between library versions -- the one that bites hardest
```

A crash is a gift. The bad outcome is (4): two versions that both work and
disagree slightly, so the cluster produces answers that are subtly wrong and
perfectly reproducible on the machine you test on. Case (3) is the one that
wastes the most time in practice, because the client imports the library fine
and the failure arrives as a task error several frames into dask internals.

**What to do instead.** Pin the worker image to the client's locked versions and
bump both together. Install into the worker image every library any submitted
function imports — client-side success proves nothing. Use `get_versions(check=True)`
as a startup assertion in a service. Treat `MIXED` across workers as an
emergency: a heterogeneous cluster produces results that depend on placement.
And in a deployment, build the API image and the worker image from the same
lock file, for exactly the reason this project builds the scheduler and the
workers from the same Dockerfile.

---

## Diagnosing a slow cluster

"The job was slow" becomes actionable when you can say which tasks, on which
worker, in which phase, and how much of the wall time they held. This section is
the procedure, using the tools phase 5 introduces.

### Step 0: capture something

Before theorizing, record. Either wrap the run in a task-stream recorder:

```python
from distributed import get_task_stream

with get_task_stream() as stream:
    result = my_computation.compute()
records = list(stream.data)
```

or write a report you can keep:

```python
from distributed import performance_report

with performance_report(filename="reports/slow-job.html"):
    result = my_computation.compute()
```

Use the report for anything you might have to explain later, and the records for
anything you want to compute a number from or assert on in a test.

### Step 1: parallel efficiency

One number, computed from the records:

```python
compute_seconds = sum(
    float(s["stop"]) - float(s["start"])
    for r in records for s in r.get("startstops", []) if s["action"] == "compute"
)
slots = sum(int(w["nthreads"]) for w in client.scheduler_info()["workers"].values())
efficiency = compute_seconds / (elapsed * slots) * 100
```

This is "what fraction of the available slot-time was real work". Interpret it
in combination with whether workers were busy:

| efficiency | workers busy? | most likely cause | next step |
|---|---|---|---|
| high (>70%) | yes | the cluster is working; the job is genuinely this big | look at the algorithm, or add capacity |
| low | mostly idle | too few tasks, too-serial a graph, or chunks too large | more/smaller chunks, check dependencies |
| low | busy | overhead is winning: transfer, spilling, or tasks too small | step 2 |
| low | job very short | overhead floor dominates; efficiency is meaningless here | make the job bigger or stop distributing it |

That last row is real and worth guarding against. `0501_dashboard_tour` reports
25% efficiency for a 0.36 s job, which is not a cluster problem — it is 164
tasks averaging 3 ms each, against a per-task floor of roughly 8 ms measured in
`0201`.

### Step 2: split the time by phase

Every task-stream record carries `startstops` spans labelled by action. Total
them:

```python
totals: defaultdict[str, float] = defaultdict(float)
for record in records:
    for span in record.get("startstops", []):
        totals[str(span["action"])] += float(span["stop"]) - float(span["start"])
```

A healthy run, from `0501`:

```text
    compute            0.533s  ( 95.6%)
    transfer           0.025s  (  4.4%)
```

How to read the categories:

- **`compute`** — the work you wanted. If this dominates and the job is still
  slow, the answer is in the algorithm or the profile, not in the cluster.
- **`transfer`** — data moving between workers. Significant transfer means data
  is in the wrong place: a locality problem (`0203`), a shuffle-heavy graph, or
  a chunk layout that forces cross-chunk combination.
- **`disk-read` / `disk-write`** — spilling. If these appear at all, workers are
  past the `target` threshold and the fix is memory: smaller chunks, fewer
  concurrent tasks, or persisting less.
- **`deserialize`** — unpacking inputs. Large values here point at oversized
  payloads, which is `0201`'s subject.

The rule of thumb: transfer plus disk exceeding perhaps 20-30% of compute means
you have a data-placement problem, and adding workers will make it worse rather
than better, because the data spreads thinner.

### Step 3: per-worker balance

```python
by_worker: defaultdict[str, float] = defaultdict(float)
for record in records:
    worker = str(record.get("worker", "?"))
    by_worker[worker] += span_seconds(record, "compute")
spread = max(by_worker.values()) - min(by_worker.values())
```

From `0503`, a balanced cluster:

```text
Load per worker:
  172.19.0.3:34041        14 tasks   2.19s ########
  172.19.0.4:41379        15 tasks   2.23s ########
  172.19.0.5:36231        14 tasks   2.18s ########
  busiest minus idlest: 0.05s of compute
```

A spread near zero *eliminates* a whole class of explanations, and elimination
is most of diagnosis. Nobody was starved, nobody was overloaded, so whatever is
wrong is not placement.

When the spread is large, the usual causes are: tasks pinned with `workers=`;
data concentrated on one worker so the scheduler keeps sending work there;
unequal task sizes with an unlucky assignment; or one worker degraded by
spilling or a noisy neighbour. `client.has_what()` and `client.who_has()`
distinguish the data-concentration case from the rest.

A caution from `0501`: a 9 / 78 / 77 split appeared on a 360 ms job in this same
cluster. Very short jobs do not balance, and reading imbalance from one is
reading noise.

### Step 4: stragglers, and the floor

Rank the tasks and compare the longest against the theoretical floor:

```python
ranked = sorted(records, key=lambda r: span_seconds(r, "compute"), reverse=True)
longest = span_seconds(ranked[0], "compute")
floor = total_compute / slots
```

From `0503`:

```text
--- Diagnosis ---
  total compute 6.59s over 6 slots -> a 1.10s floor
  actual wall time 1.54s, parallel efficiency 71%
  the single longest task took 1.50s
  That one task alone exceeds the 1.10s floor: no amount of extra
  workers can finish this batch faster than its slowest single task.
  The fix is splitting that task, not growing the cluster.
```

**`longest > floor` is the single most decisive test in this whole section.**
When it holds, capacity is not the answer — some worker must run that task
start to finish, so the wall time cannot go below it. The fixes are to split the
task (smaller chunks, a finer partition of the work), to make it faster, or to
overlap it with everything else by submitting it first.

When `longest < floor`, the batch really is capacity-bound, and more slots will
help roughly in proportion — which is exactly what `0403_scaling` measures:

```text
  3 worker(s), 6 slots:  1.04s  ########
  2 worker(s), 4 slots:  1.54s  ############
  1 worker(s), 2 slots:  3.06s  ########################
```

### Step 5: the performance report, for what the records do not cover

Some questions need panels rather than numbers. Open the report and read in
this order:

1. **Summary** — the compute/transfer triage, in one place.
2. **Task Stream** — gaps mean starved workers; long single-colour bars mean
   stragglers; a staircase means an accidentally serial graph.
3. **Bandwidth** — one hot worker pair means data is concentrated where it
   should not be.
4. **Worker Profile** — if compute dominates, this says which lines of Python
   burned the CPU. It is the only panel that answers "why is my function slow"
   rather than "where did the time go".
5. **Memory** — rising unmanaged memory across the run is the signature of a
   leak, and the precursor to a worker dying at the `terminate` threshold.

### The quick checklist

When something is slow, in order, cheapest first:

1. Is the job long enough that efficiency means anything? (Under a second: no.)
2. What fraction of slot-time was compute?
3. Is significant time in `transfer` or `disk-*`? — locality or memory problem.
4. Is the load balanced across workers? — if yes, stop blaming placement.
5. Does the longest single task exceed `total_compute / slots`? — if yes, stop
   considering more workers.
6. Are any workers spilling or paused? — check `managed_bytes` against limits.
7. Are there far more tasks than the work justifies? — chunks too small,
   scheduler overhead winning.
8. Only then: add capacity.

---

## Pitfalls and gotchas

Consolidated, with the evidence. Several of these appear in the per-example
sections; they are gathered here because they are the ones that recur.

### Sending big payloads as arguments

The bug looks innocent:

```python
futures = [client.submit(process, big_dataframe, i) for i in range(100)]
```

`big_dataframe` is serialized on every `submit`. `0202_scatter_gather` measures
the 12-task version at 400 ms versus 82 ms for the scattered equivalent, on an
array of only 5.1 MB.

The fixes, in order of preference:

1. **Do not have the data on the client.** Let workers load it from storage, and
   pass a URL. This is the architecture a service wants.
2. **Generate it on the worker.** `0201` shows building an array from `(rows, cols, seed)`
   at 20 ms versus 77 ms for shipping it.
3. **`client.scatter(obj)` once, pass the future.** Right when the client
   genuinely holds data many tasks need.

The variants that hide the same bug:

- **Accidental closure capture.** cloudpickle will happily serialize a function
  that closes over a 500 MB object, and ship it with every task. You never wrote
  it as an argument, so it does not look like one.
- **Data embedded in a graph.** Building an array in client memory and chunking
  it puts the bytes in the graph. `0303` triggers
  `UserWarning: Sending large graph of size 182.54 MiB` for exactly this. Treat
  that warning as a bug report.
- **A big default argument** on a function you submit repeatedly.

### Worker-local state does not survive anything

This is the sharpest gotcha in the project, and `0402_errors_and_retries` hits
it head-on. Its source comment is the clearest statement of the mechanism:

> State has to live on the Worker OBJECT, not in a module global. A function
> defined in `__main__` is pickled by value together with its globals, so each
> task execution unpickles a fresh namespace and a module-level counter would
> read zero forever.

Three separate facts stack here, and each one alone would be enough to break the
naive version:

**1. Functions from `__main__` are pickled by value.** cloudpickle cannot ask
the worker to `import __main__`, so it serializes the function together with a
copy of the globals it references. Each execution unpickles a fresh namespace,
so a module-level `_ATTEMPTS` dict starts empty every time. A visible artifact
of this same mechanism appears in the worker logs, where the traceback reports a
client-side path that does not exist in the container:

```text
worker-1  | Traceback: '  File "/Users/morteoh/.../dask-distributed/examples/0402_errors_and_retries.py", line 38, in always_fails\n'
```

(The user-specific middle of the path is elided. Everything else is verbatim.)

**2. Genuinely worker-local state lives on the `Worker` object.**

```python
from distributed import get_worker

worker = get_worker()
existing = getattr(worker, "_ocs_stack_attempts", None)
if existing is None:
    existing = {}
    setattr(worker, "_ocs_stack_attempts", existing)
```

That object persists for the life of the worker process, which makes it right
for caches, connection pools, and anything expensive to build once. Note the
`getattr`/`setattr` form: `Worker` declares no such attribute, so plain
attribute syntax upsets strict type checkers.

**3. It still dies with the process, and retries move.** A retry may land on a
different worker whose state is empty, which the container logs show plainly —
the same key erroring on two containers. `0402` pins its flaky task to one
worker for exactly this reason. And a nanny restart wipes everything the worker
held anyway.

The rule: **tasks should be stateless, or their state should be
externalized.** If you need per-worker state, use `get_worker()`, treat it as a
cache rather than a source of truth, and make the task correct when the cache is
empty.

If the state you want is a module import rather than mutable data, the right
tool is a different one: put the function in an installed module (not
`__main__`), so workers import it by reference, or register a
`WorkerPlugin`/`preload` script that sets things up at worker startup.

### Pinning tasks with `workers=`

`workers=[address]` forces placement. It is the right tool for correctness —
a worker with a GPU, a licensed library, a mounted disk, credentials — and
almost always the wrong tool for performance.

`0203_locality` measures the cost: eight chunks summed on one pinned worker took
38 ms, against 31 ms for all sixteen chunks summed where they lived. Most of the
pinned chunks had to be shipped first. Same arithmetic, extra network, no
warning.

Three further hazards:

- **Worker addresses identify processes, not machines.** `0401` shows
  `172.19.0.3:36739` becoming `172.19.0.3:34041` after a nanny restart. A pin
  held across a restart names something that no longer exists.
- **A pin is a hard constraint by default.** If the pinned worker dies, the task
  does not move. `allow_other_workers=True` turns the pin into a preference,
  which is usually what people actually mean.
- **Pinning is sometimes still correct.** `0402` pins deliberately, so that
  retries stay in the process holding the attempt counter, and `0403` pins to
  simulate a smaller cluster without disturbing a shared one. Both are
  correctness uses, and both say so.

For the "some workers can do this, others cannot" case, prefer
[worker resources](https://distributed.dask.org/en/stable/resources.html) over
hardcoded addresses: they survive restarts, because they describe capability
rather than identity.

### Scattered data is unrecoverable

`client.scatter` puts data on workers with **no recipe to rebuild it**. This is
the trade for the performance win, and it appears three times in the examples.

From `0401_worker_failure`, on what recovery does not cover:

```text
  - scattered data: client.scatter puts data on workers with no recipe to
    rebuild it, so losing that worker loses the data for good
```

From `0403_scaling`, on why retirement differs from killing:

```text
  - data with no recipe (anything scattered) is preserved by the drain,
    which is exactly what killing the worker would have destroyed
```

The contrast is exact. A chunk of a dask array is a *recipe* the scheduler can
re-run on any worker. A scattered object is a *value* that exists only where it
was put. Kill that worker and the value is gone; futures depending on it move to
`cancelled` rather than recomputing, because there is nothing to recompute from.

Practical consequences:

- Keep a way to rebuild scattered data — the file it came from, the query that
  produced it.
- Prefer `broadcast=True` when the data is small and important; several copies
  is redundancy.
- `retire_workers()` rather than a hard stop when scaling down, because
  retirement drains scattered data to survivors and a kill destroys it.
- Do not scatter something you could have had the workers load. The best
  recovery story is "read it again".

### Retire versus kill

The two ways a worker leaves a cluster are not variations on a theme.

| | kill | retire |
|---|---|---|
| how | `os._exit`, OOM, `docker stop`, `--scale` down, node preemption | `client.retire_workers([address])` |
| recomputable data | lost, recomputed on demand | moved to survivors first |
| scattered data | **lost permanently** | moved to survivors |
| in-flight tasks | rescheduled elsewhere | allowed to finish |
| nanny behaviour | restarts the worker with a new address | does not restart it |
| capacity afterwards | restored automatically | must be added back explicitly |
| when it is right | never — it is what failure looks like | rolling deploys, scale-down, node drain |

The asymmetry people trip over: **a crashed worker comes back and a retired one
does not.** `0403` states it directly, and it means a scale-down driven by
`retire_workers()` needs an explicit plan for restoring capacity — `make scale N=5`
here, a replica count in Kubernetes, or `cluster.adapt()` if something is
watching the queue.

Note also that `docker compose up --scale worker=2` is a *kill*, not a retire.
It stops containers. Anything only those workers held is gone.

### Smaller things worth knowing

- **`Client(...)` sets the process-wide default scheduler.** Every subsequent
  `.compute()` goes to the cluster, including ones inside libraries you did not
  write.
- **`submit` deduplicates by key.** Identical calls return the same future and
  run once. Use `pure=False` for side-effecting, random, or deliberately
  repeated work — `0402` and `0403` both need it, and `0403` would silently
  measure nothing without it.
- **`gather` raises on the first error.** `errors="skip"` returns the successes.
  Choose deliberately, and log what was skipped.
- **`persist()` without a matching `del` is a cluster memory leak.** `0301`
  shows managed memory going 0 -> 392 MB -> 0, and the middle number stays
  forever if the reference does.
- **`scheduler_info()` metrics lag by a heartbeat.** `0301` sleeps 1.0 s and
  1.5 s around its measurements for this reason. Do not build synchronous logic
  on top of them.
- **`.compute()` on something huge tries to materialize it in your client.**
  There is no protective error. Use `persist()` and reduce.
- **A `pause`d worker looks exactly like a hang.** No error, no progress. Check
  the memory bars.
- **`client.run` is your cluster introspection tool.** Paths, environment
  variables, versions, mounts, DNS — one line, per-worker answers, and a partial
  truth (`True, True, False`) is exactly the output you want to see.
- **The first batch is always slower.** Warm up before measuring anything.

---

## How this maps to open-climate-service

[OCS](https://github.com/dhis2/open-climate-service) is a climate data platform:
one instance per country, ingesting from sources like CHIRPS and ERA5, storing
GeoZarr in icechunk, and exposing the results through STAC, Zarr over HTTP, and
openEO. Its planned move to a distributed dask deployment is what this project
is groundwork for, and the mapping is direct.

### openEO graphs on a deployed cluster

An openEO process graph submitted to an OCS instance becomes a dask graph. Today
that graph executes in the API process. On a deployed cluster it executes on
workers that share neither memory nor filesystem with the API process, and every
lesson in this project becomes a deployment constraint:

- The API process becomes a **client**. It builds the graph and holds the
  futures; it does not do the work. Its memory sizing changes accordingly —
  smaller for compute, but it must still be able to hold whatever it gathers.
- **`.compute()` at the end of a user's graph brings the result into the API
  process.** For a time series or a small map that is fine. For anything larger,
  the API should write to storage from the cluster and return a reference,
  because otherwise a user's query is a memory-exhaustion vector against the API.
- **The client is a single point of failure for the graph.** An API restart
  mid-computation loses the graph, though not anything already committed to
  storage. Long-running jobs want a durable checkpoint rather than a long-lived
  in-memory result.
- **Per-request scheduling matters.** Many small user queries against one shared
  cluster is a different regime from one big ingest. Work stealing helps; a user
  submitting a pathological graph that fills worker memory is a real denial-of-
  service surface, and `0301`'s thresholds are what stands between that and a
  dead cluster.

### Why worker images must match the API image

`0102_versions` is the deployment rule in miniature. The API image and the
worker image must be built from the same lock file, and bumped together.

The failure modes, in the order `0102` ranks them: a warning everyone learns to
ignore; an unpickling error deep inside a task; a module the API has and the
workers do not; and — worst — two library versions that both work and disagree
slightly about a numerical result, so the service returns subtly wrong answers
reproducibly.

Two rules follow, and both are cheap:

1. **Every library any submitted function imports must be in the worker image.**
   xarray, zarr, icechunk, and whatever the openEO process implementations
   touch. The API importing them proves nothing.
2. **Pin, and bump both halves together.** This project pins numpy, pandas,
   tornado, msgpack, cloudpickle and toolz in the Dockerfile to the versions in
   `uv.lock`, because the base image alone was a patch behind and warned on
   every connect. An API image and a worker image built from the same lock file
   is the same discipline at deployment scale.

A `get_versions(check=True)` assertion at API startup turns skew into a loud
startup failure, which is the right place for it to be loud.

### Why a shared path is not a shared store

This is the sharpest argument in the project, and it is why the two planned OCS
extensions — S3-backed icechunk and distributed dask — are really one project.

`0302_shared_storage` establishes that worker filesystems are not the client's:
a file the client had just written reported `False` on all three workers, and
passing that path to `to_zarr` would not have errored — it would have written
somewhere useless.

`0303_distributed_xarray` establishes the harder half: a shared volume fixes
worker-to-worker sharing and does **not** fix the client. A dask graph carries
one path string, used by the client that builds it and by every worker that
executes it. The client could not see `/data` at all, so
`xr.open_zarr("/data/source.zarr")` raises `FileNotFoundError` on the client
even though every worker could open it happily.

Only two pipeline shapes work without a store both sides can resolve, and both
give something up:

- **Push the whole job to one worker.** Correct, and it throws away the cluster.
  Measured at 4.69 s on one worker for a pipeline the cluster ran in 1.72 s.
- **Keep the data in the graph.** Fully parallel, and only possible when the data
  is generated rather than read.

The third shape — a lazy graph over a store, computed across the cluster — is
what a service actually needs, and it requires an identifier that resolves
identically on both sides. `s3://bucket/store.zarr` is that identifier: one
string in the graph, each side configuring its own endpoint and credentials.

[Storage](../storage.md) works through the rest of the argument, including the
part that is about correctness rather than reachability: a commit is
compare-and-swap on a branch pointer, object stores provide the conditional
write that needs, and POSIX has no portable equivalent. Its decision table marks
"dask workers on separate machines" and "client builds a lazy graph, workers
execute it" as **required** for object storage, and both of those rows were
written from this project's evidence.

Worth knowing alongside it: icechunk's distributed write model is fork/merge —
the coordinator forks a session per worker, workers write chunks in parallel,
and the coordinator merges and commits once. Many writers, one committer. So the
concurrency hazard for OCS is not the cluster itself but two independent jobs
committing to the same branch.

---

## Where to go next

- **[icechunk](icechunk.md)** — versioned, transactional Zarr v3 storage: the
  layer that turns "a store on a path" into "a repository with commits". Read it
  after this page and the storage argument above will already make sense.
- **[Storage](../storage.md)** — do you actually need S3? The full argument:
  compare-and-swap commits, one committer at a time, and the decision table that
  says exactly when a local filesystem stops being adequate. This project
  supplied two of its rows.
- **[Scaling](../scaling.md)** — the ceilings, layer by layer, and which one you
  are actually hitting. The distributed row is measured with `0403` and `0503`.
- **[API reference](../reference/dask-distributed.md)** — generated documentation
  for `ocs_stack_dask_distributed.cluster`: `connect`, `ClusterSession`,
  `scheduler_reachable`, `wait_for_scheduler`, and `describe_workers`.

Sideways, in the same repository: [dask](dask.md) for the graphs and chunking
this project assumes, [xarray](xarray.md) for the data model underneath both, and
[climate-pipeline](climate-pipeline.md) for the whole shape assembled end to end.

## Further reading

Upstream documentation, curated rather than exhaustive:

**Core**

- [dask.distributed documentation](https://distributed.dask.org/) — the primary
  reference for everything on this page.
- [Deploying dask](https://docs.dask.org/en/stable/deploying.html) — the
  deployment landscape, from `LocalCluster` to Kubernetes to HPC job queues.
- [Dashboard diagnostics](https://docs.dask.org/en/stable/dashboard.html) —
  panel-by-panel guide to what phase 5 reads programmatically.

**Working with the cluster**

- [Managing computation](https://distributed.dask.org/en/stable/manage-computation.html)
  — futures versus collections, `persist` versus `compute`, and how they mix.
- [Managing memory](https://distributed.dask.org/en/stable/memory.html) — what
  the cluster keeps, when it releases it, and how reference counting from the
  client drives it.
- [Worker memory management](https://distributed.dask.org/en/stable/worker-memory.html)
  — target, spill, pause, terminate, and the managed/unmanaged distinction.
- [Efficiency](https://distributed.dask.org/en/stable/efficiency.html) — the
  short version of "do not send data you could have loaded".
- [Dask best practices](https://docs.dask.org/en/stable/best-practices.html) —
  including the load-data-with-dask section the large-graph warning links to.

**Reliability and operations**

- [Resilience](https://distributed.dask.org/en/stable/resilience.html) — what
  survives a worker death, what does not, and why.
- [Worker resources](https://distributed.dask.org/en/stable/resources.html) —
  the capability-based alternative to hardcoding addresses in `workers=`.
- [Adaptive deployments](https://distributed.dask.org/en/stable/http_services.html)
  and `cluster.adapt()` — letting the scheduler ask for capacity based on the
  queue.
- [Docker images for dask](https://docs.dask.org/en/stable/deploying-docker.html)
  — the `ghcr.io/dask/dask` images this project builds on.

**Related layers**

- [xarray with dask](https://docs.xarray.dev/en/stable/user-guide/dask.html) —
  how xarray builds the graphs this cluster executes.
- [Zarr v3](https://zarr-specs.readthedocs.io/) and
  [icechunk](https://icechunk.io/) — the storage layer the next project covers.
- [openEO](https://openeo.org/) — the process-graph API OCS exposes, and the
  source of the graphs a deployed cluster would run.

