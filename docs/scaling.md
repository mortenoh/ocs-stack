# Scaling: the ceilings

Every layer in [the stack](stack.md) buys headroom by a different mechanism,
and each one stops for a different reason. Knowing which ceiling you are
actually hitting is most of the work — reaching for the wrong lever is how
teams end up with a bigger cluster and the same wall time.

| Layer | Scales by | Ceiling | What you reach for next |
|---|---|---|---|
| numpy | nothing; it is the baseline | one array must fit in RAM | chunking |
| xarray + dask | splitting into chunks, computing chunk by chunk | one machine's cores and memory bandwidth | more machines |
| dask distributed | adding worker processes and machines | data movement, and the slowest single task | a better graph, not more hardware |
| icechunk | not throughput — correctness under concurrency | commit contention on one branch | separate branches, or fewer committers |

## The row that surprises people

Once work is genuinely distributed, adding hardware stops helping. Two limits
bite, and neither is fixed by more machines.

**Throughput tracks total threads, not worker count.** Three workers with two
threads each is six slots, and that is the number that governs a batch.
`dask-distributed/examples/0403_scaling.py` measures the ladder — the same
batch on 6 slots, then 4, then 2, at roughly 1.05s, 1.56s and 3.08s, very
nearly the inverse of the slot count.

**A batch can never finish faster than its longest single task.**
`dask-distributed/examples/0503_task_stream.py` catches a 1.50s straggler
holding up a batch whose theoretical floor was 1.09s. When that happens the fix
is splitting the task, not growing the cluster.

## Diagnosing rather than guessing

The distributed scheduler exposes everything needed to tell these apart, and
`dask-distributed` phase 5 reads it programmatically rather than by squinting
at the dashboard:

- **Parallel efficiency** — total compute time divided by (wall time x slots).
  Low efficiency with idle workers means the graph is too serial or the chunks
  are too big; low efficiency with busy workers means overhead is winning.
- **Time split by phase** — every task record breaks into `compute`,
  `transfer`, and disk time. A task stream dominated by transfer means data is
  in the wrong place, which is a locality problem, not a capacity problem.
- **Per-worker balance** — one worker doing most of the work is a placement
  problem; all workers equally busy but slow is a chunking problem.

`0501_dashboard_tour.py` prints each dashboard panel's underlying numbers, so
"it looked busy" can be replaced with a figure.

## Memory is a chunking problem

Each worker has a hard memory limit, and distributed escalates through four
stages as it fills: spill to disk at 60%, spill aggressively at 70%, stop
accepting tasks at 80%, and kill the worker at 95%
(`dask-distributed/examples/0301_worker_memory.py` reads these from config and
watches managed memory rise and fall).

The fix for memory pressure is almost never a bigger machine. A worker needs
room for several chunks at once — roughly `limit / threads` — so the lever is
chunk size, and unmanaged memory such as numpy scratch counts too. A persisted
result nobody released is a leak that outlives the computation.
