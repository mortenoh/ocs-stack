"""LocalCluster and Client: a real scheduler with workers and a dashboard.

What: starts a distributed.LocalCluster (in-process, 2 workers, 1 thread each)
inside context managers, shows that Client(cluster) becomes dask's default
scheduler, runs a computation on it, and prints worker info from
client.scheduler_info() plus the dashboard link.

Why: open-climate-service executes openEO process graphs on dask through a
distributed Client; knowing what a cluster is made of and what the dashboard
shows is how you debug memory blowups and stuck tasks in production.

Run: make run EXAMPLE=0302_local_cluster
"""

from typing import Any, cast

from dask.base import get_scheduler
from distributed import Client, LocalCluster

from playground_dask import chunk_report, random_field, task_count


def start_cluster() -> LocalCluster:
    """Start a small in-process LocalCluster, falling back to no dashboard.

    Returns:
        A running LocalCluster with 2 single-threaded in-process workers,
        with a dashboard on a free 127.0.0.1 port when one can be bound.
    """
    settings: dict[str, Any] = {"processes": False, "n_workers": 2, "threads_per_worker": 1}
    try:
        return LocalCluster(dashboard_address="127.0.0.1:0", **settings)
    except Exception:
        print("note: could not bind a dashboard port; continuing with dashboard_address=None")
        no_dashboard: Any = None  # runtime accepts None to disable the dashboard; the stub says str only
        return LocalCluster(dashboard_address=no_dashboard, **settings)


def main() -> None:
    """Run a computation on a LocalCluster and inspect its workers."""
    # SECTION: starting a cluster and connecting a client
    print("LocalCluster starts a scheduler plus workers on this machine; Client connects to it.")
    print("processes=False keeps workers as threads in this process: no pickling, easy debugging.")
    with start_cluster() as entered, Client(entered) as client:
        cluster = cast(LocalCluster, entered)  # sync __enter__ returns the cluster; the stub offers a coroutine too
        print(f"  cluster: {cluster.scheduler_address} with {len(cluster.workers)} workers")

        # SECTION: the dashboard
        print("\nThe dashboard is a live bokeh web app served by the scheduler:")
        try:
            print(f"  dashboard link: {client.dashboard_link}")
        except Exception:
            print("  dashboard link: unavailable (no dashboard port bound)")
        print("  it shows the task stream (which worker ran what, when), progress bars per")
        print("  task name, per-worker memory/CPU, and the task graph -- open it during a")
        print("  long compute to watch tasks flow; port 0 above means 'pick any free port'.")

        # SECTION: the client is now the default scheduler
        print("\nCreating a Client makes it dask's default scheduler -- no scheduler= needed:")
        scheduler_fn = get_scheduler()
        owner = getattr(scheduler_fn, "__self__", None)
        print(f"  dask.base.get_scheduler() is bound to: {type(owner).__name__ if owner else scheduler_fn!r}")

        field = random_field(days=120, ny=256, nx=256)
        graph = field.mean()
        print(f"  data:  {chunk_report(field)}")
        print(f"  graph: mean() = {task_count(graph)} tasks, sent to the scheduler by plain .compute()")
        result = float(graph.compute())
        print(f"  result: mean = {result:.6f} (uniform [0, 1) draws, so ~0.5)")

        # SECTION: what the scheduler knows about its workers
        print("\nclient.scheduler_info() -- the scheduler's live view of its workers:")
        info: dict[str, Any] = client.scheduler_info()  # narrowly-typed Any values; stubs give plain dict
        workers: dict[str, Any] = info["workers"]
        print(f"  workers: {len(workers)}")
        for address, meta in sorted(workers.items()):
            memory_gb = meta["memory_limit"] / 1e9
            print(f"    {meta['name']!s:>3} at {address}: nthreads={meta['nthreads']}, memory_limit={memory_gb:.1f} GB")

    # SECTION: clean shutdown
    print("\nLeaving the with-blocks closed the client and cluster; the process can exit cleanly.")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- LocalCluster = scheduler + workers on one machine; Client is your handle to it")
    print("- processes=False runs workers as threads in this process (great for tests/debugging)")
    print("- a live Client becomes the default scheduler: .compute() just uses it")
    print("- the dashboard (task stream, progress, worker memory) is the first debugging tool")
    print("- scheduler_info() exposes workers, threads, and memory limits programmatically")
    print("- always close cluster and client (context managers) so the process exits")


if __name__ == "__main__":
    main()
