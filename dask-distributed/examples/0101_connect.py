"""Connecting to a real cluster: scheduler, workers, and the dashboard.

What: opens a client against the Compose cluster, prints the worker inventory,
and runs one task to prove the round trip works.

Why: a LocalCluster hides everything that makes distributed computing hard,
because its "workers" are threads sharing one memory space. Here each worker
is a separate container with its own IP, its own memory limit, and its own
copy of the libraries. open-climate-service deploys this way: an openEO graph
submitted to an instance runs on workers that share neither memory nor
filesystem with the API process.

Run: make run EXAMPLE=0101_connect
"""

import os

from playground_dask_distributed import connect, describe_workers


def where_am_i(task_id: int) -> str:
    """Report the container and process that executed this task.

    Defined at module level so it can be pickled and shipped to a worker: a
    closure or lambda capturing local state would fail to serialize.

    Args:
        task_id: An identifier echoed back with the location.

    Returns:
        A string naming the task, host, and process id.
    """
    import socket

    return f"task {task_id} ran on {socket.gethostname()} (pid {os.getpid()})"


def main() -> None:
    """Connect to the cluster and describe what is on the other end."""
    with connect() as session:
        client = session.client

        # SECTION: what we are connected to
        print(session.banner())
        print()

        # SECTION: the worker inventory
        workers = describe_workers(client)
        print(f"The scheduler reports {len(workers)} workers:")
        for worker in workers:
            print(
                f"  {worker['address']:<28} host={worker['host']:<14} "
                f"threads={worker['threads']}  memory={worker['memory_limit_gib']} GiB"
            )

        total_threads = sum(w["threads"] for w in workers)
        total_memory = round(sum(w["memory_limit_gib"] for w in workers), 2)
        print(f"\nCluster capacity: {total_threads} threads, {total_memory} GiB across {len(workers)} workers.")
        print(f"That is {total_threads} tasks running at once before work starts queueing.")

        # SECTION: distinct hosts are the whole point
        hosts = {w["host"] for w in workers}
        if session.is_compose:
            print(f"\nEach worker is its own container: {len(hosts)} distinct host addresses.")
            print("Nothing is shared between them -- not memory, not open files, not imported modules.")
        else:
            print(f"\nThe fallback runs {len(workers)} workers as threads in ONE process ({len(hosts)} host).")
            print("They share memory, so transfers are free and the distributed lessons stay invisible.")

        # SECTION: the dashboard
        link = client.dashboard_link
        print(f"\nDashboard: {link if link else '(none: the fallback cluster runs without one)'}")
        if session.is_compose:
            print("Open it while a job runs -- the task stream is the fastest way to see what a cluster is doing.")

        # SECTION: prove the round trip
        print("\nSubmitting one task to confirm the round trip:")
        print(f"  {client.submit(where_am_i, 1).result()}")

        # SECTION: summary
        print("\n=== Summary ===")
        print("- connect() returns a client wired to the scheduler over TCP")
        print("- scheduler_info() is the authoritative inventory: workers, threads, memory")
        print("- in a container cluster every worker is a separate host with its own memory")
        print("- functions sent to workers must be importable at module level, not closures")
        print("- the dashboard link is the first thing to open when a job misbehaves")


if __name__ == "__main__":
    main()
