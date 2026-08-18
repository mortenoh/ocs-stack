"""Connecting to the Compose cluster, with a local fallback.

Every example opens its cluster through :func:`connect`. When the Compose
cluster is up it returns a client wired to the containers; when it is not, it
falls back to an in-process ``LocalCluster`` and says so. That keeps the
examples runnable on a machine without Docker while still demonstrating the
real thing when the cluster is running -- the repository rule that an example
depending on a server must still run, and explain itself, when unconfigured.

The scheduler address comes from ``DASK_SCHEDULER_ADDRESS`` so the same code
works against a remote cluster without edits.
"""

import os
import socket
import time
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Self

SCHEDULER_ADDRESS = os.environ.get("DASK_SCHEDULER_ADDRESS", "tcp://127.0.0.1:8786")

# Fallback workers are threads in one process, so keep the count small: the
# point of the fallback is that the example still runs, not that it is fast.
FALLBACK_WORKERS = 2


def _split_address(address: str) -> tuple[str, int]:
    """Split a ``tcp://host:port`` address into its host and port.

    Args:
        address: A dask scheduler address, with or without the ``tcp://``
            scheme.

    Returns:
        The host and port.

    Raises:
        ValueError: If the address has no port component.
    """
    hostport = address.rsplit("://", 1)[-1]
    host, _, port = hostport.rpartition(":")
    if not host or not port.isdigit():
        raise ValueError(f"address must look like tcp://host:port, got {address!r}")
    return host, int(port)


def scheduler_reachable(address: str = SCHEDULER_ADDRESS, timeout: float = 1.0) -> bool:
    """Report whether a scheduler is accepting TCP connections at an address.

    A plain socket probe rather than a dask connection: it answers in
    milliseconds when nothing is listening, so examples do not hang waiting on
    a cluster that was never started.

    Args:
        address: Scheduler address to probe.
        timeout: Seconds to wait for the TCP handshake.

    Returns:
        True when something accepts a connection, False otherwise.
    """
    try:
        host, port = _split_address(address)
    except ValueError:
        return False
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def wait_for_scheduler(address: str = SCHEDULER_ADDRESS, timeout: float = 120.0) -> bool:
    """Block until the scheduler accepts connections, for use by ``make up``.

    Args:
        address: Scheduler address to poll.
        timeout: Total seconds to keep trying before giving up.

    Returns:
        True once the scheduler answers.

    Raises:
        TimeoutError: If the scheduler never answers within the timeout.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if scheduler_reachable(address, timeout=1.0):
            print(f"scheduler ready at {address}")
            return True
        time.sleep(1.0)
    raise TimeoutError(f"no scheduler answered at {address} within {timeout:.0f}s (try: make logs)")


@dataclass
class ClusterSession:
    """A connected client plus how it was obtained.

    Attributes:
        client: The connected ``distributed.Client``.
        mode: Either ``"compose"`` (the container cluster) or ``"local"`` (the
            in-process fallback).
        address: The scheduler address actually in use.
    """

    client: Any
    mode: str
    address: str

    @property
    def is_compose(self) -> bool:
        """Whether this session is talking to the Compose cluster."""
        return self.mode == "compose"

    def banner(self) -> str:
        """Return a one-line description of the connection for example output."""
        if self.is_compose:
            return f"Connected to the Compose cluster at {self.address}"
        return (
            f"Compose cluster not reachable at {SCHEDULER_ADDRESS} -- "
            f"fell back to an in-process LocalCluster ({self.address}).\n"
            "  Start the real thing with: make up"
        )

    def close(self) -> None:
        """Close the client and, for the fallback, the cluster it created."""
        cluster = getattr(self.client, "cluster", None)
        self.client.close()
        if not self.is_compose and cluster is not None:
            cluster.close()

    def __enter__(self) -> Self:
        """Return self so the session can be used as a context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close the session on exit."""
        self.close()


def connect(address: str = SCHEDULER_ADDRESS, *, allow_fallback: bool = True) -> ClusterSession:
    """Connect to the Compose cluster, or to a LocalCluster if it is not up.

    Args:
        address: Scheduler address to try first.
        allow_fallback: When False, refuse to start a local cluster and raise
            instead -- used by examples that only make sense against real
            containers.

    Returns:
        A :class:`ClusterSession` wrapping the connected client.

    Raises:
        ConnectionError: If the cluster is unreachable and fallback is off.
    """
    from distributed import Client, LocalCluster

    if scheduler_reachable(address):
        return ClusterSession(client=Client(address), mode="compose", address=address)

    if not allow_fallback:
        raise ConnectionError(f"no scheduler at {address}; start the cluster with: make up")

    cluster = LocalCluster(
        n_workers=FALLBACK_WORKERS,
        threads_per_worker=2,
        processes=False,
        # Bind the fallback dashboard to an ephemeral port. Neither None nor
        # False switches it off -- distributed starts one regardless, tries the
        # default 8787, and on a machine already running the real cluster warns
        # "Port 8787 is already in use" before picking another. ":0" asks for a
        # free port up front, which is quiet and never collides.
        dashboard_address=":0",
    )
    client = Client(cluster)
    return ClusterSession(client=client, mode="local", address=str(cluster.scheduler_address))


def describe_workers(client: Any) -> list[dict[str, Any]]:
    """Summarize the cluster's workers for printing.

    Args:
        client: A connected ``distributed.Client``.

    Returns:
        One dict per worker with its address, thread count, memory limit in
        GiB, and the host it runs on -- in a container cluster the hosts are
        distinct, which is the whole point.
    """
    info = client.scheduler_info()
    workers: list[dict[str, Any]] = []
    for address, meta in sorted(info.get("workers", {}).items()):
        workers.append(
            {
                "address": address,
                "host": meta.get("host", "?"),
                "threads": meta.get("nthreads", 0),
                "memory_limit_gib": round(meta.get("memory_limit", 0) / 2**30, 2),
            }
        )
    return workers
