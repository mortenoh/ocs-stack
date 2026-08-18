"""Errors and retries: exceptions on a worker, tracebacks on your screen.

What: raises an exception inside a task and inspects how it comes back, then
uses ``retries=`` to survive a flaky task, and shows how one failure affects
the rest of a batch.

Why: a task fails on a machine you are not looking at. dask ships the
exception and its traceback back to the client, which is the only reason
distributed debugging is bearable. Knowing what is retryable -- a flaky
network read, yes; a bug in your code, no -- decides whether retries help or
just waste time.

Run: make run EXAMPLE=0402_errors_and_retries
"""

import socket
import time
from typing import Any

from ocs_stack_dask_distributed import connect

# Module-level so every attempt on the same worker sees the same counter.
_ATTEMPTS: dict[str, int] = {}


def always_fails(value: int) -> int:
    """Raise immediately, to show how an exception travels back.

    Args:
        value: Ignored; present so the failure has an argument.

    Returns:
        Never returns.

    Raises:
        ValueError: Always.
    """
    raise ValueError(f"this task was never going to work (value={value})")


def flaky(key: str, fail_times: int) -> str:
    """Fail the first ``fail_times`` attempts, then succeed.

    Models a transient fault -- a rate-limited API, a network blip -- which is
    the only kind of failure a retry can fix.

    Args:
        key: Identifies the counter to use.
        fail_times: How many attempts should fail before succeeding.

    Returns:
        A message naming the worker and the attempt that finally worked.

    Raises:
        ConnectionError: On the first ``fail_times`` attempts.
    """
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
        # getattr/setattr rather than attribute syntax: the Worker class has no
        # such attribute declared, and stashing state on it is the documented
        # way to keep per-worker state.
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


def works(value: int) -> int:
    """Double a value, as a task that always succeeds.

    Args:
        value: The number to double.

    Returns:
        value * 2.
    """
    return value * 2


def describe_exception(exc: BaseException) -> str:
    """Render an exception as a short one-line description.

    Args:
        exc: The exception to describe.

    Returns:
        Its type and message on one line.
    """
    return f"{type(exc).__name__}: {exc}"


def main() -> None:
    """Show failures propagating, retries working, and batches partially failing."""
    with connect() as session:
        client = session.client
        print(session.banner())

        # SECTION: an exception crosses back
        print("\nA task that raises on a worker:")
        future: Any = client.submit(always_fails, 42)
        print(f"  submit() returned immediately; future.status is now '{future.status}'")
        time.sleep(0.5)
        print(f"  after the task ran, status is '{future.status}'")
        try:
            future.result()
        except Exception as exc:
            print(f"  result() re-raises it here: {describe_exception(exc)}")

        traceback_obj = future.traceback()
        print(f"  future.traceback() gives the WORKER's traceback: {type(traceback_obj).__name__}")
        print("  You debug with the stack from the machine that actually failed.")

        # SECTION: retries fix transient faults
        # The attempt counter lives in the worker's own process memory, and a
        # retry is free to land on a different worker -- where the counter
        # starts at zero again and the task fails forever. Pinning to one
        # worker keeps the retries in the process that is counting them.
        pinned_to = sorted(client.scheduler_info()["workers"])[0]
        print("\nA flaky task that fails twice, then works.")
        print(f"  (pinned to {pinned_to.rsplit('/', 1)[-1]}: the attempt counter is worker-local,")
        print("   and retries can otherwise land on a worker that has never seen it)")
        print("  Without retries:")
        try:
            client.submit(flaky, "no-retry", 2, pure=False, workers=[pinned_to]).result()
        except Exception as exc:
            print(f"    {describe_exception(exc)}")

        print("  With retries=3:")
        outcome = client.submit(flaky, "with-retry", 2, retries=3, pure=False, workers=[pinned_to]).result()
        print(f"    {outcome}")
        print("  dask re-ran the task in place; the client never saw the failures.")

        # SECTION: retries do not fix bugs
        print("\nRetries only help when the fault is transient:")
        try:
            client.submit(always_fails, 7, retries=3, pure=False).result()
        except Exception as exc:
            print(f"  a real bug retried 3 times is still a bug -> {describe_exception(exc)}")
        print("  Three retries here bought nothing but latency.")

        # SECTION: one failure in a batch
        print("\nOne bad task among many does not cancel the good ones:")
        futures: list[Any] = [client.submit(works, i) for i in range(5)]
        futures.append(client.submit(always_fails, 99))
        done = [f for f in futures if f.status == "finished"]
        time.sleep(0.5)
        finished = sum(1 for f in futures if f.status == "finished")
        errored = sum(1 for f in futures if f.status == "error")
        print(f"  {finished} finished, {errored} errored out of {len(futures)}")
        good = client.gather([f for f in futures if f.status == "finished"])
        print(f"  the successful results are still available: {good}")
        print(f"  (done-before-wait was {len(done)}; futures resolve asynchronously)")

        print("\n  gather() on the whole batch raises on the first failure, so pass")
        print("  errors='skip' when you would rather keep the partial results.")
        skipped = client.gather(futures, errors="skip")
        print(f"  gather(errors='skip') -> {skipped}")

        # SECTION: summary
        print("\n=== Summary ===")
        print("- exceptions from workers are re-raised on the client, with the worker's traceback")
        print("- future.status tells you 'pending' / 'finished' / 'error' without blocking")
        print("- retries= re-runs a failed task and only helps for transient faults")
        print("- a failing task does not take its siblings down with it")
        print("- gather(errors='skip') keeps what succeeded instead of raising")


if __name__ == "__main__":
    main()
