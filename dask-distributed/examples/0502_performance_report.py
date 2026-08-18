"""Performance reports: a shareable snapshot of what the cluster did.

What: runs a workload inside ``performance_report()``, writing a standalone
HTML file, then reports what landed on disk and what is inside it.

Why: the dashboard is live and disappears when the cluster stops. A
performance report is the same information frozen into one self-contained
file you can attach to an issue, diff against last week, or read after a
nightly job finished at 3am. It is the right artifact to produce when asking
someone else why a job was slow.

Run: make run EXAMPLE=0502_performance_report
"""

import time
from pathlib import Path
from typing import Any

import dask.array as da
from distributed import performance_report

from ocs_stack_dask_distributed import connect

# Written into the project rather than a temp dir on purpose: the point of a
# report is that it outlives the run. `make clean` removes this directory.
REPORTS_DIR = Path(__file__).resolve().parent.parent / "reports"

# The sections bokeh writes into the report, in the order they appear.
SECTIONS = (
    ("Task Stream", "every task as a colored bar, per worker row"),
    ("Bandwidth", "bytes moved between each pair of workers"),
    ("Memory", "managed and unmanaged memory over the run"),
    ("Summary", "totals: compute time, transfer time, task counts"),
    ("Worker Profile", "aggregated stack samples -- which lines burned CPU"),
    ("Scheduler Profile", "the same for the scheduler's own event loop"),
)


def mixed_workload() -> Any:
    """Return a lazy computation with both compute and transfer phases.

    A reduction along one axis forces cross-chunk combination, so the report
    has bandwidth to show rather than pure embarrassing parallelism.

    Returns:
        A lazy dask scalar.
    """
    array = da.random.random((6000, 6000), chunks=(750, 750))
    scaled = (array**2 + array) ** 0.5
    return (scaled.mean(axis=0) ** 2).sum() + scaled.mean(axis=1).max()


def main() -> None:
    """Produce a performance report and describe what is in it."""
    with connect() as session:
        print(session.banner())

        REPORTS_DIR.mkdir(exist_ok=True)
        target = REPORTS_DIR / "performance-report.html"

        # SECTION: capture a run
        print(f"\nRunning a workload inside performance_report(), writing to:\n  {target}")
        started = time.perf_counter()
        with performance_report(filename=str(target)):
            result = float(mixed_workload().compute())
        elapsed = time.perf_counter() - started
        print(f"\n  computed {result:,.2f} in {elapsed:.2f}s")

        # SECTION: what landed on disk
        if not target.exists():
            print("  The report was not written -- performance_report needs bokeh installed")
            print("  on the client. Install it and re-run.")
            return

        size_kb = target.stat().st_size / 1024
        print(f"  wrote {size_kb:.0f} KB of self-contained HTML")
        print("  Self-contained means no CDN and no running cluster: open it anywhere.")
        print(f"\n  open it with:  open {target}")

        # SECTION: what is inside
        text = target.read_text(errors="ignore")
        print("\nSections in the report:")
        for name, description in SECTIONS:
            present = "yes" if name.lower().replace(" ", "") in text.lower().replace(" ", "") else " - "
            print(f"  [{present:>3}] {name:<18} {description}")

        # SECTION: how to use it
        print("\nHow to read one, in order:")
        print("  1. Summary first -- is the time going to compute, or to transfer?")
        print("  2. Task Stream -- are there gaps (starved workers) or long red bars?")
        print("  3. Bandwidth -- is one worker pair moving most of the data?")
        print("  4. Worker Profile -- if compute dominates, which lines are hot?")

        print("\nThe habit worth forming: capture a report for any job you might have to")
        print("explain later. It costs nothing to record and answers questions that are")
        print("impossible to reconstruct once the cluster is gone.")

        if not session.is_compose:
            print("\nNote: this ran on the in-process fallback, so the bandwidth and worker")
            print("panels are close to empty. Run 'make up' for a report with real transfers.")

        # SECTION: summary
        print("\n=== Summary ===")
        print("- performance_report(filename=...) captures a whole computation to HTML")
        print("- the file is self-contained: no cluster and no network needed to read it")
        print("- it holds the task stream, bandwidth, memory, and profile panels")
        print("- it is the right thing to attach when asking why a job was slow")
        print("- 'make clean' removes the reports directory")


if __name__ == "__main__":
    main()
