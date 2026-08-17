"""Streaming ingest: one period at a time, committed as it lands.

This is the heart of the open-climate-service ingestion contract. The source
enumerates periods; the framework fetches each one, normalizes it, appends it
to the store, and commits. Because every period is its own transaction, an
interrupted ingest leaves a store that is complete up to the last commit --
never half a period -- and resuming is a matter of asking the store what it
already holds.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
import xarray as xr

from playground_climate.normalize import normalize
from playground_climate.sources import Period

# The chunk sizes a country-scale daily dataset wants: about a month of time
# per chunk, and spatial chunks capped so a map tile is one or two reads.
TIME_CHUNK = 30
SPATIAL_CHUNK_CAP = 512

Fetcher = Callable[[Period], xr.Dataset]


@dataclass
class IngestReport:
    """What one ingest run did.

    Attributes:
        ingested: Period ids written during this run.
        skipped: Period ids already present and therefore skipped.
        failed: Period ids that raised, with the error message.
        snapshots: Snapshot id produced by each ingested period.
    """

    ingested: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    failed: dict[str, str] = field(default_factory=dict)
    snapshots: dict[str, str] = field(default_factory=dict)

    @property
    def total(self) -> int:
        """Number of periods considered in this run."""
        return len(self.ingested) + len(self.skipped) + len(self.failed)


def chunking_for(ds: xr.Dataset) -> dict[str, int]:
    """Choose chunk sizes for a dataset, capping the spatial dimensions.

    Args:
        ds: The dataset about to be written.

    Returns:
        A chunk mapping suitable for ``Dataset.chunk``.
    """
    chunks = {"time": min(TIME_CHUNK, int(ds.sizes.get("time", 1)))}
    for dim in ("y", "x"):
        if dim in ds.sizes:
            chunks[dim] = min(SPATIAL_CHUNK_CAP, int(ds.sizes[dim]))
    return chunks


def committed_periods(repo: Any, *, period_type: str = "month") -> set[str]:
    """Return the period ids already committed to a store.

    The store's time coordinate is authoritative: whatever is committed is
    what exists, regardless of what any external bookkeeping claims. That is
    what makes resume safe after a crash.

    Args:
        repo: An icechunk repository.
        period_type: Granularity of the period ids; only ``"month"`` is
            currently supported.

    Returns:
        The set of period ids present, empty if the store has no data yet.

    Raises:
        ValueError: If period_type is not supported.
    """
    if period_type != "month":
        raise ValueError(f"unsupported period_type: {period_type!r}")
    try:
        ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    except Exception:
        # A repository with no committed data yet has no group to open.
        return set()
    if "time" not in ds.coords or ds.sizes.get("time", 0) == 0:
        return set()
    stamps = pd.DatetimeIndex(ds["time"].values)
    return {f"{ts.year}-{ts.month:02d}" for ts in stamps}


def ingest_period(repo: Any, period: Period, fetch: Fetcher) -> str:
    """Fetch, normalize, and commit a single period.

    Args:
        repo: An icechunk repository to write into.
        period: The period to ingest.
        fetch: Callable returning the raw source dataset for the period.

    Returns:
        The snapshot id of the commit.
    """
    ds = normalize(fetch(period))
    ds = ds.chunk(chunking_for(ds))

    session = repo.writable_session("main")
    existing = _has_data(session)
    if existing:
        # align_chunks=True is not optional here. Months are 28-31 days and the
        # store's time chunk is 30, so after a few appends the final zarr chunk
        # is partial and the incoming period straddles it. Without alignment
        # xarray refuses the write outright -- "would overlap multiple Dask
        # chunks" -- because a parallel write across a shared chunk can corrupt
        # it. Alignment rechunks the incoming data to the store's boundaries.
        ds.to_zarr(session.store, append_dim="time", consolidated=False, align_chunks=True)
    else:
        ds.to_zarr(session.store, mode="w", zarr_format=3, consolidated=False)
    return str(session.commit(f"ingest {period.period_id}"))


def _has_data(session: Any) -> bool:
    """Report whether a session's store already holds data variables.

    Args:
        session: An icechunk session.

    Returns:
        True when the store has at least one data variable.
    """
    try:
        ds = xr.open_zarr(session.store, consolidated=False)
    except Exception:
        return False
    return bool(ds.data_vars)


def ingest(
    repo: Any,
    periods: list[Period],
    fetch: Fetcher,
    *,
    resume: bool = True,
    stop_after: int | None = None,
) -> IngestReport:
    """Ingest a list of periods, one commit each.

    Args:
        repo: An icechunk repository to write into.
        periods: Periods to ingest, in chronological order.
        fetch: Callable returning the raw source dataset for a period.
        resume: When True, skip periods the store already holds.
        stop_after: Stop once this many periods have been ingested, simulating
            an interrupted run.

    Returns:
        An :class:`IngestReport` describing what happened.
    """
    report = IngestReport()
    present = committed_periods(repo) if resume else set()

    for period in periods:
        if stop_after is not None and len(report.ingested) >= stop_after:
            break
        if period.period_id in present:
            report.skipped.append(period.period_id)
            continue
        try:
            snapshot = ingest_period(repo, period, fetch)
        except Exception as exc:
            report.failed[period.period_id] = f"{type(exc).__name__}: {exc}"
            continue
        report.ingested.append(period.period_id)
        report.snapshots[period.period_id] = snapshot

    return report


def store_path(base: Path | str, dataset_id: str) -> Path:
    """Return the on-disk path for a dataset's store.

    Mirrors the open-climate-service layout,
    ``{data_dir}/downloads/{dataset_id}.icechunk``.

    Args:
        base: The instance's data directory.
        dataset_id: Public identifier of the dataset.

    Returns:
        The store path.
    """
    return Path(base) / "downloads" / f"{dataset_id}.icechunk"
