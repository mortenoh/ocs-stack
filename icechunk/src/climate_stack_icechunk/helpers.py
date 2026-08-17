"""Shared helpers: repositories, commits, and OCS-shaped test data.

The write/read pair here is the pattern open-climate-service uses for every
ingest: open a writable session on a branch, hand ``session.store`` to xarray
as if it were any other zarr store, then commit. Nothing is visible to readers
until that commit lands.
"""

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import xarray as xr


def quiet_icechunk_logs() -> None:
    """Silence icechunk's Rust-layer INFO and WARN output.

    Local filesystem storage prints two warnings on every repository open --
    about concurrent-commit safety and conditional PUT metadata -- which are
    correct advice for production object stores and pure noise in a lesson.
    Examples call this once at startup so their output stays readable.
    """
    import icechunk

    icechunk.set_logs_filter("error")


def open_repo(path: Path | str) -> Any:
    """Open an icechunk repository, creating it if it does not exist.

    Mirrors ``open_or_create_repo`` in open-climate-service.

    Args:
        path: Directory for the repository, conventionally ending ``.icechunk``.

    Returns:
        An ``icechunk.Repository``.
    """
    import icechunk

    path = Path(path)
    storage = icechunk.local_filesystem_storage(str(path))
    if path.exists():
        return icechunk.Repository.open(storage)
    path.parent.mkdir(parents=True, exist_ok=True)
    return icechunk.Repository.create(storage)


def write_dataset(
    repo: Any, ds: xr.Dataset, message: str, *, branch: str = "main", append_dim: str | None = None
) -> str:
    """Write a dataset to a branch and commit it as one transaction.

    Args:
        repo: The repository to write into.
        ds: The dataset to store.
        message: Commit message, which becomes part of the permanent history.
        branch: Branch to write on.
        append_dim: When given, append along this dimension instead of
            replacing the store's contents.

    Returns:
        The new snapshot id.
    """
    session = repo.writable_session(branch)
    if append_dim is None:
        ds.to_zarr(session.store, mode="w", zarr_format=3, consolidated=False)
    else:
        ds.to_zarr(session.store, append_dim=append_dim, consolidated=False)
    return str(session.commit(message))


def read_dataset(
    repo: Any, *, branch: str | None = None, snapshot_id: str | None = None, tag: str | None = None
) -> xr.Dataset:
    """Open a dataset from a branch tip, a tag, or a specific snapshot.

    At most one selector may be given; with none, the read is of ``main``.
    Silently preferring one selector over another would make a typo look like a
    working read of the wrong point in history, which is the failure mode
    version control exists to prevent -- so a conflict raises instead.

    Args:
        repo: The repository to read from. Defaults to the ``main`` tip.
        branch: Branch whose tip to read.
        snapshot_id: A specific snapshot to read.
        tag: A tag to read.

    Returns:
        The dataset as of that point in history.

    Raises:
        ValueError: If more than one of branch, snapshot_id, and tag is given.
    """
    selectors = {"branch": branch, "snapshot_id": snapshot_id, "tag": tag}
    given = sorted(name for name, value in selectors.items() if value is not None)
    if len(given) > 1:
        raise ValueError(f"give at most one of branch, snapshot_id, tag; got {', '.join(given)}")

    if snapshot_id is not None:
        session = repo.readonly_session(snapshot_id=snapshot_id)
    elif tag is not None:
        session = repo.readonly_session(tag=tag)
    else:
        session = repo.readonly_session(branch or "main")
    ds: xr.Dataset = xr.open_zarr(session.store, consolidated=False)
    return ds


def describe_history(repo: Any, *, branch: str = "main") -> list[dict[str, Any]]:
    """Summarize a branch's commit history, newest first.

    Args:
        repo: The repository to inspect.
        branch: Branch whose ancestry to walk.

    Returns:
        One dict per snapshot with its id, short id, message, and timestamp.
    """
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


def climate_dataset(
    days: int = 30,
    ny: int = 32,
    nx: int = 32,
    start: str = "2024-01-01",
    offset: float = 0.0,
    seed: int = 0,
) -> xr.Dataset:
    """Build a small OCS-shaped dataset with dims (time, y, x).

    Args:
        days: Number of daily time steps; must be at least 1.
        ny: Grid height; must be at least 1.
        nx: Grid width; must be at least 1.
        start: First date, as an ISO date string.
        offset: Constant added to every value, to make revisions distinguishable.
        seed: Seed for the noise term.

    Returns:
        A dataset with one variable, ``t2m``, in degrees Celsius.

    Raises:
        ValueError: If days, ny, or nx is less than 1.
    """
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
