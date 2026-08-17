"""The pandas boundary: when dask.dataframe helps and when it only adds overhead.

What: times the same 100k-row groupby in pandas and in dask.dataframe to expose
the small-data anti-pattern, then shows the right division of labour -- lazy
heavy reduction, .compute() down to a small pandas result, finish in pandas --
including a parquet round-trip that reads back only the needed columns.

Why: open-climate-service uses dask-geopandas for vector aggregation on top of
dataframe partitioning, but its final per-region statistics are small pandas
tables -- knowing where to cross the boundary keeps OCS pipelines fast.

Run: make run EXAMPLE=0403_pandas_boundary
"""

import pathlib
import tempfile
import time
from collections.abc import Callable

import dask.dataframe as dd
import numpy as np
import pandas as pd

N_ROWS = 100_000
N_STATIONS = 25


def build_observations(n_rows: int = N_ROWS, seed: int = 42) -> pd.DataFrame:
    """Return a seeded synthetic station-observations table.

    Args:
        n_rows: Number of observation rows to generate.
        seed: Seed for the random generator.

    Returns:
        A pandas DataFrame with columns station_id, date, temperature, rainfall.
    """
    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        {
            "station_id": rng.integers(0, N_STATIONS, n_rows),
            "date": pd.Timestamp("2024-01-01") + pd.to_timedelta(rng.integers(0, 366, n_rows), unit="D"),
            "temperature": rng.normal(10.0, 8.0, n_rows).round(1),
            "rainfall": rng.gamma(0.6, 4.0, n_rows).round(2),
        }
    )


def best_of(fn: Callable[[], object], repeats: int = 5) -> float:
    """Return the best wall-clock time of several runs of a callable.

    Args:
        fn: Zero-argument callable to time.
        repeats: Number of timed runs; the minimum is returned.

    Returns:
        The fastest observed duration in seconds.
    """
    times: list[float] = []
    for _ in range(repeats):
        start = time.perf_counter()
        fn()
        times.append(time.perf_counter() - start)
    return min(times)


def main() -> None:
    """Time pandas vs dask on small data, then show the right boundary."""
    # SECTION: the anti-pattern -- dask on data that fits in memory
    print("Anti-pattern: 100k rows fit comfortably in memory, but we wrap them anyway.")
    df = build_observations()
    ddf: dd.DataFrame = dd.from_pandas(df, npartitions=4)
    mem_mb = df.memory_usage(deep=True).sum() / 1e6
    print(f"  in-memory size: ~{mem_mb:.1f} MB -- pandas territory")

    t_pandas = best_of(lambda: df.groupby("station_id")[["temperature", "rainfall"]].mean())
    t_dask = best_of(lambda: ddf.groupby("station_id")[["temperature", "rainfall"]].mean().compute())
    ratio = t_dask / t_pandas
    print(f"  same groupby-mean:  pandas {t_pandas * 1000:6.1f} ms")
    print(f"                      dask   {t_dask * 1000:6.1f} ms  (~{ratio:.0f}x slower)")
    print("  Graph construction, scheduling, and result concatenation are pure")
    print("  overhead here -- there is no out-of-core or parallel win to buy.")

    # SECTION: the right pattern -- reduce lazily, then drop to pandas
    print("\nRight pattern: keep the HEAVY reduction lazy, then .compute() once")
    print("the result is small, and finish the analysis in plain pandas:")
    monthly = ddf.assign(month=ddf["date"].dt.month).groupby(["station_id", "month"])["rainfall"].sum()
    print(f"  lazy: {type(monthly).__module__.split('.')[0]}.{type(monthly).__name__} -- nothing has run yet")
    summary = monthly.compute()  # the boundary crossing: dask -> pandas
    print(
        f"  .compute() -> {type(summary).__module__.split('.')[0]}.{type(summary).__name__} with {len(summary)} rows "
        f"({N_ROWS} rows reduced to {N_STATIONS} stations x 12 months)"
    )
    wettest = summary.groupby("station_id").max().nlargest(3)
    print("  final analysis is trivial pandas -- wettest station-months (mm):")
    print("    " + wettest.to_string().replace("\n", "\n    "))

    # SECTION: parquet round-trip with column pruning
    print("\nWhere dask.dataframe earns its keep is on-disk data -- and parquet")
    print("lets it read only the columns a query needs:")
    with tempfile.TemporaryDirectory() as tmp:
        path = pathlib.Path(tmp) / "observations.parquet"
        ddf.to_parquet(path)
        parts = sorted(p.name for p in path.iterdir())
        print(f"  to_parquet wrote one file per partition: {parts}")

        subset: dd.DataFrame = dd.read_parquet(path, columns=["station_id", "rainfall"])
        print(f"  read_parquet(columns=['station_id', 'rainfall']) -> columns {list(subset.columns)}")
        print("  (date and temperature are never read from disk at all)")
        totals = subset.groupby("station_id")["rainfall"].sum().compute()
        print(f"  lazy reduction over the subset -> {len(totals)} station totals,")
        print(f"  e.g. station 0: {totals.loc[0]:.1f} mm across {N_ROWS} source rows")

    # SECTION: summary
    print("\n=== Summary ===")
    print(f"- small data in memory: dask was ~{ratio:.0f}x slower than pandas on the")
    print("  identical groupby -- partitioning overhead with nothing to amortize it")
    print("- use dask when data is bigger than memory or starts on disk/object store")
    print("- the pattern: heavy reductions stay lazy in dask; .compute() only once")
    print("  the result is small; final analysis happens in plain pandas")
    print("- parquet + column selection means dask never even reads unused columns")


if __name__ == "__main__":
    main()
