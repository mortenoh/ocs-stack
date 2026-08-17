"""Groupby and shuffle: which dataframe operations move data between partitions.

What: runs three classes of operations on a partitioned station table --
per-partition column math and filters (no movement), groupby aggregations
(tree-reduce), and sorting/set_index (full shuffle) -- and compares their
task counts to show the cost ladder.

Why: open-climate-service uses dask-geopandas for vector aggregation, which
is dask.dataframe partitioning underneath -- knowing which operations shuffle
is what separates a fast OCS aggregation from a cluster-melting one.

Run: make run EXAMPLE=0402_groupby_shuffle
"""

import dask.dataframe as dd
import numpy as np
import pandas as pd

from playground_data_dask.helpers import task_count

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


def main() -> None:
    """Compare per-partition ops, tree-reduce aggregations, and shuffles."""
    # SECTION: the partitioned table
    print("100k observations from 25 stations, split into 4 partitions.")
    ddf: dd.DataFrame = dd.from_pandas(build_observations(), npartitions=4)
    base_tasks = task_count(ddf)
    print(f"  npartitions={ddf.npartitions}, baseline task count = {base_tasks} (one task per partition)")

    # SECTION: cheap class -- per-partition (embarrassingly parallel) ops
    print("\nClass 1 -- per-partition ops: each partition is processed alone,")
    print("no data ever crosses a partition boundary:")
    with_f = ddf.assign(temp_f=ddf["temperature"] * 9 / 5 + 32)
    warm = ddf[ddf["temperature"] > 25.0]
    print(f"  column arithmetic (assign temp_f): {task_count(with_f)} tasks")
    print(f"  row filter (temperature > 25):     {task_count(warm)} tasks")
    print("  task count grows only linearly with npartitions -- the cheap class.")

    # SECTION: why groupby cannot be purely per-partition
    print("\nEvery partition contains rows from (almost) every station:")
    stations_per_part = ddf.map_partitions(lambda p: p["station_id"].nunique()).compute()
    print(f"  distinct stations per partition: {stations_per_part.tolist()} (of {N_STATIONS})")
    print("  so a per-station mean needs data from ALL partitions combined.")

    # SECTION: aggregations use a tree reduce, not a shuffle
    print("\nClass 2 -- groupby aggregations: partial result per partition,")
    print("then combine. Only tiny partials move, never the raw rows:")
    means = ddf.groupby("station_id")[["temperature", "rainfall"]].mean()
    print(f"  groupby('station_id').mean(): {task_count(means)} tasks")
    result = means.compute()
    print(f"  result is small: {len(result)} rows (one per station), e.g.:")
    print("    " + result.head(2).to_string().replace("\n", "\n    "))

    # SECTION: sorting and set_index require a full shuffle
    print("\nClass 3 -- shuffles: every output partition may need rows from every")
    print("input partition, so ALL raw data is repartitioned and moved:")
    by_temp = ddf.sort_values("temperature")
    by_station = ddf.set_index("station_id")
    print(f"  sort_values('temperature'): {task_count(by_temp)} tasks")
    print(f"  set_index('station_id'):    {task_count(by_station)} tasks")
    print("  (split/transfer/merge tasks between every pair of partitions)")

    # SECTION: the cost ladder in one table
    print("\nTask counts side by side (4 partitions, same data):")
    rows = [
        ("baseline (just the partitions)", base_tasks),
        ("assign column arithmetic", task_count(with_f)),
        ("filter rows", task_count(warm)),
        ("groupby mean (tree reduce)", task_count(means)),
        ("sort_values (shuffle)", task_count(by_temp)),
        ("set_index (shuffle)", task_count(by_station)),
    ]
    for label, count in rows:
        print(f"  {label:<34} {count:>3} tasks")
    print("  On a real cluster the shuffle rows also mean network + disk traffic.")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- per-partition ops (column math, filters) never move data: the cheap class")
    print("- groupby aggregations tree-reduce: per-partition partials + a combine step,")
    print("  moving only small intermediates -- no full shuffle needed")
    print("- sorting and set_index must shuffle: all rows repartitioned across workers")
    print("- shuffles are THE expensive class of distributed operations -- design")
    print("  pipelines to aggregate early and sort/re-index rarely (or never)")


if __name__ == "__main__":
    main()
