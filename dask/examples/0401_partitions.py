"""Partitions: a dask DataFrame is a list of pandas DataFrames plus divisions.

What: builds a synthetic station-observations table in pandas, splits it into
4 partitions with dd.from_pandas, and inspects npartitions, divisions, and
individual partitions -- then shows what known vs unknown divisions cost.

Why: open-climate-service uses dask-geopandas for vector aggregation, and
dask-geopandas is partitioned dask.dataframe underneath -- every OCS tabular
aggregation ultimately runs one pandas operation per partition.

Run: make run EXAMPLE=0401_partitions
"""

import dask.dataframe as dd
import numpy as np
import pandas as pd

from climate_stack_dask.helpers import task_count

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
    """Split a pandas table into partitions and inspect divisions."""
    # SECTION: the pandas starting point
    print("One pandas DataFrame: 100k weather observations from 25 stations.")
    df = build_observations()
    print(f"  rows={len(df)}, columns={list(df.columns)}")
    print(f"  index: RangeIndex 0..{df.index[-1]} (sorted, unique)")

    # SECTION: from_pandas splits it into partitions
    print("\ndd.from_pandas(df, npartitions=4) cuts the table along its index:")
    ddf: dd.DataFrame = dd.from_pandas(df, npartitions=4)
    print(f"  .npartitions = {ddf.npartitions}")
    print(f"  .divisions   = {ddf.divisions}")
    print("  divisions are the index boundaries: partition i holds rows with")
    print("  divisions[i] <= index <= divisions[i+1] (last edge inclusive).")

    # SECTION: a partition is a plain pandas DataFrame
    print("\nEach partition is nothing exotic -- it is a pandas DataFrame:")
    part0 = ddf.partitions[0].compute()
    print(f"  type(ddf.partitions[0].compute()) = {type(part0).__name__}")
    print(f"  partition 0: rows {part0.index[0]}..{part0.index[-1]}, len={len(part0)}")
    lengths = ddf.map_partitions(len).compute()
    print(f"  map_partitions(len) -> one number per partition: {lengths.tolist()}")
    print(f"  sum of partition lengths = {int(lengths.sum())} (the full table)")

    # SECTION: known divisions enable index-based pruning
    print("\nKnown divisions let dask route index lookups to ONE partition:")
    print(f"  ddf.known_divisions = {ddf.known_divisions}")
    lookup_known = ddf.loc[60_000]
    print(f"  ddf.loc[60_000] task count = {task_count(lookup_known)}  (only the owning partition is read)")

    # SECTION: unknown divisions force a full scan
    print("\nWithout divisions, dask cannot know which partition holds an index:")
    unknown = ddf.clear_divisions()
    print(f"  .divisions = {unknown.divisions}")
    print(f"  .known_divisions = {unknown.known_divisions}")
    lookup_unknown = unknown.loc[60_000]
    print(f"  same .loc[60_000] task count = {task_count(lookup_unknown)}  (every partition must be checked)")

    # SECTION: a sorted index is what makes divisions possible
    print("\nDivisions only make sense over a SORTED index -- set_index sorts:")
    by_date = ddf.set_index("date")
    edges = [str(d)[:10] for d in by_date.divisions]
    print(f"  set_index('date') -> known_divisions={by_date.known_divisions}")
    print(f"  date divisions: {edges}")
    march = by_date.loc["2024-03-01":"2024-03-31"]
    print(f"  .loc['2024-03-01':'2024-03-31'] computes {len(march.compute())} rows,")
    print("  touching only partitions whose division range overlaps March.")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- a dask DataFrame = N pandas DataFrames (partitions) + divisions metadata")
    print("- .npartitions counts the pieces; each piece is an ordinary pandas DataFrame")
    print("- divisions record the sorted-index boundaries between partitions")
    print(f"- known divisions: .loc hits 1 partition ({task_count(lookup_known)} tasks);")
    print(f"  unknown: all {ddf.npartitions} partitions scanned ({task_count(lookup_unknown)} tasks)")
    print("- a sorted index (e.g. after set_index) is the prerequisite for divisions")


if __name__ == "__main__":
    main()
