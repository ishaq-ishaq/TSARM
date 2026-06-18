#!/usr/bin/env python
"""Example driver: ingest the committed sample snapshots into Parquet.

Run from the project root with the project venv::

    .venv/bin/python scripts/ingest_samples.py

This demonstrates the end-to-end ingestion stage: two N-Triples snapshots
(2023, 2024) are read with the distributed parser, tagged with their snapshot
timestamps, unioned, and written as snapshot-partitioned Parquet under
``data/processed/triples``.
"""

from datetime import datetime
from pathlib import Path

from src.ingestion.ingest import ingest_snapshots, read_parquet, write_parquet
from src.ingestion.spark_session import get_spark

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"
OUT = ROOT / "data" / "processed" / "triples"


def main() -> None:
    spark = get_spark(app_name="TSARM-ingest-samples")
    try:
        snapshots = {
            "2023": (RAW / "sample_2023.nt", datetime(2023, 1, 1)),
            "2024": (RAW / "sample_2024.nt", datetime(2024, 1, 1)),
        }
        df = ingest_snapshots(spark, snapshots)
        print(f"Parsed {df.count()} triples across {len(snapshots)} snapshots.")
        df.show(truncate=60)

        write_parquet(df, OUT)
        print(f"Wrote snapshot-partitioned Parquet to {OUT}")

        reloaded = read_parquet(spark, OUT)
        print("Triple count per snapshot:")
        reloaded.groupBy("snapshot").count().orderBy("snapshot").show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
