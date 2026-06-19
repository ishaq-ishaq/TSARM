#!/usr/bin/env python
"""Multi-window temporal analysis on real DBpedia releases (objectives ii + iv).

Uses three DBpedia ``instance_types_en`` releases (2015-10, 2016-04, 2016-10) as
temporal snapshots. With sliding windows of width 2 (step 1) this yields two
overlapping windows::

    w0 = {2015-10, 2016-04}
    w1 = {2016-04, 2016-10}

so each type-evolution rule has a confidence in each window -- a real temporal
trajectory over which TSARM's temporal metrics (confidence drift, rule
persistence) are computed. This is the multi-window temporal result that the
two-snapshot run (``run_dbpedia.py``) cannot produce.

Usage (from the project root)::

    export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
    python3 scripts/run_dbpedia_temporal.py
"""

import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pyspark.sql import functions as F  # noqa: E402

from src.ingestion.ingest import ingest_snapshots  # noqa: E402
from src.ingestion.spark_session import get_spark  # noqa: E402
from src.metrics.temporal import compute_metrics  # noqa: E402
from src.mining.arm import mine  # noqa: E402
from src.windowing.sliding_window import (  # noqa: E402
    assign_windows,
    build_windows,
    ordered_snapshots,
)

RAW = ROOT / "data" / "raw"
SNAPSHOTS = {
    "2015-10": (RAW / "dbpedia_2015-10_instance_types_en.ttl.bz2", datetime(2015, 10, 1)),
    "2016-04": (RAW / "dbpedia_2016-04_instance_types_en.ttl.bz2", datetime(2016, 4, 1)),
    "2016-10": (RAW / "dbpedia_2016-10_instance_types_en.ttl.bz2", datetime(2016, 10, 1)),
}

WIDTH = 2
STEP = 1
MIN_SUPPORT = 0.005
MIN_CONFIDENCE = 0.2
TAU = 0.2


def _short(item: str) -> str:
    return item.split("/")[-1]


def main() -> None:
    for _, (path, _ts) in SNAPSHOTS.items():
        if not path.exists():
            sys.exit(f"Missing snapshot {path}. Download the DBpedia dumps first.")

    spark = get_spark(
        app_name="TSARM-dbpedia-temporal",
        config={"spark.sql.shuffle.partitions": "32"},
    )
    try:
        start = time.perf_counter()
        triples = ingest_snapshots(spark, SNAPSHOTS).cache()
        n_triples = triples.count()
        print(f"Ingested {n_triples:,} triples from {len(SNAPSHOTS)} DBpedia releases.")
        triples.groupBy("snapshot").count().orderBy("snapshot").show()

        snaps = ordered_snapshots(triples)
        windows = build_windows(snaps, width=WIDTH, step=STEP)
        n_windows = len(windows)
        print(f"Sliding windows (width={WIDTH}, step={STEP}):")
        for w in windows:
            print(f"  w{w.window_id} = {w.snapshots}")

        windowed = assign_windows(spark, triples, width=WIDTH, step=STEP)
        _, rules = mine(windowed, min_support=MIN_SUPPORT, min_confidence=MIN_CONFIDENCE)
        metrics = compute_metrics(rules, n_windows=n_windows, tau=TAU).cache()

        n_rules = metrics.count()
        print(f"\nMined {n_rules} distinct type-evolution rule(s) across windows.")

        print("\nTemporal profile per rule (persistence over the 2 windows):")
        rows = metrics.orderBy(
            F.col("persistence_score").desc(), F.col("mean_confidence").desc()
        ).collect()
        for r in rows:
            ante = ", ".join(_short(x) for x in r["antecedent"])
            cons = ", ".join(_short(x) for x in r["consequent"])
            present_in = ", ".join(f"w{w}" for w in r["window_series"])
            conf_series = ", ".join(f"{c:.3f}" for c in r["confidence_series"])
            kind = "PERSISTENT" if r["occurrences"] > 1 else "TRANSIENT"
            print(
                f"  [{ante}] => [{cons}]\n"
                f"      {kind}: present in [{present_in}] of {n_windows} windows | "
                f"confidence [{conf_series}] | "
                f"drift={r['confidence_drift']:+.3f} | "
                f"persistence={r['persistence_score']:.3f}"
            )

        print(f"\nTotal wall time: {time.perf_counter() - start:.1f}s")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
