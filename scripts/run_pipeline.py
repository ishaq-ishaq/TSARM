#!/usr/bin/env python
"""End-to-end TSARM pipeline on the committed sample snapshots.

Runs all four stages and prints the temporal rule metrics::

    ingestion -> windowing -> mining -> temporal metrics

Usage (from the project root)::

    export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
    python3 scripts/run_pipeline.py
"""

import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.ingestion.ingest import ingest_snapshots  # noqa: E402
from src.ingestion.spark_session import get_spark  # noqa: E402
from src.metrics.temporal import compute_metrics  # noqa: E402
from src.mining.arm import mine  # noqa: E402
from src.windowing.sliding_window import assign_windows, build_windows, ordered_snapshots  # noqa: E402

RAW = ROOT / "data" / "raw"

# Mining/metric parameters (low thresholds so the tiny sample yields rules).
WIDTH = 1
STEP = 1
MIN_SUPPORT = 0.5
MIN_CONFIDENCE = 0.5
TAU = 0.6


def main() -> None:
    spark = get_spark(app_name="TSARM-pipeline")
    try:
        # 1. Ingestion -------------------------------------------------------
        triples = ingest_snapshots(
            spark,
            {
                "2023": (RAW / "sample_2023.nt", datetime(2023, 1, 1)),
                "2024": (RAW / "sample_2024.nt", datetime(2024, 1, 1)),
            },
        )
        print(f"[1/4] Ingested {triples.count()} triples.")

        # 2. Windowing -------------------------------------------------------
        snaps = ordered_snapshots(triples)
        n_windows = len(build_windows(snaps, width=WIDTH, step=STEP))
        windowed = assign_windows(spark, triples, width=WIDTH, step=STEP)
        print(f"[2/4] Built {n_windows} sliding window(s) over snapshots {snaps}.")

        # 3. Mining ----------------------------------------------------------
        _, rules = mine(
            windowed, min_support=MIN_SUPPORT, min_confidence=MIN_CONFIDENCE
        )
        print(f"[3/4] Mined {rules.count()} per-window rule instance(s).")

        # 4. Temporal metrics ------------------------------------------------
        metrics = compute_metrics(rules, n_windows=n_windows, tau=TAU)
        print(f"[4/4] Temporal metrics for {metrics.count()} distinct rule(s):")
        metrics.select(
            "rule_key",
            "occurrences",
            "temporal_support",
            "mean_confidence",
            "confidence_drift",
            "confidence_volatility",
            "persistence_score",
        ).show(truncate=70, vertical=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
