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
from src.metrics.export import export_rules  # noqa: E402
from src.mining.arm import mine_temporal  # noqa: E402
from src.windowing.sliding_window import assign_windows, build_windows, ordered_snapshots  # noqa: E402

RAW = ROOT / "data" / "raw"

# Mining/metric parameters (low thresholds so the tiny sample yields rules).
WIDTH = 1
STEP = 1
MIN_SUPPORT = 0.5
MIN_CONFIDENCE = 0.5
TAU = 0.70  # proposal Sec. 3.5.5 default


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
        _, rules, candidates = mine_temporal(
            windowed,
            range(n_windows),
            min_support=MIN_SUPPORT,
            min_confidence=MIN_CONFIDENCE,
        )
        print(f"[3/4] Mined {rules.count()} per-window rule instance(s).")

        # 4. Temporal metrics ------------------------------------------------
        metrics = compute_metrics(candidates, n_windows=n_windows, tau=TAU)
        print(f"[4/4] Temporal metrics for {metrics.count()} distinct rule(s):")
        metrics.select(
            "rule_key",
            "lifecycle",
            "temporal_support",
            "confidence_drift",
            "persistence_score",
            "confidence_series",
            "occurrences",
        ).show(truncate=70, vertical=True)
        json_path, csv_path = export_rules(
            metrics,
            ROOT / "results",
            basename="sample_temporal_rules",
            params={
                "width": WIDTH,
                "step": STEP,
                "min_support": MIN_SUPPORT,
                "min_confidence": MIN_CONFIDENCE,
                "tau": TAU,
            },
        )
        print(f"Wrote {json_path} and {csv_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
