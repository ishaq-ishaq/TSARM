#!/usr/bin/env python
"""Run the TSARM vs SANSA vs RDFRules comparison on the sample dataset.

TSARM runs in-process. SANSA and RDFRules run only if their command templates
are configured via ``$SANSA_CMD`` / ``$RDFRULES_CMD`` (see
``src/evaluation/adapters.py``); otherwise they are reported as skipped.

Usage (from the project root)::

    export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
    python3 scripts/run_benchmark.py
"""

import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.evaluation.adapters import (  # noqa: E402
    Dataset,
    Snapshot,
    TSARMBaseline,
    rdfrules_baseline,
    sansa_baseline,
)
from src.evaluation.benchmark import run_benchmark  # noqa: E402
from src.evaluation.report import temporal_addendum, to_csv, to_markdown  # noqa: E402
from src.ingestion.spark_session import get_spark  # noqa: E402

RAW = ROOT / "data" / "raw"
RESULTS = ROOT / "results"

MIN_SUPPORT = 0.5
MIN_CONFIDENCE = 0.5


def sample_dataset() -> Dataset:
    return Dataset(
        name="sample",
        snapshots=[
            Snapshot("2023", RAW / "sample_2023.nt", datetime(2023, 1, 1)),
            Snapshot("2024", RAW / "sample_2024.nt", datetime(2024, 1, 1)),
        ],
    )


def main() -> None:
    spark = get_spark(app_name="TSARM-benchmark")
    try:
        baselines = [
            TSARMBaseline(
                spark, min_support=MIN_SUPPORT, min_confidence=MIN_CONFIDENCE
            ),
            sansa_baseline(min_support=MIN_SUPPORT, min_confidence=MIN_CONFIDENCE),
            rdfrules_baseline(min_support=MIN_SUPPORT, min_confidence=MIN_CONFIDENCE),
        ]
        results = run_benchmark(baselines, [sample_dataset()])

        print("\n## Benchmark comparison\n")
        print(to_markdown(results))

        addendum = temporal_addendum(results)
        if addendum:
            print("\n" + addendum)

        RESULTS.mkdir(exist_ok=True)
        csv_path = RESULTS / "benchmark_sample.csv"
        to_csv(results, csv_path)
        print(f"\nWrote {csv_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
