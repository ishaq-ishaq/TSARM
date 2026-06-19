#!/usr/bin/env python
"""Run the TSARM vs SANSA vs RDFRules comparison on the sample dataset.

TSARM runs in-process. SANSA and RDFRules run only if their command templates
are configured via ``$SANSA_CMD`` / ``$RDFRULES_CMD`` (see
``src/evaluation/adapters.py``); otherwise they are reported as skipped.

Usage (from the project root)::

    export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
    python3 scripts/run_benchmark.py
"""

import argparse
import sys
import tempfile
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
from src.evaluation.synthetic import generate_dataset  # noqa: E402
from src.ingestion.spark_session import get_spark  # noqa: E402

RAW = ROOT / "data" / "raw"
RESULTS = ROOT / "results"

# Default thresholds (overridable via CLI). Lower for synthetic so both miners
# find the planted patterns; the tiny sample needs higher support.
DEFAULTS = {"sample": (0.5, 0.5), "synthetic": (0.05, 0.3)}


def sample_dataset() -> Dataset:
    return Dataset(
        name="sample",
        snapshots=[
            Snapshot("2023", RAW / "sample_2023.nt", datetime(2023, 1, 1)),
            Snapshot("2024", RAW / "sample_2024.nt", datetime(2024, 1, 1)),
        ],
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--synthetic",
        type=int,
        metavar="N_ENTITIES",
        help="benchmark a generated synthetic dataset of this many entities "
        "instead of the tiny committed sample",
    )
    ap.add_argument("--min-support", type=float)
    ap.add_argument("--min-confidence", type=float)
    args = ap.parse_args()

    mode = "synthetic" if args.synthetic else "sample"
    min_support = args.min_support or DEFAULTS[mode][0]
    min_confidence = args.min_confidence or DEFAULTS[mode][1]

    tmp_ctx = tempfile.TemporaryDirectory(prefix="tsarm_bench_") if args.synthetic else None
    spark = get_spark(app_name="TSARM-benchmark")
    try:
        if args.synthetic:
            dataset, n = generate_dataset(
                Path(tmp_ctx.name),
                name=f"synthetic-{args.synthetic}",
                n_entities=args.synthetic,
                n_snapshots=4,
            )
            print(f"Generated synthetic dataset: {n} triples, "
                  f"{len(dataset.snapshots)} snapshots.")
        else:
            dataset = sample_dataset()

        baselines = [
            TSARMBaseline(spark, min_support=min_support, min_confidence=min_confidence),
            sansa_baseline(min_support=min_support, min_confidence=min_confidence),
            rdfrules_baseline(min_support=min_support, min_confidence=min_confidence),
        ]
        results = run_benchmark(baselines, [dataset])

        print("\n## Benchmark comparison\n")
        print(to_markdown(results))

        addendum = temporal_addendum(results)
        if addendum:
            print("\n" + addendum)

        RESULTS.mkdir(exist_ok=True)
        csv_path = RESULTS / f"benchmark_{mode}.csv"
        to_csv(results, csv_path)
        print(f"\nWrote {csv_path}")
    finally:
        spark.stop()
        if tmp_ctx is not None:
            tmp_ctx.cleanup()


if __name__ == "__main__":
    main()
