#!/usr/bin/env python
"""Scalability sweep for TSARM (research objective iv: computational scalability).

Generates synthetic temporal-RDF datasets at increasing sizes, runs the full
TSARM pipeline on each, and records runtime + throughput. Also verifies temporal
sensitivity: the injected *persistent* rule should score high persistence while
the *transient* rule decays.

Usage (from the project root)::

    export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
    python3 scripts/scale_test.py                 # default sweep
    python3 scripts/scale_test.py 1000 5000 20000 # custom entity counts
"""

import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pyspark.sql import functions as F  # noqa: E402

from src.evaluation.adapters import TSARMBaseline  # noqa: E402
from src.evaluation.report import to_csv, to_markdown  # noqa: E402
from src.evaluation.synthetic import (  # noqa: E402
    PERSISTENT_RULE,
    TRANSIENT_RULE,
    generate_dataset,
)
from src.ingestion.spark_session import get_spark  # noqa: E402

RESULTS = ROOT / "results"
N_SNAPSHOTS = 4
TRANSIENT_UNTIL = 2
MIN_SUPPORT = 0.05
MIN_CONFIDENCE = 0.5


def _sensitivity_check(spark, dataset):
    """Run TSARM and return persistence of the injected persistent/transient rules."""
    from src.ingestion.ingest import ingest_snapshots
    from src.metrics.temporal import compute_metrics
    from src.mining.arm import mine
    from src.windowing.sliding_window import (
        assign_windows,
        build_windows,
        ordered_snapshots,
    )

    snap_map = {s.snapshot_id: (s.path, s.timestamp) for s in dataset.snapshots}
    triples = ingest_snapshots(spark, snap_map)
    snaps = ordered_snapshots(triples)
    n_windows = len(build_windows(snaps, width=1, step=1))
    windowed = assign_windows(spark, triples, width=1, step=1)
    _, rules = mine(windowed, min_support=MIN_SUPPORT, min_confidence=MIN_CONFIDENCE)
    metrics = compute_metrics(rules, n_windows=n_windows, tau=MIN_CONFIDENCE)

    def persistence_of(rule):
        row = metrics.where(
            (F.array_contains("antecedent", rule["antecedent"]))
            & (F.array_contains("consequent", rule["consequent"]))
            & (F.size("antecedent") == 1)
            & (F.size("consequent") == 1)
        ).select("persistence_score", "confidence_drift").collect()
        return row[0] if row else None

    return persistence_of(PERSISTENT_RULE), persistence_of(TRANSIENT_RULE)


def main() -> None:
    sizes = [int(x) for x in sys.argv[1:]] or [1000, 5000, 20000]

    spark = get_spark(app_name="TSARM-scale-test")
    results = []
    try:
        with tempfile.TemporaryDirectory(prefix="tsarm_scale_") as tmp:
            tmp_dir = Path(tmp)
            for n_entities in sizes:
                ds_dir = tmp_dir / f"n{n_entities}"
                gen_start = time.perf_counter()
                dataset, total_triples = generate_dataset(
                    ds_dir,
                    name=f"synthetic-{n_entities}",
                    n_entities=n_entities,
                    n_snapshots=N_SNAPSHOTS,
                    transient_until=TRANSIENT_UNTIL,
                )
                gen_time = time.perf_counter() - gen_start

                baseline = TSARMBaseline(
                    spark, min_support=MIN_SUPPORT, min_confidence=MIN_CONFIDENCE
                )
                result = baseline.run(dataset)
                results.append(result)

                tput = (
                    result.n_input_triples / result.runtime_sec
                    if result.runtime_sec
                    else 0
                )
                print(
                    f"n_entities={n_entities:>7} | triples={total_triples:>8} | "
                    f"gen={gen_time:5.1f}s | pipeline={result.runtime_sec:6.2f}s | "
                    f"{tput:8.0f} triples/s | rules={result.n_rules}"
                )

            # Temporal sensitivity check on the largest dataset.
            persistent, transient = _sensitivity_check(spark, dataset)
            print("\nTemporal sensitivity (largest dataset):")
            if persistent:
                print(
                    f"  PERSISTENT rule  persistence={persistent['persistence_score']:.3f} "
                    f"drift={persistent['confidence_drift']:+.3f}  (expect persistence high)"
                )
            if transient:
                print(
                    f"  TRANSIENT  rule  persistence={transient['persistence_score']:.3f} "
                    f"drift={transient['confidence_drift']:+.3f}  (expect persistence low / drift <= 0)"
                )

        print("\n## Scalability results\n")
        print(to_markdown(results))
        RESULTS.mkdir(exist_ok=True)
        csv_path = RESULTS / "scale_test.csv"
        to_csv(results, csv_path)
        print(f"\nWrote {csv_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
