#!/usr/bin/env python
"""Run TSARM on real DBpedia release snapshots (objective iv: real-world data).

Ingests two DBpedia `instance_types_en` dumps (2015-10 and 2016-10 releases) as
temporal snapshots and runs the pipeline. Spark reads the ``.bz2`` files
directly, so no manual decompression is needed.

Note on this dataset: ``instance_types_en`` assigns essentially one ``rdf:type``
per entity per release, so per-snapshot transactions are single-item and yield
no association rules. Using a single window spanning both releases (``width=2``)
lets entities whose type was *refined* between 2015 and 2016 form co-occurring
type items, surfacing type-evolution rules. (Meaningful temporal trajectories
would need more than two snapshots and/or a multi-predicate dump such as
``mappingbased_objects``.)

Usage (from the project root)::

    export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
    python3 scripts/run_dbpedia.py
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
from src.mining.arm import build_transactions, mine_rules  # noqa: E402
from src.windowing.sliding_window import assign_windows  # noqa: E402

RAW = ROOT / "data" / "raw"
SNAP_2015 = RAW / "dbpedia_2015-10_instance_types_en.ttl.bz2"
SNAP_2016 = RAW / "dbpedia_2016-10_instance_types_en.ttl.bz2"

WIDTH = 2  # one window spanning both releases (see module docstring)
MIN_SUPPORT = 0.01
MIN_CONFIDENCE = 0.3


def main() -> None:
    for path in (SNAP_2015, SNAP_2016):
        if not path.exists():
            sys.exit(f"Missing snapshot {path}. Download the DBpedia dumps first.")

    spark = get_spark(app_name="TSARM-dbpedia", config={"spark.sql.shuffle.partitions": "32"})
    try:
        start = time.perf_counter()
        triples = ingest_snapshots(
            spark,
            {
                "2015-10": (SNAP_2015, datetime(2015, 10, 1)),
                "2016-10": (SNAP_2016, datetime(2016, 10, 1)),
            },
        )
        triples = triples.cache()
        n_triples = triples.count()
        ingest_time = time.perf_counter() - start
        print(
            f"Ingested {n_triples:,} real DBpedia triples from 2 releases "
            f"in {ingest_time:.1f}s "
            f"({n_triples / ingest_time:,.0f} triples/s)."
        )

        print("\nTriples per snapshot:")
        triples.groupBy("snapshot").count().orderBy("snapshot").show()

        n_entities = triples.select("subject").distinct().count()
        n_classes = triples.select("object").distinct().count()
        print(f"Distinct entities: {n_entities:,} | distinct classes: {n_classes:,}")

        # Mining: single window spanning both releases.
        windowed = assign_windows(spark, triples, width=WIDTH, step=1)
        transactions = build_transactions(windowed)
        multi = transactions.where(F.size("items") > 1).count()
        print(
            f"\nEntities with >1 type across the two releases "
            f"(type refinement): {multi:,}"
        )

        mine_start = time.perf_counter()
        _, rules = mine_rules(
            transactions, min_support=MIN_SUPPORT, min_confidence=MIN_CONFIDENCE
        )
        n_rules = rules.count()
        mine_time = time.perf_counter() - mine_start
        print(f"Mined {n_rules} type-evolution rule(s) in {mine_time:.1f}s.")

        if n_rules:
            print("\nTop rules by confidence:")
            rules.orderBy(F.col("confidence").desc()).select(
                "antecedent", "consequent", "confidence", "support", "lift"
            ).show(15, truncate=55)

        print(f"\nTotal wall time: {time.perf_counter() - start:.1f}s")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
