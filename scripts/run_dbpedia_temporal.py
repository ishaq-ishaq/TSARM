#!/usr/bin/env python
"""Multi-window temporal analysis on real DBpedia releases (objectives ii + iv).

Uses the DBpedia English ``instance_types`` releases found in ``data/raw``
(``dbpedia_<YYYY-MM>_instance_types_en.ttl.bz2``; get them with
``scripts/download_dbpedia.sh``) as temporal snapshots, forms sliding windows
over them, mines rules per window, recounts every candidate in every window and
computes the temporal metrics and lifecycle classes.

Two item models (Section 4.2.2 of the thesis):

* ``--item-mode fact`` (default): items are the entity's ``rdf:type=Class``
  facts. With one direct type per release, a window's transaction holds the
  types an entity had in the window's releases, so rules describe *type
  changes* between releases.
* ``--item-mode class``: items are the entity's classes plus their ancestors in
  the DBpedia ontology (``--ontology``, default
  ``data/raw/dbpedia_ontology_2016-10.owl``) up to ``--superclass-depth`` hops;
  taxonomic rules (Person => Agent) are pruned.

The defaults reproduce the original three-release run (2015-10, 2016-04,
2016-10; width 2, step 1; sigma 0.005, minimum confidence 0.2, tau 0.2)::

    export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
    python3 scripts/run_dbpedia_temporal.py
    python3 scripts/run_dbpedia_temporal.py --releases all --tag series
    python3 scripts/run_dbpedia_temporal.py --item-mode class --tag class
"""

import argparse
import re
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pyspark.sql import functions as F  # noqa: E402

from src.evaluation.scaling import hardware_fingerprint  # noqa: E402
from src.ingestion.ingest import ingest_snapshots  # noqa: E402
from src.ingestion.spark_session import get_spark  # noqa: E402
from src.metrics.export import export_rules  # noqa: E402
from src.metrics.temporal import compute_metrics  # noqa: E402
from src.mining.arm import mine_temporal  # noqa: E402
from src.windowing.sliding_window import (  # noqa: E402
    assign_windows,
    build_windows,
    ordered_snapshots,
)

RAW = ROOT / "data" / "raw"
DEFAULT_RELEASES = "2015-10,2016-04,2016-10"
_NAME = re.compile(r"dbpedia_(\d{4}-\d{2})_instance_types_en\.ttl\.bz2$")


def _short(item: str) -> str:
    return item.split("/")[-1].split("#")[-1]


def find_releases(spec: str):
    found = {}
    for f in sorted(RAW.glob("dbpedia_*_instance_types_en.ttl.bz2")):
        m = _NAME.search(f.name)
        if m:
            found[m.group(1)] = (f, datetime.strptime(m.group(1), "%Y-%m"))
    if spec == "all":
        return found
    wanted = [r.strip() for r in spec.split(",") if r.strip()]
    missing = [r for r in wanted if r not in found]
    if missing:
        sys.exit(f"Missing DBpedia release(s) {missing} in {RAW}. Run scripts/download_dbpedia.sh first.")
    return {r: found[r] for r in wanted}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--releases", default=DEFAULT_RELEASES, help="comma-separated YYYY-MM, or 'all'")
    ap.add_argument("--width", type=int, default=2)
    ap.add_argument("--step", type=int, default=1)
    ap.add_argument("--min-support", type=float, default=0.005)
    ap.add_argument("--min-confidence", type=float, default=0.2)
    ap.add_argument("--tau", type=float, default=0.2)
    ap.add_argument("--item-mode", choices=["fact", "class"], default="fact")
    ap.add_argument("--ontology", type=Path, default=RAW / "dbpedia_ontology_2016-10.owl")
    ap.add_argument("--superclass-depth", type=int, default=-1, help="-1 = full closure (DBpedia's ontology is shallow)")
    ap.add_argument("--class-namespace", default="http://dbpedia.org/ontology/",
                    help="keep only hierarchy edges within this namespace ('' = keep all)")
    ap.add_argument("--tag", default="")
    args = ap.parse_args()

    releases = find_releases(args.releases)
    if len(releases) < 2:
        sys.exit(f"Need at least two releases, found {list(releases)}.")

    spark = get_spark(app_name="TSARM-dbpedia-temporal", config={"spark.sql.shuffle.partitions": "32"})
    try:
        start = time.perf_counter()
        triples = ingest_snapshots(spark, releases)
        n_edges = 0
        if args.item_mode == "class":
            from src.ingestion.ontology import load_subclass_edges, with_hierarchy

            if not args.ontology.exists():
                sys.exit(f"Ontology {args.ontology} not found. Run scripts/download_dbpedia.sh first.")
            edges = load_subclass_edges(args.ontology, namespace=args.class_namespace or None)
            n_edges = len(edges)
            print(f"Ontology: {n_edges} subclass edges from {args.ontology.name}")
            triples = with_hierarchy(spark, triples, edges)
        triples = triples.cache()
        per_release = {r["snapshot"]: r["count"] for r in triples.groupBy("snapshot").count().collect()}
        n_triples = sum(per_release.values()) - n_edges * len(releases)
        print(f"Ingested {n_triples:,} triples from {len(releases)} DBpedia releases.")
        for r in releases:
            print(f"  {r}: {per_release.get(r, 0) - n_edges:,}")

        windows = build_windows(ordered_snapshots(triples), width=args.width, step=args.step)
        n_windows = len(windows)
        print(f"Sliding windows (width={args.width}, step={args.step}):")
        for w in windows:
            print(f"  w{w.window_id} = {w.snapshots}")

        windowed = assign_windows(spark, triples, width=args.width, step=args.step)
        depth = None if args.superclass_depth < 0 else args.superclass_depth
        _, _, candidates = mine_temporal(
            windowed, range(n_windows), min_support=args.min_support, min_confidence=args.min_confidence,
            item_mode=args.item_mode, include_superclasses=depth != 0, superclass_depth=depth or None,
        )
        metrics = compute_metrics(candidates, n_windows=n_windows, tau=args.tau).cache()
        n_rules = metrics.count()
        print(f"\nMined {n_rules} distinct rule(s) across {n_windows} windows.")

        rows = metrics.orderBy(F.col("lifecycle"), F.col("persistence_score").desc(), F.col("mean_confidence").desc()).collect()
        for r in rows:
            ante = ", ".join(_short(x) for x in r["antecedent"])
            cons = ", ".join(_short(x) for x in r["consequent"])
            present_in = ", ".join(f"w{w}" for w, e in zip(r["window_series"], r["emitted_series"]) if e)
            conf_series = ", ".join(f"{c:.3f}" for c in r["confidence_series"])
            print(
                f"  [{ante}] => [{cons}]\n"
                f"      {r['lifecycle'].upper()}: frequent in [{present_in}] | confidence [{conf_series}] | "
                f"drift={r['confidence_drift']:+.3f} | persistence={r['persistence_score']:.3f}"
            )

        wall = time.perf_counter() - start
        suffix = f"_{args.tag}" if args.tag else ""
        json_path, csv_path = export_rules(
            metrics.orderBy(F.col("lifecycle"), F.col("persistence_score").desc(), F.col("mean_confidence").desc()),
            ROOT / "results",
            basename=f"dbpedia_temporal_rules{suffix}",
            params={
                "dataset": "dbpedia instance_types_en",
                "snapshots": list(releases),
                "triples_per_release": {r: per_release.get(r, 0) - n_edges for r in releases},
                "width": args.width, "step": args.step,
                "min_support": args.min_support, "min_confidence": args.min_confidence, "tau": args.tau,
                "item_mode": args.item_mode,
                "ontology": args.ontology.name if args.item_mode == "class" else None,
                "ontology_subclass_edges": n_edges or None,
                "class_namespace": args.class_namespace if args.item_mode == "class" else None,
                "superclass_depth": args.superclass_depth if args.item_mode == "class" else None,
                "wall_time_s": wall,
                "hardware": hardware_fingerprint(),
            },
        )
        print(f"\nWrote {json_path} and {csv_path}")
        print(f"Total wall time: {wall:.1f}s")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
