#!/usr/bin/env python
"""TSARM on a set of per-snapshot N-Triples files (the inputs given to the baselines).

The head-to-head comparison (experiment E5) needs TSARM, AMIE+ and RDFRules to
read the *same* files. The Wikidata sample exports
(``scripts/export_wikidata_sample.py``) are one N-Triples file per snapshot, named
``..._<YYYY-MM>_...nt.gz``; this driver ingests them as snapshots, builds the
sliding windows and runs the full pipeline with fact items
(``predicate=object``, i.e. ``P31=<class>`` on those exports), exactly as
``scripts/run_dbpedia_temporal.py`` does for DBpedia::

    python3 scripts/run_nt_temporal.py --input results/wikidata_sample_0001/*.nt.gz \\
        --min-support 0.01 --min-confidence 0.5 --tag wd_s0001

Writes ``results/<basename>_<tag>.json`` / ``.csv`` with the parameters, the
stage timings and the hardware fingerprint.
"""

from __future__ import annotations

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
from src.metrics.temporal import DEFAULT_TAU, LIFECYCLE_CLASSES, compute_metrics  # noqa: E402
from src.mining.arm import mine_temporal  # noqa: E402
from src.windowing.sliding_window import assign_windows, build_windows, ordered_snapshots  # noqa: E402

_SNAP = re.compile(r"(?<!\d)(\d{4})-(\d{2})(?!\d)")


def snapshots_from_files(paths):
    """``{YYYY-MM: (path, timestamp)}`` from file names containing YYYY-MM."""
    snaps = {}
    for p in paths:
        m = _SNAP.search(Path(p).name)
        if not m:
            sys.exit(f"No YYYY-MM snapshot date in file name: {p}")
        name = f"{m.group(1)}-{m.group(2)}"
        if name in snaps:
            sys.exit(f"Two files for snapshot {name}: {snaps[name][0]} and {p}")
        snaps[name] = (str(p), datetime(int(m.group(1)), int(m.group(2)), 1))
    return dict(sorted(snaps.items(), key=lambda kv: kv[1][1]))


def _short(iri: str) -> str:
    return iri.rsplit("/", 1)[-1]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--input", nargs="+", required=True, help="one N-Triples file per snapshot (.nt, .nt.gz)")
    ap.add_argument("--width", type=int, default=2)
    ap.add_argument("--step", type=int, default=1)
    ap.add_argument("--min-support", type=float, default=0.01)
    ap.add_argument("--min-confidence", type=float, default=0.5)
    ap.add_argument("--tau", type=float, default=DEFAULT_TAU)
    ap.add_argument("--out", type=Path, default=ROOT / "results")
    ap.add_argument("--basename", default="nt_temporal_rules")
    ap.add_argument("--tag", default="")
    ap.add_argument("--top", type=int, default=3)
    args = ap.parse_args()

    snapshots = snapshots_from_files(args.input)
    if len(snapshots) < max(args.width, 2):
        sys.exit(f"Need at least {max(args.width, 2)} snapshots, got {list(snapshots)}")

    timings = {}
    spark = get_spark(app_name="TSARM-nt-temporal")
    try:
        t0 = time.perf_counter()
        triples = ingest_snapshots(spark, snapshots).cache()
        per_snap = {r["snapshot"]: r["count"] for r in triples.groupBy("snapshot").count().collect()}
        timings["load_s"] = time.perf_counter() - t0
        print(f"Ingested {sum(per_snap.values()):,} triples in {timings['load_s']:.1f}s")
        for s in snapshots:
            print(f"  {s}: {per_snap.get(s, 0):,}")

        t1 = time.perf_counter()
        windows = build_windows(ordered_snapshots(triples), width=args.width, step=args.step)
        windowed = assign_windows(spark, triples, width=args.width, step=args.step)
        _, _, candidates = mine_temporal(
            windowed, range(len(windows)), min_support=args.min_support,
            min_confidence=args.min_confidence, item_mode="fact",
        )
        metrics = compute_metrics(candidates, n_windows=len(windows), tau=args.tau).cache()
        n_rules = metrics.count()
        timings["mine_and_metrics_s"] = time.perf_counter() - t1
        print(f"{n_rules:,} rules over {len(windows)} windows in {timings['mine_and_metrics_s']:.1f}s")

        ordered = metrics.orderBy(F.col("lifecycle"), F.col("persistence_score").desc(),
                                  F.col("mean_confidence").desc())
        rows = ordered.collect()
        counts = {}
        for r in rows:
            counts[r["lifecycle"]] = counts.get(r["lifecycle"], 0) + 1
        for cls in LIFECYCLE_CLASSES:
            members = [r for r in rows if r["lifecycle"] == cls]
            print(f"{cls.upper()}: {len(members)}")
            for r in members[: args.top]:
                conf = ", ".join(f"{c:.2f}" for c in r["confidence_series"])
                print(f"  {', '.join(map(_short, r['antecedent']))} => {', '.join(map(_short, r['consequent']))}"
                      f"   conf [{conf}]  RPS {r['persistence_score']:.2f}")

        timings["total_s"] = time.perf_counter() - t0
        suffix = f"_{args.tag}" if args.tag else ""
        json_path, csv_path = export_rules(
            ordered, args.out, basename=f"{args.basename}{suffix}",
            params={
                "inputs": {s: p for s, (p, _) in snapshots.items()},
                "snapshots": list(snapshots), "triples_per_snapshot": per_snap,
                "windows": [list(w.snapshots) for w in windows],
                "width": args.width, "step": args.step, "item_mode": "fact",
                "min_support": args.min_support, "min_confidence": args.min_confidence, "tau": args.tau,
                "lifecycle_counts": counts, "timings_s": timings,
                "hardware": hardware_fingerprint(),
            },
        )
        print(f"Wrote {json_path} and {csv_path}")
        print(f"Total {timings['total_s']:.1f}s")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
