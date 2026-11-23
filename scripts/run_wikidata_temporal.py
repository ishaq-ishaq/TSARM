#!/usr/bin/env python
"""TSARM on the Wikidata snapshot series (proposal §3.7.1, Amendment 5).

Reads the per-snapshot class statements written by
``scripts/download_wikidata.sh`` (``data/processed/wikidata/<YYYY-MM>.tsv.gz``),
then runs the full pipeline:

1. **Load**: best-rank ("truthy") P31 / P279 statements per snapshot;
   optionally a consistent entity sample and a valid-time filter.
2. **Window**: sliding windows over consecutive snapshots (default width 2,
   step 1, so 7 snapshots give 6 windows).
3. **Mine**: semantic class items (Algorithm 3.1), i.e. an entity's P31
   classes plus their P279 ancestors up to ``--superclass-depth`` hops;
   per-window FP-Growth, taxonomic-rule pruning, exact candidate recount.
4. **Metrics**: TempSupp, Drift, RPS and the lifecycle class (Table 3.2 as amended).
5. **Export**: ranked rules as JSON + CSV, with parameters, stage timings and
   the hardware fingerprint.

Start with a small sample to check memory and runtime, then scale up::

    export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
    python3 scripts/run_wikidata_temporal.py --sample 0.01
    python3 scripts/run_wikidata_temporal.py --sample 0.1
    python3 scripts/run_wikidata_temporal.py            # everything

Any subset of at least ``--width`` snapshots works, e.g. two downloaded
snapshots: ``--snapshots 2022-01,2026-11 --width 1``.

**Valid-time windows** (proposal Equation 3.1 on real-world time): mine one
dump over calendar windows built from the P580/P582/P585 qualifiers, e.g. ten
windows of 5 years every 5 years from 1975::

    python3 scripts/run_wikidata_temporal.py --snapshots 2026-11 \
        --valid-time 1975-01,2025-01,60,60 --unqualified drop
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.evaluation.scaling import hardware_fingerprint  # noqa: E402
from src.ingestion.spark_session import get_spark  # noqa: E402
from src.ingestion.wikidata import load_snapshots, snapshot_ts_from_name  # noqa: E402
from src.metrics.export import export_rules  # noqa: E402
from src.metrics.temporal import DEFAULT_TAU, LIFECYCLE_CLASSES, compute_metrics  # noqa: E402
from src.mining.arm import mine_temporal  # noqa: E402
from src.windowing.sliding_window import assign_windows, build_windows, ordered_snapshots  # noqa: E402

DEFAULT_DIR = ROOT / "data" / "processed" / "wikidata"


def _short(iri: str) -> str:
    return iri.rsplit("/", 1)[-1]


def find_snapshots(data_dir: Path, wanted=None):
    files = sorted(data_dir.glob("*.tsv.gz"))
    snaps = {}
    for f in files:
        name = f.name[: -len(".tsv.gz")]
        if wanted and name not in wanted:
            continue
        snaps[name] = (f, snapshot_ts_from_name(name))
    missing = sorted(set(wanted or ()) - set(snaps))
    if missing:
        sys.exit(f"Snapshot file(s) not found in {data_dir}: {', '.join(missing)}")
    return dict(sorted(snaps.items(), key=lambda kv: kv[1][1]))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--data-dir", type=Path, default=DEFAULT_DIR)
    ap.add_argument("--snapshots", help="comma-separated subset, e.g. 2017-01,2018-01 (default: all)")
    ap.add_argument("--width", type=int, default=2, help="window width in snapshots (default 2)")
    ap.add_argument("--step", type=int, default=1)
    ap.add_argument("--min-support", type=float, default=0.01, help="σ (§3.7.4: 0.01)")
    ap.add_argument("--min-confidence", type=float, default=0.5, help="§3.7.4: 0.50")
    ap.add_argument("--tau", type=float, default=DEFAULT_TAU, help="RPS threshold τ (0.70)")
    ap.add_argument("--superclass-depth", type=int, default=1,
                    help="P279 hops of ancestors added as items; 0 = none, -1 = full closure (default 1)")
    ap.add_argument("--all-ranks", action="store_true", help="keep non-best-rank (but not deprecated) statements too")
    ap.add_argument("--valid-at-snapshot", action="store_true",
                    help="drop statements whose P580/P582 dates exclude the snapshot date")
    ap.add_argument("--sample", type=float, help="fraction of entities to keep, e.g. 0.01")
    ap.add_argument("--valid-time", metavar="START,END,WIDTH,STEP",
                    help="valid-time windows instead of snapshot windows: YYYY-MM,YYYY-MM,months,months")
    ap.add_argument("--unqualified", choices=["keep", "drop"], default="keep",
                    help="with --valid-time: keep statements without dates in every window, or drop them")
    ap.add_argument("--master", default="local[*]")
    ap.add_argument("--out", type=Path, default=ROOT / "results")
    ap.add_argument("--miner", choices=["auto", "fpgrowth", "bounded"], default="auto",
                    help="auto: bounded (ancestor-pruned, <= 3 items) for class items with superclasses, else FP-Growth")
    ap.add_argument("--tag", default="", help="suffix for the output file names")
    ap.add_argument("--top", type=int, default=5, help="rules to print per lifecycle class")
    args = ap.parse_args()

    wanted = [s.strip() for s in args.snapshots.split(",")] if args.snapshots else None
    snapshots = find_snapshots(args.data_dir.resolve(), wanted)
    if args.valid_time:
        if len(snapshots) != 1:
            sys.exit("--valid-time uses one snapshot (normally the latest): pass --snapshots YYYY-MM.")
    elif len(snapshots) < max(args.width, 1) or (args.width == 1 and len(snapshots) < 2):
        sys.exit(f"Need at least {max(args.width, 2)} snapshots; found {list(snapshots)} in {args.data_dir}")

    depth = None if args.superclass_depth < 0 else args.superclass_depth
    timings = {}
    spark = get_spark(app_name="TSARM-wikidata", master=args.master)
    try:
        t0 = time.perf_counter()
        triples = load_snapshots(
            spark, snapshots,
            best_rank_only=not args.all_ranks,
            sample_fraction=args.sample,
            valid_at_snapshot=args.valid_at_snapshot,
        ).cache()
        per_snap = {r["snapshot"]: r["count"] for r in triples.groupBy("snapshot").count().collect()}
        n_triples = sum(per_snap.values())
        timings["load_s"] = time.perf_counter() - t0
        print(f"Loaded {n_triples:,} statements in {timings['load_s']:.1f}s")
        for snap in snapshots:
            print(f"  {snap}: {per_snap.get(snap, 0):,}")

        t1 = time.perf_counter()
        if args.valid_time:
            from datetime import datetime as _dt

            from src.windowing.valid_time import assign_valid_time_windows, build_time_windows

            vs, ve, vw, vstep = args.valid_time.split(",")
            tw = build_time_windows(_dt.strptime(vs, "%Y-%m").date(), _dt.strptime(ve, "%Y-%m").date(),
                                    int(vw), int(vstep))
            windowed = assign_valid_time_windows(spark, triples, tw, unqualified=args.unqualified)
            window_labels = [[str(w.start), str(w.end)] for w in tw]
            print(f"Valid-time windows ({vw} months every {vstep}; unqualified statements: {args.unqualified}):")
        else:
            windows = build_windows(ordered_snapshots(triples), width=args.width, step=args.step)
            windowed = assign_windows(spark, triples, width=args.width, step=args.step)
            window_labels = [list(w.snapshots) for w in windows]
            print(f"Windows (width={args.width}, step={args.step}):")
        for i, label in enumerate(window_labels):
            print(f"  w{i} = {label}")
        windows = window_labels
        _, rules, candidates = mine_temporal(
            windowed, range(len(windows)),
            min_support=args.min_support, min_confidence=args.min_confidence,
            item_mode="class", include_superclasses=depth != 0,
            superclass_depth=depth if depth else None, miner=args.miner,
        )
        metrics = compute_metrics(candidates, n_windows=len(windows), tau=args.tau).cache()
        n_rules = metrics.count()
        timings["mine_and_metrics_s"] = time.perf_counter() - t1
        print(f"\n{n_rules:,} rules in {timings['mine_and_metrics_s']:.1f}s")

        rows = metrics.orderBy("lifecycle", "persistence_score", "mean_confidence", ascending=False).collect()
        by_class = Counter(r["lifecycle"] for r in rows)
        for cls in LIFECYCLE_CLASSES:
            members = [r for r in rows if r["lifecycle"] == cls]
            print(f"\n{cls.upper()}: {by_class.get(cls, 0)}")
            for r in members[: args.top]:
                ante = ", ".join(_short(x) for x in r["antecedent"])
                cons = ", ".join(_short(x) for x in r["consequent"])
                conf = ", ".join(f"{c:.2f}" for c in r["confidence_series"])
                print(f"  {ante} => {cons}   conf [{conf}]  drift {r['confidence_drift']:+.3f}"
                      f"  RPS {r['persistence_score']:.2f}")

        t2 = time.perf_counter()
        suffix = f"_{args.tag}" if args.tag else ""
        json_path, csv_path = export_rules(
            metrics.orderBy("lifecycle", "persistence_score", "mean_confidence", ascending=False),
            args.out, basename=f"wikidata_temporal_rules{suffix}",
            params={
                "dataset": "wikidata P31/P279",
                "snapshots": list(snapshots),
                "statements_per_snapshot": per_snap,
                "windows": windows,
                "window_time": "valid" if args.valid_time else "transaction",
                "valid_time": args.valid_time, "unqualified": args.unqualified if args.valid_time else None,
                "width": args.width, "step": args.step,
                "min_support": args.min_support, "min_confidence": args.min_confidence,
                "tau": args.tau, "superclass_depth": args.superclass_depth, "miner": args.miner,
                "best_rank_only": not args.all_ranks, "valid_at_snapshot": args.valid_at_snapshot,
                "sample_fraction": args.sample, "master": args.master,
                "lifecycle_counts": dict(by_class),
                "timings_s": timings,
                "hardware": hardware_fingerprint(),
            },
        )
        timings["export_s"] = time.perf_counter() - t2
        print(f"\nWrote {json_path} and {csv_path}")
        print(f"Total {time.perf_counter() - t0:.1f}s")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
