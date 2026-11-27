#!/usr/bin/env python3
"""E4, detection only: measure the reference-event rules directly, then score them.

``propose_events.py score`` tests the *whole* pipeline: an event counts only if
TSARM mined its rule and the rule's trajectory changes at the event. On the
Wikidata series that is 0 of the confirmed events for two structural reasons:
events at the first or last snapshot transition have no window on one side
(width-2 windows), and the retyped classes are far below the mining support
threshold, so their rules are never candidates.

This driver isolates the temporal metric layer. It takes the event rules
themselves as the candidate set and computes their exact confidence in every
window with TSARM's own counting (:func:`src.mining.arm.recount_candidates`) on
the same items as the mining run (class items, P279 ancestors up to
``--superclass-depth`` hops, best-rank statements). With ``--width 1`` every
snapshot is its own window, so every transition between consecutive snapshots
is bracketed by a window before and a window after it. The confirmed events of
each sheet are then scored as before (TS, Eq. in proposal Sec. 3.7.3 iii).

``--check-report`` adds the first ``--check-n`` rules of an earlier TSARM export
and compares their recomputed confidences with the export (same windows
required, i.e. the same ``--width`` and ``--sample``), as a check that this
driver measures exactly what the mining run measured::

    python3 scripts/measure_event_rules.py --data-dir results/wikidata_azure/snapshots \\
        --events results/events/wikidata_candidates_full_review.csv \\
        --sample 0.01 --width 2 --check-report results/.../wikidata_temporal_rules_az_s001.json \\
        --tag check_s001

    python3 scripts/measure_event_rules.py --data-dir results/wikidata_azure/snapshots \\
        --events results/events/wikidata_candidates_full_review.csv \\
                 results/events/wikidata_candidates_s010_review.csv --width 1 --tag full
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.evaluation.events import load_events  # noqa: E402
from src.evaluation.quality import evaluate_events, rule_key  # noqa: E402
from src.evaluation.scaling import hardware_fingerprint  # noqa: E402


def event_rules(paths):
    """Distinct ``(antecedent list, consequent list)`` of every row (confirmed or not)."""
    out = {}
    for p in paths:
        with open(p, newline="", encoding="utf-8-sig") as fh:
            for r in csv.DictReader(line for line in fh if not line.lstrip().startswith("#")):
                ante = sorted(x.strip() for x in r["antecedent"].split(";") if x.strip())
                cons = sorted(x.strip() for x in r["consequent"].split(";") if x.strip())
                out[rule_key(ante, cons)] = (ante, cons)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--data-dir", type=Path, required=True)
    ap.add_argument("--events", nargs="+", required=True, help="review sheets (confirmed column filled)")
    ap.add_argument("--width", type=int, default=1, help="window width in snapshots (default 1)")
    ap.add_argument("--superclass-depth", type=int, default=1, help="as in run_wikidata_temporal.py (default 1)")
    ap.add_argument("--sample", type=float, help="entity sample fraction (same hash sample as the driver)")
    ap.add_argument("--epsilon", type=float, default=0.05)
    ap.add_argument("--check-report", type=Path, help="earlier TSARM export to cross-check against")
    ap.add_argument("--check-n", type=int, default=10)
    ap.add_argument("--master", default="local[*]")
    ap.add_argument("--out", type=Path, default=ROOT / "results" / "events")
    ap.add_argument("--tag", default="")
    args = ap.parse_args()
    if args.superclass_depth < 1:
        sys.exit("--superclass-depth must be >= 1 (the Wikidata runs used 1)")

    from pyspark.sql import functions as F

    from src.ingestion.spark_session import get_spark
    from src.ingestion.wikidata import load_snapshots, snapshot_ts_from_name
    from src.mining.arm import recount_candidates
    from src.mining.semantic_items import build_class_transactions, subclass_closure_spark, subclass_edges
    from src.windowing.sliding_window import assign_windows, build_windows, ordered_snapshots

    files = sorted(args.data_dir.glob("*.tsv.gz"))
    snaps = {f.name[: -len(".tsv.gz")]: (f, snapshot_ts_from_name(f.name[: -len(".tsv.gz")])) for f in files}
    snaps = dict(sorted(snaps.items(), key=lambda kv: kv[1][1]))
    rules = event_rules(args.events)
    check = {}
    if args.check_report:
        rep = json.loads(args.check_report.read_text())
        for r in rep["rules"][: args.check_n]:
            k = rule_key(r["antecedent"], r["consequent"])
            check[k] = dict(zip(r["window_series"], r["confidence_series"]))
            rules[k] = (sorted(r["antecedent"]), sorted(r["consequent"]))
    print(f"{len(rules)} rules to measure ({len(check)} check rules) over {len(snaps)} snapshots")

    timings = {}
    spark = get_spark(app_name="TSARM-measure-events", master=args.master)
    try:
        t0 = time.perf_counter()
        triples = load_snapshots(spark, snaps, best_rank_only=True, sample_fraction=args.sample)
        windows = build_windows(ordered_snapshots(triples), width=args.width, step=1)
        labels = [list(w.snapshots) for w in windows]
        windowed = assign_windows(spark, triples, width=args.width, step=1)
        closure = subclass_closure_spark(subclass_edges(windowed), max_depth=args.superclass_depth - 1)
        transactions = build_class_transactions(windowed, include_superclasses=True, closure=closure)
        cand = spark.createDataFrame(
            [(-1, k, a, c) for k, (a, c) in rules.items()],
            "window_id int, rule_key string, antecedent array<string>, consequent array<string>",
        )
        dense = recount_candidates(transactions, cand, range(len(windows))).orderBy("rule_key", "window_id")
        rows = dense.collect()
        timings["measure_s"] = time.perf_counter() - t0
        print(f"measured in {timings['measure_s']:.0f}s; windows: {labels}")
    finally:
        spark.stop()

    series = {}
    for r in rows:
        k = rule_key(r["antecedent"], r["consequent"])
        s = series.setdefault(k, {"antecedent": list(r["antecedent"]), "consequent": list(r["consequent"]),
                                  "n_transactions": [], "antecedent_count": [], "rule_count": [], "confidence": []})
        for f in ("n_transactions", "antecedent_count", "rule_count", "confidence"):
            s[f].append(r[f])

    out = {"params": {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()},
           "snapshots": list(snaps), "windows": labels, "timings_s": timings,
           "hardware": hardware_fingerprint(), "rules": series}

    if check:
        worst = 0.0
        for k, ref in check.items():
            got = series[k]["confidence"]
            worst = max(worst, max(abs(got[w] - c) for w, c in ref.items()))
        out["check"] = {"report": str(args.check_report), "n": len(check), "max_abs_diff": worst}
        print(f"CHECK vs {args.check_report.name}: {len(check)} rules, max |conf difference| = {worst:.2e}")

    from datetime import datetime

    times = {i: (min(d), max(d)) for i, d in
             enumerate([[snapshot_ts_from_name(s) for s in lab] for lab in labels])}
    trajectories = {k: dict(enumerate(s["confidence"])) for k, s in series.items()}
    out["scores"] = {}
    for p in args.events:
        events = load_events(p, confirmed_only=True)
        ts, outcomes = evaluate_events(events, trajectories, times, epsilon=args.epsilon)
        n_ok = sum(o.detected for o in outcomes)
        print(f"\n{Path(p).name}: TS = {ts:.3f} ({n_ok}/{len(outcomes)}), epsilon {args.epsilon}")
        for o in outcomes:
            if not o.detected:
                print(f"  MISS {o.event.kind:9s} {o.event.when:%Y-%m} {o.event.rule_key.replace('http://www.wikidata.org/entity/', '')}  {o.reason}")
        out["scores"][Path(p).name] = {
            "ts": ts, "n_events": len(outcomes), "n_detected": n_ok, "epsilon": args.epsilon,
            "events": [{"rule_key": o.event.rule_key, "kind": o.event.kind, "when": o.event.when.isoformat(),
                        "window_before": o.window_before, "window_after": o.window_after,
                        "conf_before": o.conf_before, "conf_after": o.conf_after,
                        "detected": o.detected, "reason": o.reason} for o in outcomes],
        }
    args.out.mkdir(parents=True, exist_ok=True)
    path = args.out / f"event_rules_measured{'_' + args.tag if args.tag else ''}.json"
    path.write_text(json.dumps(out, indent=2, default=str))
    print(f"\nwrote {path}  ({datetime.now():%Y-%m-%d %H:%M})")


if __name__ == "__main__":
    main()
