#!/usr/bin/env python
"""Experiments E3 (rule quality) and E4 (temporal sensitivity) on synthetic data.

The synthetic generator plants known ground truth (src/evaluation/synthetic.py):

* a **persistent** rule ``type=Class0 => worksAt=Org0`` holding in every snapshot;
* a **transient** rule ``type=Class1 => worksAt=Org1`` holding only before
  snapshot ``transient_until`` and then collapsing.

So the reference rule set is ``R_true = {persistent, transient}``, and the
reference event list is "the transient rule *decays* at snapshot
``transient_until``". This validates the E3/E4 machinery on controlled data.
For Wikidata the same functions take hand-validated reference files instead
(``load_reference_rules``).

E3 compares TSARM's rule set with AMIE's (pooled graph) when ``$AMIE_JAR`` /
``tools/amie`` is available. E4 applies to TSARM only: the static baselines
produce no per-window confidence, so they cannot detect a transition.

Usage::

    export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
    python3 scripts/evaluate_quality.py --entities 20000
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.evaluation.quality import (  # noqa: E402
    Event,
    amie_rule_key,
    evaluate_events,
    rdfrules_rule_key,
    precision_recall_f1,
    rule_key,
)
from src.evaluation.synthetic import (  # noqa: E402
    PERSISTENT_RULE,
    TRANSIENT_RULE,
    generate_dataset,
)

RESULTS = ROOT / "results"
N_SNAPSHOTS, TRANSIENT_UNTIL, START_YEAR = 4, 2, 2020
MIN_SUPPORT, MIN_CONFIDENCE, TAU = 0.05, 0.3, 0.7


def _key(rule):
    return rule_key([rule["antecedent"]], [rule["consequent"]])


def run_tsarm(dataset):
    from src.ingestion.ingest import ingest_snapshots
    from src.ingestion.spark_session import get_spark
    from src.metrics.temporal import compute_metrics
    from src.mining.arm import mine_temporal
    from src.windowing.sliding_window import assign_windows, build_windows, ordered_snapshots

    spark = get_spark(app_name="TSARM-quality")
    try:
        triples = ingest_snapshots(spark, {s.snapshot_id: (s.path, s.timestamp) for s in dataset.snapshots})
        snaps = ordered_snapshots(triples)
        windows = build_windows(snaps, width=1, step=1)
        windowed = assign_windows(spark, triples, width=1, step=1)
        _, _, cands = mine_temporal(windowed, range(len(windows)), MIN_SUPPORT, MIN_CONFIDENCE)
        metrics = compute_metrics(cands, n_windows=len(windows), tau=TAU).collect()
    finally:
        spark.stop()
    ts_by_snap = {s.snapshot_id: s.timestamp for s in dataset.snapshots}
    win_times = {
        w.window_id: (min(ts_by_snap[s] for s in w.snapshots), max(ts_by_snap[s] for s in w.snapshots))
        for w in windows
    }
    rules = {
        m.rule_key: {
            "lifecycle": m.lifecycle,
            "trajectory": dict(zip(m.window_series, m.confidence_series)),
        }
        for m in metrics
    }
    return rules, win_times


def run_amie(dataset, out_dir: Path):
    jar = ROOT / "tools" / "amie" / "amie3.5.1.jar"
    if not jar.exists():
        return None
    cmd = [sys.executable, str(ROOT / "scripts" / "amie_mine.py"), "--input",
           *[str(s.path) for s in dataset.snapshots], "--output", str(out_dir),
           "--min-support", str(MIN_SUPPORT), "--min-confidence", str(MIN_CONFIDENCE)]
    subprocess.run(cmd, check=True, capture_output=True, text=True)
    with open(out_dir / "rules.csv", newline="") as fh:
        rows = list(csv.DictReader(fh))
    keys = [amie_rule_key(r["rule"]) for r in rows]
    confs = {k: float(r["confidence"]) for k, r in zip(keys, rows) if k}
    return {"n_total": len(rows), "comparable": confs}


def run_rdfrules(dataset, out_dir: Path):
    home = ROOT / "tools" / "rdfrules"
    if not (home / "bin" / "main").exists():
        return None
    import os

    cmd = [sys.executable, str(ROOT / "scripts" / "rdfrules_mine.py"), "--input",
           *[str(s.path) for s in dataset.snapshots], "--output", str(out_dir),
           "--min-support", str(MIN_SUPPORT), "--min-confidence", str(MIN_CONFIDENCE)]
    subprocess.run(cmd, check=True, capture_output=True, text=True,
                   env=dict(os.environ, RDFRULES_HOME=str(home)))
    export = json.loads((out_dir / "rdfrules_export.json").read_text())
    confs = {}
    for rule in export:
        k = rdfrules_rule_key(rule)
        if k:
            conf = next((m["value"] for m in rule.get("measures", []) if m.get("name") == "CwaConfidence"), None)
            confs[k] = conf
    return {"n_total": len(export), "comparable": confs}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--entities", type=int, default=20000)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    true_rules = {_key(PERSISTENT_RULE), _key(TRANSIENT_RULE)}
    decay_at = datetime(START_YEAR + TRANSIENT_UNTIL, 1, 1)
    events = [Event(_key(TRANSIENT_RULE), decay_at, "decay", "planted transient rule collapses")]

    with tempfile.TemporaryDirectory(prefix="tsarm_quality_") as tmp:
        dataset, n_triples = generate_dataset(
            Path(tmp) / "data", name=f"synthetic-{args.entities}", n_entities=args.entities,
            n_snapshots=N_SNAPSHOTS, transient_until=TRANSIENT_UNTIL, start_year=START_YEAR, seed=args.seed,
        )
        tsarm_rules, win_times = run_tsarm(dataset)
        amie = run_amie(dataset, Path(tmp) / "amie")
        rdfrules = run_rdfrules(dataset, Path(tmp) / "rdfrules")

    report = {"dataset": dataset.name, "triples": n_triples, "reference_rules": sorted(true_rules)}

    q = precision_recall_f1(tsarm_rules, true_rules)
    report["E3_tsarm"] = vars(q)
    print(f"E3 TSARM : {q.n_mined} rules, P={q.precision:.3f} R={q.recall:.3f} F1={q.f1:.3f}")
    for k in sorted(true_rules):
        r = tsarm_rules.get(k)
        print(f"   {k.split('/')[-1]:<10} -> {r['lifecycle'] if r else 'NOT FOUND'}")

    if amie is not None:
        qa = precision_recall_f1(amie["comparable"], true_rules)
        report["E3_amie"] = {**vars(qa), "n_total_rules": amie["n_total"]}
        print(f"E3 AMIE+ : {amie['n_total']} rules ({qa.n_mined} comparable), "
              f"P={qa.precision:.3f} R={qa.recall:.3f} F1={qa.f1:.3f}")
        for k in sorted(true_rules):
            c = amie["comparable"].get(k)
            print(f"   {k.split('/')[-1]:<10} -> pooled confidence {c if c is None else round(c, 3)}")

    if rdfrules is not None:
        qr = precision_recall_f1(rdfrules["comparable"], true_rules)
        report["E3_rdfrules"] = {**vars(qr), "n_total_rules": rdfrules["n_total"]}
        print(f"E3 RDFRules: {rdfrules['n_total']} rules ({qr.n_mined} comparable), "
              f"P={qr.precision:.3f} R={qr.recall:.3f} F1={qr.f1:.3f}")
        for k in sorted(true_rules):
            c = rdfrules["comparable"].get(k)
            print(f"   {k.split('/')[-1]:<10} -> pooled confidence {c if c is None else round(c, 3)}")

    ts, outcomes = evaluate_events(
        events, {k: v["trajectory"] for k, v in tsarm_rules.items()}, win_times, epsilon=0.05
    )
    report["E4_tsarm"] = {
        "temporal_sensitivity": ts,
        "events": [
            {"rule": o.event.rule_key, "kind": o.event.kind, "when": o.event.when.isoformat(),
             "detected": o.detected, "reason": o.reason}
            for o in outcomes
        ],
    }
    print(f"E4 TSARM : TS = {ts:.2f}  ({'; '.join(o.reason for o in outcomes)})")
    print("E4 AMIE+ / RDFRules: not applicable (no per-window confidence on a pooled graph)")

    RESULTS.mkdir(exist_ok=True)
    out = RESULTS / f"quality_{dataset.name}.json"
    out.write_text(json.dumps(report, indent=2))
    print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
