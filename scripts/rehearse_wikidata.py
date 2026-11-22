#!/usr/bin/env python
"""Rehearse the Wikidata run on synthetic Wikidata-like data.

Generates the seven-snapshot series with ``src.evaluation.synthetic_wikidata``
(same file format as the real extractor), runs the real driver
``scripts/run_wikidata_temporal.py`` on it for each ancestor depth, and
records runtime, rule counts per lifecycle class and temporal sensitivity on
the two planted reclassification events. This answers, before the real data
arrive, which ``--superclass-depth`` and sample size are feasible on this
machine, and checks E4 end to end on Wikidata-shaped output.

    export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
    caffeinate -i python3 scripts/rehearse_wikidata.py --entities 200000,2000000 --depths 1,2,3,-1
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.evaluation.quality import Event, evaluate_events, rule_key  # noqa: E402
from src.evaluation.synthetic_wikidata import generate  # noqa: E402


def _trajectories(report):
    out = {}
    for r in report["rules"]:
        key = rule_key(r["antecedent"], r["consequent"])
        out[key] = dict(zip(r["window_series"], r["confidence_series"]))
    return out


def _window_times(report):
    times = {}
    for i, snaps in enumerate(report["params"]["windows"]):
        ds = [datetime.strptime(s, "%Y-%m") for s in snaps]
        times[i] = (min(ds), max(ds))
    return times


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--entities", default="200000", help="comma-separated final population sizes")
    ap.add_argument("--depths", default="1,2,-1", help="superclass depths to try; -1 = full closure")
    ap.add_argument("--classes", type=int, default=5000)
    ap.add_argument("--max-depth", type=int, default=12, help="depth of the generated hierarchy")
    ap.add_argument("--min-support", type=float, default=0.01)
    ap.add_argument("--min-confidence", type=float, default=0.5)
    ap.add_argument("--timeout", type=int, default=1800, help="seconds per run")
    ap.add_argument("--out", type=Path, default=ROOT / "results" / "wikidata_rehearsal.csv")
    args = ap.parse_args()

    rows = []
    for n in [int(x) for x in args.entities.split(",")]:
        with tempfile.TemporaryDirectory(prefix="tsarm_wd_rehearsal_") as tmp:
            t0 = time.perf_counter()
            files, events, n_rows = generate(Path(tmp) / "data", n_entities=n, n_classes=args.classes,
                                             max_depth=args.max_depth)
            print(f"\n== {n:,} entities: {n_rows:,} statements over 7 snapshots "
                  f"(generated in {time.perf_counter() - t0:.0f}s)", flush=True)
            ref = [Event(rule_key([e.antecedent], [e.consequent]), e.when, e.kind) for e in events]
            for depth in [int(x) for x in args.depths.split(",")]:
                tag = f"rehearsal_n{n}_h{depth}"
                cmd = [sys.executable, str(ROOT / "scripts" / "run_wikidata_temporal.py"),
                       "--data-dir", str(Path(tmp) / "data"), "--out", tmp, "--tag", tag,
                       "--superclass-depth", str(depth), "--min-support", str(args.min_support),
                       "--min-confidence", str(args.min_confidence)]
                t1 = time.perf_counter()
                try:
                    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=args.timeout)
                    status = "ok" if proc.returncode == 0 else "error"
                except subprocess.TimeoutExpired:
                    proc, status = None, "timeout"
                wall = time.perf_counter() - t1
                row = {"entities": n, "statements": n_rows, "superclass_depth": depth, "status": status,
                       "wall_s": round(wall, 1)}
                if status == "ok":
                    report = json.loads((Path(tmp) / f"wikidata_temporal_rules_{tag}.json").read_text())
                    p = report["params"]
                    ts, outcomes = evaluate_events(ref, _trajectories(report), _window_times(report))
                    row.update({
                        "load_s": round(p["timings_s"]["load_s"], 1),
                        "mine_and_metrics_s": round(p["timings_s"]["mine_and_metrics_s"], 1),
                        "n_rules": report["n_rules"],
                        **{f"n_{k}": v for k, v in sorted(p["lifecycle_counts"].items())},
                        "temporal_sensitivity": ts,
                        "events": " | ".join(f"{o.event.kind}: {o.reason}" for o in outcomes),
                    })
                elif status == "error":
                    row["error"] = proc.stderr.strip().splitlines()[-1][:300] if proc.stderr else ""
                rows.append(row)
                print(f"   h={depth:>2}: {status} in {wall:.0f}s; rules={row.get('n_rules', '-')}; "
                      f"TS={row.get('temporal_sensitivity', '-')}", flush=True)
                if status != "ok":
                    print(f"   (skipping deeper closures for {n:,} entities)")
                    break

    args.out.parent.mkdir(exist_ok=True)
    fields = sorted({k for r in rows for k in r}, key=lambda k: (k not in ("entities", "statements",
                    "superclass_depth", "status", "wall_s"), k))
    with open(args.out, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    print(f"\nWrote {args.out}")


if __name__ == "__main__":
    main()
