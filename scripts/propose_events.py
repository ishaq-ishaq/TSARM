#!/usr/bin/env python3
"""E4 on real data: propose candidate events, then score TSARM on the confirmed ones.

Step 1, propose (Spark; reads the extracted snapshots, not TSARM's output)::

    python3 scripts/propose_events.py propose --data-dir data/processed/wikidata \\
        --min-entities 1000 --min-share 0.2 --out results/events/wikidata_candidates.csv

Step 2, review by hand: for each row, check the change in Wikidata (links
column; the class's history, the bot-request archive) and write ``1`` in
``confirmed`` with the evidence in ``source``. Hand-found events can be added as
extra rows. See docs/experiments.md (E4 on Wikidata).

Step 3, score a TSARM run on the confirmed events::

    python3 scripts/propose_events.py score --events results/events/wikidata_candidates.csv \\
        --report results/wikidata_temporal_rules_h1.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.evaluation.events import (  # noqa: E402
    load_events,
    retyping_candidates,
    trajectories_from_report,
    window_times_from_report,
    write_candidates,
)
from src.evaluation.quality import evaluate_events  # noqa: E402


def cmd_propose(args) -> None:
    from src.ingestion.spark_session import get_spark
    from src.ingestion.wikidata import load_snapshots, snapshot_ts_from_name

    files = sorted(Path(args.data_dir).glob("*.tsv.gz"))
    if args.snapshots:
        wanted = args.snapshots.split(",")
        files = [f for f in files if f.name.split(".")[0] in wanted]
    if len(files) < 2:
        sys.exit(f"Need at least two snapshots in {args.data_dir}")
    snaps = {f.name.split(".")[0]: (f, snapshot_ts_from_name(f.name)) for f in files}
    spark = get_spark(app_name="TSARM-propose-events", master=args.master)
    stmts = load_snapshots(spark, snaps, sample_fraction=args.sample)
    rows = retyping_candidates(stmts, min_entities=args.min_entities, min_share=args.min_share).limit(args.top).collect()
    out = write_candidates(rows, args.out)
    print(f"wrote {out}: {len(rows)} candidates over {len(snaps)} snapshots")
    spark.stop()


def cmd_score(args) -> None:
    events = load_events(args.events, confirmed_only=not args.all)
    if not events:
        sys.exit("No confirmed events (fill the 'confirmed' column, or pass --all to score every candidate).")
    report = json.loads(Path(args.report).read_text())
    ts, outcomes = evaluate_events(
        events, trajectories_from_report(report), window_times_from_report(report), epsilon=args.epsilon
    )
    for o in outcomes:
        mark = "OK  " if o.detected else "MISS"
        print(f"{mark} {o.event.kind:9s} {o.event.when:%Y-%m} {o.event.rule_key[:90]}  {o.reason}")
    print(f"TS = {ts:.3f} ({sum(o.detected for o in outcomes)}/{len(outcomes)})")
    if args.out:
        Path(args.out).write_text(json.dumps({
            "ts": ts, "n_events": len(outcomes), "epsilon": args.epsilon, "report": args.report,
            "events": [{"rule_key": o.event.rule_key, "kind": o.event.kind, "when": o.event.when.isoformat(),
                        "conf_before": o.conf_before, "conf_after": o.conf_after,
                        "detected": o.detected, "reason": o.reason} for o in outcomes],
        }, indent=2))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("propose")
    p.add_argument("--data-dir", required=True)
    p.add_argument("--snapshots", help="comma-separated snapshot names (default: all files)")
    p.add_argument("--sample", type=float, help="fraction of entities (same hash sample as the driver)")
    p.add_argument("--min-entities", type=int, default=1000)
    p.add_argument("--min-share", type=float, default=0.2)
    p.add_argument("--top", type=int, default=500)
    p.add_argument("--master", default="local[*]")
    p.add_argument("--out", required=True)
    p.set_defaults(func=cmd_propose)
    s = sub.add_parser("score")
    s.add_argument("--events", required=True)
    s.add_argument("--report", required=True, help="TSARM JSON export from run_wikidata_temporal.py")
    s.add_argument("--epsilon", type=float, default=0.05)
    s.add_argument("--all", action="store_true", help="score unconfirmed candidates too (pilot only)")
    s.add_argument("--out")
    s.set_defaults(func=cmd_score)
    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
