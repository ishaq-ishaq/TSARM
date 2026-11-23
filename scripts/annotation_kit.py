#!/usr/bin/env python3
"""E3 on real data: build a blind annotation sheet, then score the filled sheet.

Build a sheet from a TSARM export plus (optionally) AMIE+ and RDFRules output::

    python3 scripts/annotation_kit.py sheet \\
        --tsarm results/wikidata_temporal_rules_h1.json \\
        --amie results/amie_wikidata/rules.csv \\
        --rdfrules results/rdfrules_wikidata/rdfrules_export.json \\
        --sample 300 --fetch-labels --out annotation/wikidata_sheet.csv

Two annotators fill ``judge_a`` and ``judge_b`` (1 / 0 / ?) independently,
following docs/annotation_guidelines.md; disagreements are settled in
``final``. Then::

    python3 scripts/annotation_kit.py score annotation/wikidata_sheet.csv \\
        --reference-out annotation/wikidata_reference.csv

prints Cohen's kappa and per-system precision (Wilson 95 % CI) and relative
recall, writes the reference set, and a JSON summary next to the sheet.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.evaluation.annotation import (  # noqa: E402
    fetch_wikidata_labels,
    load_tsarm_export,
    pool_rules,
    rule_iris,
    score_sheet,
    stratified_sample,
    write_reference,
    write_sheet,
)
from src.evaluation.quality import amie_rule_key, rdfrules_rule_key  # noqa: E402


def _amie(path):
    with open(path, newline="") as fh:
        rows = list(csv.DictReader(fh))
    out = {}
    for r in rows:
        k = amie_rule_key(r["rule"])
        if k:
            out[k] = float(r["confidence"]) if r.get("confidence") else None
    return out, len(rows)


def _rdfrules(path):
    export = json.loads(Path(path).read_text())
    out = {}
    for rule in export:
        k = rdfrules_rule_key(rule)
        if k:
            out[k] = next((m["value"] for m in rule.get("measures", []) if m.get("name") == "CwaConfidence"), None)
    return out, len(export)


def cmd_sheet(args) -> None:
    tsarm = load_tsarm_export(args.tsarm) if args.tsarm else []
    baselines, totals = {}, {"tsarm": len(tsarm)}
    if args.amie:
        baselines["amie"], totals["amie"] = _amie(args.amie)
    if args.rdfrules:
        baselines["rdfrules"], totals["rdfrules"] = _rdfrules(args.rdfrules)
    pool = pool_rules(tsarm, baselines)
    sample = stratified_sample(pool, args.sample, seed=args.seed, min_per_stratum=args.min_per_stratum)
    labels = {}
    if args.fetch_labels:
        iris = {i for p in sample for i in rule_iris(p.key)}
        try:
            labels = fetch_wikidata_labels(iris)
        except OSError as exc:  # offline: the links column still works
            print(f"label fetch failed ({exc}); sheet written without labels", file=sys.stderr)
    out = write_sheet(sample, args.out, labels=labels, seed=args.seed)
    meta = {
        "sources": {"tsarm": args.tsarm, "amie": args.amie, "rdfrules": args.rdfrules},
        "rules_output": totals,
        "comparable": {"tsarm": len(tsarm), **{k: len(v) for k, v in baselines.items()}},
        "pool": len(pool), "sampled": len(sample), "seed": args.seed,
    }
    Path(str(out) + ".meta.json").write_text(json.dumps(meta, indent=2))
    print(f"wrote {out}: {len(sample)} of {len(pool)} pooled rules")


def cmd_score(args) -> None:
    s = score_sheet(args.sheet)
    print(f"rules {s.n_rules}; both decided {s.n_both_decided}; agreement {s.agreement:.3f}; "
          f"kappa {s.kappa:.3f}; disagreements {s.n_disagreements}; reference {len(s.reference)}")
    for system, m in s.per_system.items():
        print(f"  {system:9s} judged {m['judged']:4d}  P {m['precision']:.3f} "
              f"[{m['precision_ci_low']:.3f}, {m['precision_ci_high']:.3f}]  rel.recall {m['relative_recall']:.3f}")
    if args.reference_out:
        write_reference(s.reference, args.reference_out, note=f"agreed-valid rules from {args.sheet}")
        print(f"wrote {args.reference_out}")
    summary = Path(args.sheet).with_suffix(".score.json")
    summary.write_text(json.dumps({k: v for k, v in s.__dict__.items()}, indent=2))
    print(f"wrote {summary}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sh = sub.add_parser("sheet", help="pool and sample rules into an annotation sheet")
    sh.add_argument("--tsarm", help="TSARM JSON export")
    sh.add_argument("--amie", help="AMIE rules.csv (from scripts/amie_mine.py)")
    sh.add_argument("--rdfrules", help="RDFRules JSON export (from scripts/rdfrules_mine.py)")
    sh.add_argument("--sample", type=int, default=300)
    sh.add_argument("--min-per-stratum", type=int, default=5)
    sh.add_argument("--seed", type=int, default=42)
    sh.add_argument("--fetch-labels", action="store_true", help="add English Wikidata labels (needs internet)")
    sh.add_argument("--out", required=True)
    sh.set_defaults(func=cmd_sheet)
    sc = sub.add_parser("score", help="score a filled sheet")
    sc.add_argument("sheet")
    sc.add_argument("--reference-out")
    sc.set_defaults(func=cmd_score)
    args = ap.parse_args()
    if args.cmd == "sheet" and not (args.tsarm or args.amie or args.rdfrules):
        ap.error("give at least one of --tsarm, --amie, --rdfrules")
    args.func(args)


if __name__ == "__main__":
    main()
