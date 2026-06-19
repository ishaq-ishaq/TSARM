#!/usr/bin/env python
"""Bridge RDFRules (AMIE+) batch mode to the TSARM benchmark harness.

RDFRules (Zeman, Kliegr & Svatek, 2021) mines Horn rules from RDF via a JSON
task pipeline run in batch mode (`sh bin/main task.json result.json`). This
wrapper builds that pipeline (LoadGraph -> Index -> Mine -> ComputeConfidence ->
ExportRules), runs it, and writes a simple `rules.csv` with `support` and
`confidence` columns into the output directory, which is exactly what
`src.evaluation.adapters.ExternalCommandBaseline` parses.

Wire it into the harness with::

    export RDFRULES_HOME=/path/to/rdfrules-1.9.0
    export RDFRULES_CMD='python3 scripts/rdfrules_mine.py --input {input} \\
        --output {output} --min-support {min_support} --min-confidence {min_confidence}'

Semantic note: RDFRules/AMIE thresholds are not identical to TSARM's relative
support. We map ``--min-support`` to AMIE's MinHeadCoverage (a [0,1] relative
measure) and ``--min-confidence`` to the standard (CWA) confidence minimum. The
comparison is therefore on rule count / runtime, not on identical rule
semantics (AMIE mines logical Horn rules; TSARM mines transactional itemset
rules).
"""

import argparse
import csv
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


def build_task(graph_names, export_path, min_head_coverage, min_confidence, max_len):
    """Construct the RDFRules JSON task pipeline.

    ``graph_names`` and ``export_path`` are paths relative to the RDFRules
    workspace directory (RDFRules resolves all paths against its workspace).
    """
    pipeline = []
    for name in graph_names:
        pipeline.append({"name": "LoadGraph", "parameters": {"path": name}})
    if len(graph_names) > 1:
        pipeline.append({"name": "MergeDatasets", "parameters": None})
    pipeline += [
        {"name": "Index", "parameters": {}},
        {
            "name": "Mine",
            "parameters": {
                "thresholds": [
                    {"name": "MinHeadSize", "value": 1},
                    {"name": "MinHeadCoverage", "value": min_head_coverage},
                    {"name": "MaxRuleLength", "value": max_len},
                ],
                "constraints": [],
                "patterns": [],
            },
        },
        {"name": "ComputeConfidence", "parameters": {"name": "StandardConfidence", "min": min_confidence}},
        {
            "name": "ExportRules",
            "parameters": {"path": export_path, "format": "json"},
        },
    ]
    return pipeline


def parse_rfrules_export(export_file):
    """Read RDFRules exported rules (JSON array or NDJSON) -> list of dicts."""
    text = Path(export_file).read_text(encoding="utf-8").strip()
    if not text:
        return []
    try:
        data = json.loads(text)
        return data if isinstance(data, list) else [data]
    except json.JSONDecodeError:
        # NDJSON: one rule object per line.
        out = []
        for line in text.splitlines():
            line = line.strip()
            if line:
                out.append(json.loads(line))
        return out


def extract_measures(rule):
    """Pull confidence and a relative support from an RDFRules rule object.

    RDFRules stores ``measures`` as a list of ``{name, value}``. Confidence is
    ``CwaConfidence`` (standard CWA confidence); for support we use
    ``HeadCoverage`` -- a [0, 1] relative measure comparable to TSARM's relative
    support (RDFRules' ``Support`` is an absolute count).
    """
    measures = rule.get("measures", rule)
    by_name = {}
    if isinstance(measures, list):
        by_name = {(m.get("name") or "").lower(): m.get("value") for m in measures}
    elif isinstance(measures, dict):
        by_name = {k.lower(): v for k, v in measures.items()}

    conf = by_name.get("cwaconfidence")
    if conf is None:
        conf = by_name.get("confidence") or by_name.get("pcaconfidence")
    supp = by_name.get("headcoverage")
    if supp is None:
        supp = by_name.get("support")
    return conf, supp


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", nargs="+", required=True, help="one or more RDF paths")
    ap.add_argument("--output", required=True, help="output dir for rules.csv")
    ap.add_argument("--min-support", type=float, default=0.01)
    ap.add_argument("--min-confidence", type=float, default=0.5)
    ap.add_argument("--max-len", type=int, default=3)
    ap.add_argument("--home", default=os.environ.get("RDFRULES_HOME"))
    args = ap.parse_args()

    if not args.home:
        sys.exit("Set RDFRULES_HOME (or --home) to the unpacked rdfrules folder.")
    main_script = Path(args.home) / "bin" / "main"
    if not main_script.exists():
        sys.exit(f"RDFRules launcher not found: {main_script}")

    input_paths = args.input
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    # RDFRules resolves every path against its workspace dir, so we point the
    # workspace at a temp dir, symlink the inputs into it, and use relative
    # names. The export also lands in the workspace (its root, ".", is writable).
    with tempfile.TemporaryDirectory(prefix="rdfrules_ws_") as ws:
        ws_dir = Path(ws)
        graph_names = []
        for i, p in enumerate(input_paths):
            src = Path(p).resolve()
            link = ws_dir / f"input_{i}{''.join(src.suffixes)}"
            link.symlink_to(src)
            graph_names.append(link.name)

        export_name = "rules_export.json"
        task = build_task(
            graph_names,
            export_name,
            min_head_coverage=args.min_support,
            min_confidence=args.min_confidence,
            max_len=args.max_len,
        )
        task_file = ws_dir / "task.json"
        result_file = ws_dir / "result.json"
        task_file.write_text(json.dumps(task, indent=2), encoding="utf-8")

        env = dict(os.environ, RDFRULES_WORKSPACE=str(ws_dir))
        proc = subprocess.run(
            ["sh", str(main_script), str(task_file), str(result_file)],
            capture_output=True,
            text=True,
            cwd=args.home,
            env=env,
        )
        if proc.returncode != 0:
            sys.stderr.write(proc.stdout[-2000:] + "\n" + proc.stderr[-2000:])
            sys.exit(f"RDFRules batch run failed (exit {proc.returncode}).")

        # Rules may be exported to the file, or returned in result.json.
        rules = []
        export_file = ws_dir / export_name
        if export_file.exists():
            rules = parse_rfrules_export(export_file)
        if not rules and result_file.exists():
            try:
                res = json.loads(result_file.read_text(encoding="utf-8"))
                r = res.get("result") if isinstance(res, dict) else res
                if isinstance(r, list):
                    rules = r
            except json.JSONDecodeError:
                pass

    csv_path = out_dir / "rules.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["support", "confidence"])
        for rule in rules:
            conf, supp = extract_measures(rule)
            writer.writerow([supp if supp is not None else "", conf if conf is not None else ""])

    print(f"RDFRules: wrote {len(rules)} rules to {csv_path}")


if __name__ == "__main__":
    main()
