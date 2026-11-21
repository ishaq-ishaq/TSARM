#!/usr/bin/env python
"""Run AMIE (AMIE+ lineage, v3.5.1) on RDF inputs and write ``rules.csv``.

Bridges AMIE's command line to the benchmark harness's external-baseline
contract (``{input}`` files -> ``{output}`` dir containing a rules file), in the
same way ``scripts/rdfrules_mine.py`` does for RDFRules.

As the proposal requires (§3.7.2), the baseline receives the **pooled,
temporally undifferentiated** graph: all input snapshots are merged into one
TSV, with duplicate triples dropped. Literal-object triples are excluded, as in
TSARM's default item generation, so both systems see the same IRI graph.

Measures written to ``rules.csv``, aligned with the RDFRules wrapper:

* ``support``: AMIE *head coverage* (relative support of the rule),
* ``confidence``: AMIE *standard (closed-world) confidence*,
* ``pca_confidence``: AMIE's partial-completeness confidence (kept for
  reference).

Setup::

    # AMIE 3.5.1: https://github.com/dig-team/AMIE/releases (needs Java 8+)
    export AMIE_JAR=$PWD/tools/amie/amie3.5.1.jar
    export AMIE_CMD='python3 scripts/amie_mine.py --input {input} --output {output} \\
        --min-support {min_support} --min-confidence {min_confidence}'
"""

from __future__ import annotations

import argparse
import bz2
import csv
import gzip
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_JAR = ROOT / "tools" / "amie" / "amie3.5.1.jar"

# <s> <p> <o> .   (IRI objects only; literals and blank nodes skipped)
_NT_IRI = re.compile(r"^\s*(<[^>]*>)\s+(<[^>]*>)\s+(<[^>]*>)\s*\.\s*$")


def _open_text(path: str):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    if path.endswith(".bz2"):
        return bz2.open(path, "rt", encoding="utf-8", errors="replace")
    return open(path, encoding="utf-8", errors="replace")


def pool_to_tsv(inputs, tsv_path: Path) -> int:
    """Merge N-Triples inputs into one de-duplicated TSV; returns triple count."""
    seen = set()
    with open(tsv_path, "w", encoding="utf-8") as out:
        for path in inputs:
            with _open_text(str(path)) as fh:
                for line in fh:
                    m = _NT_IRI.match(line)
                    if not m:
                        continue
                    triple = m.groups()
                    if triple in seen:
                        continue
                    seen.add(triple)
                    out.write("\t".join(triple) + "\n")
    return len(seen)


def parse_amie_output(text: str):
    """Rules from AMIE stdout: header row ``Rule\\tHead Coverage\\t...`` then rows."""
    rules = []
    columns = None
    for line in text.splitlines():
        if line.startswith("Rule\t"):
            columns = [c.strip().lower() for c in line.split("\t")]
            continue
        if columns is None or "=>" not in line:
            continue
        cells = line.split("\t")
        row = dict(zip(columns, cells))

        def num(name):
            try:
                return float(row.get(name, ""))
            except ValueError:
                return None

        rules.append(
            {
                "rule": cells[0].strip(),
                "support": num("head coverage"),
                "confidence": num("std confidence"),
                "pca_confidence": num("pca confidence"),
                "positive_examples": num("positive examples"),
            }
        )
    return rules


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--input", nargs="+", required=True, help="N-Triples files (pooled)")
    ap.add_argument("--output", required=True, help="output dir for rules.csv")
    ap.add_argument("--min-support", type=float, default=0.01, help="min head coverage")
    ap.add_argument("--min-confidence", type=float, default=0.1, help="min std confidence")
    ap.add_argument("--max-atoms", type=int, default=3, help="max rule length (AMIE -maxad)")
    ap.add_argument("--threads", type=int, default=os.cpu_count() or 4)
    ap.add_argument("--jar", default=os.environ.get("AMIE_JAR", str(DEFAULT_JAR)))
    ap.add_argument("--java", default=None, help="java binary (default: $JAVA_HOME/bin/java)")
    args = ap.parse_args()

    jar = Path(args.jar)
    if not jar.exists():
        sys.exit(f"AMIE jar not found: {jar} (set $AMIE_JAR or --jar)")
    java = args.java or (
        str(Path(os.environ["JAVA_HOME"]) / "bin" / "java") if os.environ.get("JAVA_HOME") else "java"
    )

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="amie_in_") as tmp:
        tsv = Path(tmp) / "pooled.tsv"
        n = pool_to_tsv(args.input, tsv)
        cmd = [
            java,
            "-jar",
            str(jar),
            "-minhc", str(args.min_support),
            "-minc", str(args.min_confidence),
            "-maxad", str(args.max_atoms),
            "-nc", str(args.threads),
            str(tsv),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr[-2000:])
        sys.exit(proc.returncode)

    rules = parse_amie_output(proc.stdout)
    with open(out_dir / "rules.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["rule", "support", "confidence", "pca_confidence", "positive_examples"])
        for r in rules:
            w.writerow([r["rule"], r["support"], r["confidence"], r["pca_confidence"], r["positive_examples"]])
    (out_dir / "amie_stdout.log").write_text(proc.stdout)
    print(f"AMIE: {n:,} pooled triples -> {len(rules):,} rules -> {out_dir / 'rules.csv'}")


if __name__ == "__main__":
    main()
