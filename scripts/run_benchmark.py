#!/usr/bin/env python
"""Run the TSARM vs SANSA vs RDFRules vs AMIE+ comparison on the sample dataset.

TSARM runs in-process. SANSA, RDFRules and AMIE+ run only if their command templates
are configured via ``$SANSA_CMD`` / ``$RDFRULES_CMD`` / ``$AMIE_CMD`` (see
``src/evaluation/adapters.py``); otherwise they are reported as skipped.

Usage (from the project root)::

    export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
    python3 scripts/run_benchmark.py
"""

import argparse
import sys
import tempfile
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.evaluation.adapters import (  # noqa: E402
    BenchmarkResult,
    Dataset,
    Snapshot,
    TSARMBaseline,
    amie_baseline,
    rdfrules_baseline,
    sansa_baseline,
)
from src.evaluation.benchmark import run_benchmark  # noqa: E402
from src.evaluation.report import temporal_addendum, to_csv, to_markdown  # noqa: E402
from src.evaluation.synthetic import generate_dataset  # noqa: E402
from src.ingestion.spark_session import get_spark  # noqa: E402

RAW = ROOT / "data" / "raw"
RESULTS = ROOT / "results"

# Default thresholds (overridable via CLI). Lower for synthetic so both miners
# find the planted patterns; the tiny sample needs higher support.
DEFAULTS = {"sample": (0.5, 0.5), "synthetic": (0.05, 0.3)}


def sample_dataset() -> Dataset:
    return Dataset(
        name="sample",
        snapshots=[
            Snapshot("2023", RAW / "sample_2023.nt", datetime(2023, 1, 1)),
            Snapshot("2024", RAW / "sample_2024.nt", datetime(2024, 1, 1)),
        ],
    )


def snapshots_dataset(directory: Path, name: str) -> Dataset:
    """One N-Triples file per snapshot, the snapshot date (YYYY-MM) in the file name."""
    import re

    pat = re.compile(r"(?<!\d)(\d{4})-(\d{2})(?!\d)")
    snaps = []
    for f in sorted(directory.iterdir()):
        m = pat.search(f.name)
        if m and (f.name.endswith(".nt") or f.name.endswith(".nt.gz")):
            snaps.append(Snapshot(f"{m.group(1)}-{m.group(2)}", f, datetime(int(m.group(1)), int(m.group(2)), 1)))
    if len(snaps) < 2:
        sys.exit(f"Need at least two snapshot files (…YYYY-MM….nt) in {directory}")
    return Dataset(name=name, snapshots=sorted(snaps, key=lambda s: s.timestamp))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--synthetic",
        type=int,
        metavar="N_ENTITIES",
        help="benchmark a generated synthetic dataset of this many entities "
        "instead of the tiny committed sample",
    )
    ap.add_argument(
        "--snapshots-dir",
        type=Path,
        help="benchmark the per-snapshot N-Triples files in this directory "
        "(e.g. a Wikidata sample export); requires --name, --min-support, --min-confidence",
    )
    ap.add_argument("--name", help="dataset name for --snapshots-dir (used in the output file names)")
    ap.add_argument("--min-support", type=float)
    ap.add_argument("--min-confidence", type=float)
    ap.add_argument(
        "--repeats",
        type=int,
        default=1,
        help="measured repetitions per system (proposal: 3). With >1, one extra "
        "warm-up round is discarded and TSARM is compared with each baseline "
        "using the Table 3.4 tests",
    )
    ap.add_argument(
        "--timeout",
        type=int,
        default=3600,
        help="wall-clock limit per external-baseline run, in seconds (default 3600)",
    )
    ap.add_argument("--width", type=int, default=1, help="TSARM window width in snapshots (default 1)")
    args = ap.parse_args()

    if args.snapshots_dir:
        if not (args.name and args.min_support and args.min_confidence):
            sys.exit("--snapshots-dir needs --name, --min-support and --min-confidence")
        mode = args.name
        min_support, min_confidence = args.min_support, args.min_confidence
    else:
        mode = "synthetic" if args.synthetic else "sample"
        min_support = args.min_support or DEFAULTS[mode][0]
        min_confidence = args.min_confidence or DEFAULTS[mode][1]

    tmp_ctx = tempfile.TemporaryDirectory(prefix="tsarm_bench_") if args.synthetic else None
    spark = get_spark(app_name="TSARM-benchmark")
    try:
        if args.synthetic:
            dataset, n = generate_dataset(
                Path(tmp_ctx.name),
                name=f"synthetic-{args.synthetic}",
                n_entities=args.synthetic,
                n_snapshots=4,
            )
            print(f"Generated synthetic dataset: {n} triples, "
                  f"{len(dataset.snapshots)} snapshots.")
        elif args.snapshots_dir:
            dataset = snapshots_dataset(args.snapshots_dir.resolve(), args.name)
            print(f"Dataset {args.name}: {len(dataset.snapshots)} snapshots from {args.snapshots_dir}")
        else:
            dataset = sample_dataset()

        baselines = [
            TSARMBaseline(spark, min_support=min_support, min_confidence=min_confidence, width=args.width),
            sansa_baseline(min_support=min_support, min_confidence=min_confidence, timeout_sec=args.timeout),
            rdfrules_baseline(min_support=min_support, min_confidence=min_confidence, timeout_sec=args.timeout),
            amie_baseline(min_support=min_support, min_confidence=min_confidence, timeout_sec=args.timeout),
        ]
        timed_out = set()
        if args.repeats > 1:
            print("Warm-up round (discarded) ...", flush=True)
            _round(baselines, dataset, timed_out)
        rounds = [_round(baselines, dataset, timed_out) for _ in range(max(1, args.repeats))]
        results = rounds[-1]

        print("\n## Benchmark comparison\n")
        print(to_markdown(results))

        addendum = temporal_addendum(results)
        if addendum:
            print("\n" + addendum)

        RESULTS.mkdir(exist_ok=True)
        csv_path = RESULTS / f"benchmark_{mode}.csv"
        to_csv(results, csv_path)
        print(f"\nWrote {csv_path}")

        if args.repeats > 1:
            _write_repeats(rounds, mode)
    finally:
        spark.stop()
        if tmp_ctx is not None:
            tmp_ctx.cleanup()


def _round(baselines, dataset, timed_out: set):
    """One benchmark round. A system that has timed out once is not re-run:
    it is reported as ``timeout`` again, which saves hours on large inputs."""
    active = [b for b in baselines if b.name not in timed_out]
    results = run_benchmark(active, [dataset])
    for name in sorted(timed_out):
        results.append(
            BenchmarkResult(
                system=name,
                dataset=dataset.name,
                status="timeout",
                supports_temporal=False,
                note="not re-run: timed out in an earlier round",
            )
        )
    timed_out.update(r.system for r in results if r.status == "timeout")
    return results


def _write_repeats(rounds, mode: str) -> None:
    """Per-run CSV plus paired TSARM-vs-baseline runtime tests (Table 3.4)."""
    import csv

    from src.evaluation.stats import compare_paired

    runs_path = RESULTS / f"benchmark_{mode}_runs.csv"
    with open(runs_path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["repeat", "system", "status", "runtime_sec", "n_rules", "mean_confidence"])
        for i, rnd in enumerate(rounds, 1):
            for r in rnd:
                w.writerow([i, r.system, r.status, r.runtime_sec, r.n_rules, r.mean_confidence])
    print(f"Wrote {runs_path}")

    def runtimes(system):
        vals = [
            next((r.runtime_sec for r in rnd if r.system == system and r.status == "ok"), None)
            for rnd in rounds
        ]
        return vals if all(v is not None for v in vals) else None

    tsarm = runtimes("TSARM")
    systems = sorted({r.system for rnd in rounds for r in rnd} - {"TSARM"})
    rows = []
    for system in systems:
        base = runtimes(system)
        if tsarm is None or base is None:
            continue
        c = compare_paired(tsarm, base, metric=f"runtime_sec: TSARM vs {system}")
        rows.append(c.as_dict())
        print(
            f"\n{c.metric}: {c.mean_tsarm:.1f}s vs {c.mean_baseline:.1f}s | {c.test} "
            f"p={c.p_value if c.p_value is None else round(c.p_value, 4)} | "
            f"{c.effect_size_name}={c.effect_size:.2f}" + (f" | {c.note}" if c.note else "")
        )
    if rows:
        stats_path = RESULTS / f"benchmark_{mode}_stats.csv"
        with open(stats_path, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)
        print(f"Wrote {stats_path}")


if __name__ == "__main__":
    main()
