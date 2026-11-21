#!/usr/bin/env python
"""Experiment E1: strong and weak scalability across CPU cores on one machine.

Implements the single-node part of the scalability evaluation (proposal
Sec. 3.7.3 i): one workstation, varying CPU cores. For each core count ``p`` the
full TSARM pipeline (ingest -> window -> mine + recount -> temporal metrics) is
run in Spark local mode with ``master = local[p]``.

* **Strong scaling**: fixed synthetic dataset; reports latency, speedup
  ``S_p = T_1 / T_p`` and efficiency ``E_p = S_p / p``.
* **Weak scaling**: dataset grows with ``p`` (``entities = base * p``); reports
  throughput, ``Th_p / Th_1`` and the per-worker weak-scaling efficiency.

Every configuration gets one discarded warm-up run (JVM/JIT start-up), then
``--repeats`` measured runs (proposal: 3). Shuffle partitions are held fixed
across ``p`` so every run does identical work. Outputs, tagged with a hardware
fingerprint, go to ``results/``::

    core_scaling_<mode>_runs.csv     raw per-run measurements
    core_scaling_<mode>_summary.csv  mean/SD/95% CI + scaling ratios
    core_scaling_<mode>.json         summary + hardware + parameters

Usage (from the project root)::

    export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
    python3 scripts/core_scaling.py --mode strong --entities 200000
    python3 scripts/core_scaling.py --mode weak --entities 20000

**Cluster mode (experiment E2)**: with ``--master-url spark://HOST:7077``, the
same experiment runs on a Spark standalone cluster. ``p`` then caps the
executor cores (``spark.cores.max``); with 1-core workers it equals the number
of worker nodes. There is no shared filesystem, so every node needs the input
files at the same path. Generate them first, on every node (the generator is
deterministic for a given seed, so the copies are identical)::

    python3 scripts/core_scaling.py --generate-only --data-dir ~/tsarm-data \
        --mode strong --cores 1,2,3,4 --entities 300000
    python3 scripts/core_scaling.py --master-url spark://10.0.0.10:7077 \
        --data-dir ~/tsarm-data --mode strong --cores 1,2,3,4 --entities 300000
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.evaluation.adapters import TSARMBaseline  # noqa: E402
from src.evaluation.scaling import (  # noqa: E402
    hardware_fingerprint,
    strong_scaling_table,
    throughput,
    weak_scaling_table,
)
from src.evaluation.synthetic import generate_dataset  # noqa: E402
from src.ingestion.spark_session import get_spark  # noqa: E402

RESULTS = ROOT / "results"
N_SNAPSHOTS = 4
MIN_SUPPORT = 0.05
MIN_CONFIDENCE = 0.5
SHUFFLE_PARTITIONS = 32  # fixed for all p (>= 2x the M5's 15 cores)


def _parse_cores(text: str):
    return sorted({int(x) for x in text.split(",") if x.strip()})


def _run_once(p: int, dataset, master_url=None, executor_memory="3g") -> dict:
    """One full pipeline run with ``p`` cores; returns runtime and sizes.

    Local mode uses ``local[p]``. Cluster mode connects to ``master_url`` and
    caps the application at ``p`` single-core executors.
    """
    config = {"spark.sql.shuffle.partitions": str(SHUFFLE_PARTITIONS)}
    if master_url:
        config.update(
            {
                "spark.cores.max": str(p),
                "spark.executor.cores": "1",
                "spark.executor.memory": executor_memory,
            }
        )
    spark = get_spark(
        app_name=f"TSARM-core-scaling-p{p}",
        master=master_url or f"local[{p}]",
        config=config,
    )
    try:
        result = TSARMBaseline(
            spark, min_support=MIN_SUPPORT, min_confidence=MIN_CONFIDENCE
        ).run(dataset)
    finally:
        # A fresh SparkContext per run, so local[p] actually changes.
        spark.stop()
    return {
        "runtime_sec": result.runtime_sec,
        "n_input_triples": result.n_input_triples,
        "n_rules": result.n_rules,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--mode", choices=["strong", "weak"], default="strong")
    ap.add_argument("--cores", default="1,2,4,8,15", help="comma-separated core counts")
    ap.add_argument(
        "--entities",
        type=int,
        default=100_000,
        help="entities per snapshot (strong: total; weak: per core)",
    )
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--master-url", default=None, help="spark://HOST:7077 for cluster mode (E2)")
    ap.add_argument("--executor-memory", default="3g", help="cluster mode only")
    ap.add_argument("--data-dir", default=None, help="persistent dataset dir (required for clusters)")
    ap.add_argument("--generate-only", action="store_true", help="write datasets to --data-dir and exit")
    ap.add_argument("--tag", default="", help="suffix for output file names (e.g. a re-run)")
    args = ap.parse_args()
    if (args.master_url or args.generate_only) and not args.data_dir:
        ap.error("--master-url and --generate-only need --data-dir (same path on every node)")

    cores = _parse_cores(args.cores)
    hw = hardware_fingerprint()
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    runs = []

    with tempfile.TemporaryDirectory(prefix="tsarm_core_scaling_") as tmp:
        tmp_dir = Path(args.data_dir).expanduser().resolve() if args.data_dir else Path(tmp)
        datasets = {}
        for p in cores:
            n = args.entities * (p if args.mode == "weak" else 1)
            if n not in datasets:
                ds, total = generate_dataset(
                    tmp_dir / f"n{n}",
                    name=f"synthetic-{n}",
                    n_entities=n,
                    n_snapshots=N_SNAPSHOTS,
                    seed=args.seed,
                )
                datasets[n] = (ds, total)
                print(f"generated {total:,} triples ({n:,} entities/snapshot) in {tmp_dir}")
        if args.generate_only:
            return

        for p in cores:
            n = args.entities * (p if args.mode == "weak" else 1)
            dataset, _ = datasets[n]
            print(f"\np={p}: warm-up ...", flush=True)
            _run_once(p, dataset, args.master_url, args.executor_memory)
            for rep in range(1, args.repeats + 1):
                t0 = time.perf_counter()
                m = _run_once(p, dataset, args.master_url, args.executor_memory)
                wall = time.perf_counter() - t0
                row = {
                    "mode": args.mode,
                    "p": p,
                    "repeat": rep,
                    "entities_per_snapshot": n,
                    "n_input_triples": m["n_input_triples"],
                    "runtime_sec": m["runtime_sec"],
                    "wall_sec_incl_session": wall,
                    "throughput_triples_per_sec": throughput(
                        m["n_input_triples"], m["runtime_sec"]
                    ),
                    "n_rules": m["n_rules"],
                }
                runs.append(row)
                print(
                    f"  run {rep}: {m['runtime_sec']:7.2f}s  "
                    f"{row['throughput_triples_per_sec']:10,.0f} triples/s  rules={m['n_rules']}",
                    flush=True,
                )

    if args.mode == "strong":
        summary = strong_scaling_table(
            {p: [r["runtime_sec"] for r in runs if r["p"] == p] for p in cores}
        )
    else:
        summary = weak_scaling_table(
            {p: [r["throughput_triples_per_sec"] for r in runs if r["p"] == p] for p in cores}
        )

    RESULTS.mkdir(exist_ok=True)
    stem = RESULTS / (
        (f"cluster_scaling_{args.mode}" if args.master_url else f"core_scaling_{args.mode}")
        + (f"_{args.tag}" if args.tag else "")
    )
    with open(f"{stem}_runs.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(runs[0]))
        w.writeheader()
        w.writerows(runs)
    with open(f"{stem}_summary.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(summary[0]))
        w.writeheader()
        w.writerows(summary)
    with open(f"{stem}.json", "w") as fh:
        json.dump(
            {
                "experiment": (
                    f"E2 cluster scaling ({args.mode})" if args.master_url else f"E1 core scaling ({args.mode})"
                ),
                "started_utc": started,
                "hardware": hw,
                "params": {
                    **vars(args),
                    "n_snapshots": N_SNAPSHOTS,
                    "min_support": MIN_SUPPORT,
                    "min_confidence": MIN_CONFIDENCE,
                    "shuffle_partitions": SHUFFLE_PARTITIONS,
                    "warmup_runs_discarded": 1,
                },
                "summary": summary,
            },
            fh,
            indent=2,
        )

    print(f"\n## {args.mode} scaling on {hw.get('cpu')} ({hw.get('memory_gb')} GB)\n")
    for row in summary:
        print(", ".join(f"{k}={v:.3f}" if isinstance(v, float) else f"{k}={v}" for k, v in row.items()))
    print(f"\nWrote {stem}_runs.csv, {stem}_summary.csv, {stem}.json")


if __name__ == "__main__":
    main()
