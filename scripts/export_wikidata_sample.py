#!/usr/bin/env python
"""Write a Wikidata entity sample as N-Triples, for the baseline systems.

Uses the same loader and the same hash sample as
``scripts/run_wikidata_temporal.py --sample F`` (best-rank P31 statements of the
entities whose IRI hash falls in the sample), so AMIE+ and RDFRules receive
exactly the instance statements TSARM mines. One gzipped N-Triples file per
snapshot is written to ``--out``; the baseline wrappers pool them::

    python3 scripts/export_wikidata_sample.py --sample 0.001 --out results/wikidata_sample_0001
"""

from __future__ import annotations

import argparse
import gzip
import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from pyspark.sql import functions as F  # noqa: E402

from run_wikidata_temporal import DEFAULT_DIR, find_snapshots  # noqa: E402
from src.ingestion.spark_session import get_spark  # noqa: E402
from src.ingestion.wikidata import DIRECT_NS, load_snapshots  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--data-dir", type=Path, default=DEFAULT_DIR)
    ap.add_argument("--snapshots", help="comma-separated subset (default: all)")
    ap.add_argument("--sample", type=float, required=True, help="fraction of entities, e.g. 0.001")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    wanted = [s.strip() for s in args.snapshots.split(",")] if args.snapshots else None
    snapshots = find_snapshots(args.data_dir.resolve(), wanted)
    args.out.mkdir(parents=True, exist_ok=True)
    spark = get_spark(app_name="TSARM-wikidata-sample-export")
    counts = {}
    try:
        df = load_snapshots(spark, snapshots, sample_fraction=args.sample)
        lines = df.where(F.col("predicate") == DIRECT_NS + "P31").select(
            "snapshot",
            F.concat(F.lit("<"), "subject", F.lit("> <"), "predicate", F.lit("> <"), "object", F.lit("> .")).alias("value"),
        )
        for snap in snapshots:
            tmp = args.out / f".{snap}.parts"
            part = lines.where(F.col("snapshot") == snap).select("value")
            part.coalesce(1).write.mode("overwrite").text(str(tmp), compression="gzip")
            target = args.out / f"wikidata_{snap}_P31_sample.nt.gz"
            with open(target, "wb") as dst:
                for f in sorted(tmp.glob("part-*")):
                    with open(f, "rb") as src:
                        shutil.copyfileobj(src, dst)
            shutil.rmtree(tmp)
            with gzip.open(target, "rt", encoding="utf-8") as fh:
                counts[snap] = sum(1 for _ in fh)
            print(f"{snap}: {counts[snap]:,} P31 statements -> {target}")
    finally:
        spark.stop()
    (args.out / "sample.json").write_text(json.dumps(
        {"sample_fraction": args.sample, "hash": "pmod(xxhash64(subject), 1e6) < fraction*1e6",
         "statements": "best-rank P31", "per_snapshot": counts}, indent=2))


if __name__ == "__main__":
    main()
