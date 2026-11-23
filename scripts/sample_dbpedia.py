#!/usr/bin/env python3
"""Deterministic entity sample of DBpedia releases, for baselines that cannot
load the full releases.

An entity (subject IRI) is kept when ``crc32(iri) % modulus == 0``, so the same
entities are kept in every release and their type changes stay visible. The
output files keep the input names (``dbpedia_<YYYY-MM>_instance_types_en.ttl.bz2``),
so ``scripts/run_dbpedia_temporal.py --raw-dir`` and the baseline wrappers read
them unchanged::

    python3 scripts/sample_dbpedia.py --modulus 20 --out data/processed/dbpedia_sample5
"""
import argparse
import bz2
import json
import re
import zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_SUBJECT = re.compile(r"^<([^>]+)>")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--raw-dir", type=Path, default=ROOT / "data" / "raw")
    ap.add_argument("--releases", default="2015-10,2016-04,2016-10")
    ap.add_argument("--modulus", type=int, default=20, help="keep 1 entity in MODULUS (20 = 5%%)")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    meta = {"modulus": args.modulus, "rule": "crc32(subject IRI) % modulus == 0", "releases": {}}
    for rel in [r.strip() for r in args.releases.split(",") if r.strip()]:
        name = f"dbpedia_{rel}_instance_types_en.ttl.bz2"
        kept = total = 0
        entities = set()
        with bz2.open(args.raw_dir / name, "rt", encoding="utf-8") as src, \
                bz2.open(args.out / name, "wt", encoding="utf-8") as dst:
            for line in src:
                m = _SUBJECT.match(line)
                if not m:
                    continue
                total += 1
                iri = m.group(1)
                if zlib.crc32(iri.encode("utf-8")) % args.modulus == 0:
                    dst.write(line)
                    kept += 1
                    entities.add(iri)
        meta["releases"][rel] = {"triples_in": total, "triples_kept": kept, "entities_kept": len(entities)}
        print(f"{rel}: kept {kept:,} of {total:,} triples ({len(entities):,} entities)")
    (args.out / "sample.json").write_text(json.dumps(meta, indent=2))


if __name__ == "__main__":
    main()
