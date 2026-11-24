#!/usr/bin/env python
"""Stream a Wikidata dump and keep class statements + temporal qualifiers.

Examples (from the project root)::

    # local file (format inferred from the name)
    python3 scripts/extract_wikidata.py \\
        --input wikidatawiki-20170101-pages-articles.xml.bz2 \\
        --out data/processed/wikidata/2017-01-01.tsv.gz

    # straight from the Internet Archive, never storing the raw dump
    # (lbzip2 = parallel bzip2: `sudo apt install lbzip2` on the Oracle VM)
    curl -sL https://archive.org/download/wikidatawiki-20170101/wikidatawiki-20170101-pages-articles.xml.bz2 \\
      | lbzip2 -dc \\
      | python3 scripts/extract_wikidata.py --input - --format xml \\
          --out data/processed/wikidata/2017-01-01.tsv.gz

Reading/decompression happens in the main process. Entity JSON parsing, the
expensive part, runs in ``--workers`` processes. Progress is printed every
million entities.
"""

from __future__ import annotations

import argparse
import gzip
import sys
import time
from functools import partial
from itertools import islice
from multiprocessing import Pool
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.ingestion.wikidata import (  # noqa: E402
    DEFAULT_PROPERTIES,
    header,
    iter_entities,
    iter_xml_raw,
    open_dump,
    parse_entity_text,
    write_rows,
    xml_text,
)

BATCH = 500


def _batches(it, size):
    while True:
        batch = list(islice(it, size))
        if not batch:
            return
        yield batch


def _parse_batch(texts, properties, raw_xml=False):
    """Rows of a batch of entity texts, and how many entities it held.

    With ``raw_xml`` the texts are still XML-escaped page contents
    (:func:`iter_xml_raw`): decoding them here, in the worker, keeps the
    single reading process from limiting the extraction."""
    rows = []
    n = 0
    for t in texts:
        if raw_xml:
            t = xml_text(t)
            if t is None:  # an item page whose text is not entity JSON
                continue
        n += 1
        rows.extend(parse_entity_text(t, properties))
    return n, rows


def _infer_format(path: str) -> str:
    if "pages-articles" in path or path.endswith((".xml", ".xml.bz2", ".xml.gz")):
        return "xml"
    return "json"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--input", required=True, help="dump path, or - for stdin")
    ap.add_argument("--format", choices=["json", "xml"], help="default: inferred from name")
    ap.add_argument("--out", required=True, help="output .tsv.gz")
    ap.add_argument("--properties", default=",".join(DEFAULT_PROPERTIES))
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--limit", type=int, default=0, help="stop after N entities (testing)")
    args = ap.parse_args()

    fmt = args.format or ("xml" if args.input == "-" else _infer_format(args.input))
    props = tuple(p.strip() for p in args.properties.split(",") if p.strip())
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    stream = open_dump(args.input)
    texts = iter_xml_raw(stream) if fmt == "xml" else iter_entities(stream, fmt)
    if args.limit:
        texts = islice(texts, args.limit)

    t0 = time.perf_counter()
    n_entities = n_rows = 0
    next_report = 1_000_000
    work = partial(_parse_batch, properties=props, raw_xml=fmt == "xml")
    with gzip.open(out_path, "wt", encoding="utf-8") as out, Pool(args.workers) as pool:
        out.write(header())
        for n_texts, rows in pool.imap(work, _batches(texts, BATCH), chunksize=4):
            n_entities += n_texts
            n_rows += write_rows(rows, out)
            if n_entities >= next_report:
                rate = n_entities / (time.perf_counter() - t0)
                print(f"{n_entities:,} entities, {n_rows:,} statements ({rate:,.0f} entities/s)", flush=True)
                next_report += 1_000_000

    secs = time.perf_counter() - t0
    print(
        f"done: {n_entities:,} entities -> {n_rows:,} statements "
        f"({', '.join(props)}) in {secs:,.0f}s -> {out_path}"
    )


if __name__ == "__main__":
    main()
