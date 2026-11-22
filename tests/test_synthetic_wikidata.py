"""Tests for the synthetic Wikidata-like snapshot generator (no Spark needed)."""

import collections
import gzip

from src.evaluation.synthetic_wikidata import A, B, C, D, SNAPSHOTS, generate
from src.ingestion.wikidata import COLUMNS, snapshot_ts_from_name


def _types(path):
    types = collections.defaultdict(set)
    with gzip.open(path, "rt") as fh:
        header = fh.readline().rstrip("\n").split("\t")
        for line in fh:
            s, p, o = line.split("\t")[:3]
            if p.endswith("/P31"):
                types[s].add(o.rsplit("/", 1)[1])
    return header, types


def test_format_and_planted_events(tmp_path):
    files, events, n_rows = generate(tmp_path, n_entities=20000, n_classes=1000)
    assert [f.name for f in files] == [f"{s}.tsv.gz" for s in SNAPSHOTS]
    assert all(snapshot_ts_from_name(f.name) for f in files)
    shares = []
    sizes = []
    for f in files:
        header, types = _types(f)
        assert tuple(header) == COLUMNS  # same format as scripts/extract_wikidata.py
        a = [t for t in types.values() if A in t]
        c = [t for t in types.values() if C in t]
        shares.append((sum(B in t for t in a) / len(a), sum(D in t for t in c) / len(c)))
        sizes.append(len(types))
    assert sizes == sorted(sizes) and sizes[-1] > 3 * sizes[0]  # growing population
    before, after = shares[:3], shares[3:]
    assert all(ab > 0.95 for ab, _ in before) and all(ab < 0.3 for ab, _ in after)  # A => B decays
    assert all(cd < 0.2 for _, cd in before) and all(cd > 0.8 for _, cd in after)  # C => D emerges
    assert {e.kind for e in events} == {"decay", "emergence"}
    assert all(e.when.year == 2020 for e in events)
    assert n_rows > 20000
