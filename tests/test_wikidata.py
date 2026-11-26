"""Tests for streaming Wikidata extraction (class statements + time qualifiers)."""

import gzip
import io
import json
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from xml.sax.saxutils import escape

import pytest

from src.ingestion.wikidata import (
    DIRECT_NS,
    ENTITY_NS,
    extract_entity,
    iter_json_dump,
    iter_xml_dump,
    normalize_time,
    parse_entity_text,
)

ROOT = Path(__file__).resolve().parent.parent


def _time(t, precision=11):
    return {"time": t, "precision": precision, "timezone": 0, "calendarmodel": "x"}


def _statement(prop, target, rank="normal", qualifiers=None, numeric_only=False):
    value = {"entity-type": "item", "numeric-id": int(target[1:])}
    if not numeric_only:
        value["id"] = target
    st = {
        "mainsnak": {
            "snaktype": "value",
            "property": prop,
            "datavalue": {"value": value, "type": "wikibase-entityid"},
        },
        "type": "statement",
        "rank": rank,
    }
    if qualifiers:
        st["qualifiers"] = {
            p: [{"snaktype": "value", "property": p, "datavalue": {"value": v, "type": "time"}}]
            for p, v in qualifiers.items()
        }
    return st


# Douglas Adams-style item: two P31 statements (one preferred), one with dates.
ENTITY_A = {
    "type": "item",
    "id": "Q42",
    "claims": {
        "P31": [
            _statement("P31", "Q5", rank="preferred"),
            _statement(
                "P31",
                "Q36180",
                qualifiers={"P580": _time("+1979-00-00T00:00:00Z", 9), "P582": _time("+2001-05-11T00:00:00Z")},
            ),
            _statement("P31", "Q999", rank="deprecated"),
            {"mainsnak": {"snaktype": "somevalue", "property": "P31"}, "rank": "normal"},
        ],
        "P106": [_statement("P106", "Q214917")],  # not extracted by default
    },
}
# Old-format class item: numeric-id only, P279 with a point in time.
ENTITY_B = {
    "type": "item",
    "id": "Q5",
    "claims": {
        "P279": [
            _statement(
                "P279", "Q154954", numeric_only=True, qualifiers={"P585": _time("-0500-00-00T00:00:00Z", 7)}
            )
        ]
    },
}
PROPERTY_ENTITY = {"type": "property", "id": "P31", "claims": {}}


def test_normalize_time():
    assert normalize_time(_time("+2001-05-11T00:00:00Z")) == ("2001-05-11", 11)
    assert normalize_time(_time("+1979-00-00T00:00:00Z", 9)) == ("1979-01-01", 9)
    assert normalize_time(_time("-0500-00-00T00:00:00Z", 7)) == ("-0500-01-01", 7)
    assert normalize_time({"time": "garbage"}) == (None, None)
    assert normalize_time(None) == (None, None)


def test_extract_entity_ranks_and_qualifiers():
    rows = extract_entity(ENTITY_A)
    by_obj = {r[2].rsplit("/", 1)[-1]: r for r in rows}
    # somevalue skipped; P106 not requested.
    assert set(by_obj) == {"Q5", "Q36180", "Q999"}
    q5, writer, dep = by_obj["Q5"], by_obj["Q36180"], by_obj["Q999"]
    assert q5[0] == ENTITY_NS + "Q42" and q5[1] == DIRECT_NS + "P31"
    # Preferred exists -> only it is best-rank; deprecated never is.
    assert (q5[3], q5[4]) == ("preferred", True)
    assert (writer[3], writer[4]) == ("normal", False)
    assert (dep[3], dep[4]) == ("deprecated", False)
    # start / end qualifiers normalised with precision
    assert writer[5:9] == ("1979-01-01", 9, "2001-05-11", 11)
    assert writer[9:] == (None, None)


def test_extract_entity_old_format_and_point_in_time():
    (row,) = extract_entity(ENTITY_B)
    assert row[0] == ENTITY_NS + "Q5" and row[1] == DIRECT_NS + "P279"
    assert row[2] == ENTITY_NS + "Q154954"  # built from numeric-id
    assert row[4] is True  # sole normal statement -> best-rank
    assert row[9:] == ("-0500-01-01", 7)


def test_non_items_and_bad_json_yield_nothing():
    assert extract_entity(PROPERTY_ENTITY) == []
    assert parse_entity_text("{not json") == []
    assert parse_entity_text("[") == []
    assert len(parse_entity_text(json.dumps(ENTITY_A) + ",")) == 3


def test_extra_properties():
    rows = extract_entity(ENTITY_A, properties=("P106",))
    assert [r[2] for r in rows] == [ENTITY_NS + "Q214917"]


def _json_dump(entities):
    lines = ["["] + [json.dumps(e) + "," for e in entities[:-1]] + [json.dumps(entities[-1]), "]"]
    return "\n".join(lines) + "\n"


def _xml_dump(entities):
    pages = []
    for i, e in enumerate(entities):
        ns = "120" if e.get("type") == "property" else "0"
        pages.append(
            f"<page><title>{e['id']}</title><ns>{ns}</ns><id>{i}</id>"
            f"<revision><id>{i}</id><model>wikibase-item</model>"
            f'<text xml:space="preserve">{escape(json.dumps(e))}</text></revision></page>'
        )
    pages.append("<page><title>Talk</title><ns>1</ns><revision><text>hello</text></revision></page>")
    return (
        '<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.10/" version="0.10">'
        "<siteinfo><sitename>Wikidata</sitename></siteinfo>" + "".join(pages) + "</mediawiki>"
    )


def test_json_and_xml_readers_agree():
    entities = [ENTITY_A, PROPERTY_ENTITY, ENTITY_B]
    from_json = list(iter_json_dump(io.StringIO(_json_dump(entities))))
    from_xml = list(iter_xml_dump(io.BytesIO(_xml_dump(entities).encode())))
    assert len(from_json) == 3  # the JSON dump includes the property entity
    assert len(from_xml) == 2  # property page is namespace 120 -> skipped
    rows_json = [r for t in from_json for r in parse_entity_text(t)]
    rows_xml = [r for t in from_xml for r in parse_entity_text(t)]
    assert rows_json == rows_xml and len(rows_xml) == 4


@pytest.mark.parametrize("fmt", ["json", "xml"])
def test_extract_script_end_to_end(tmp_path, fmt):
    entities = [ENTITY_A, PROPERTY_ENTITY, ENTITY_B]
    if fmt == "json":
        src = tmp_path / "dump-all.json.gz"
        with gzip.open(src, "wt") as fh:
            fh.write(_json_dump(entities))
    else:
        src = tmp_path / "dump-pages-articles.xml"
        src.write_text(_xml_dump(entities))
    out = tmp_path / "out.tsv.gz"
    proc = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "extract_wikidata.py"),
         "--input", str(src), "--out", str(out), "--workers", "2"],
        capture_output=True, text=True, check=True,
    )
    assert "4 statements" in proc.stdout
    with gzip.open(out, "rt") as fh:
        lines = fh.read().splitlines()
    assert lines[0].startswith("subject\tpredicate\tobject\trank\tbest_rank")
    assert len(lines) == 5


def test_read_statements_into_spark(tmp_path):
    from src.ingestion.spark_session import get_spark
    from src.ingestion.wikidata import header, write_rows

    path = tmp_path / "s.tsv.gz"
    with gzip.open(path, "wt") as fh:
        fh.write(header())
        write_rows(extract_entity(ENTITY_A) + extract_entity(ENTITY_B), fh)
    spark = get_spark(app_name="TSARM-wikidata-tests", master="local[2]")
    try:
        from src.ingestion.wikidata import read_statements

        df = read_statements(spark, path, "2017-01-01", datetime(2017, 1, 1))
        rows = {r.object.rsplit("/", 1)[-1]: r for r in df.collect()}
        w = rows["Q36180"]
        assert w.object_kind == "iri" and w.snapshot == "2017-01-01"
        assert w.best_rank is False and w.rank == "normal"
        assert w.valid_from == date(1979, 1, 1) and w.valid_to == date(2001, 5, 11)
        assert rows["Q5"].best_rank is True and rows["Q5"].valid_from is None
        # BCE point in time: raw string kept, date null
        bce = rows["Q154954"]
        assert bce.point_in_time == "-0500-01-01" and bce.valid_from is None
    finally:
        spark.stop()


def test_read_statements_tolerates_impossible_dates(tmp_path):
    # Real Wikidata dumps contain impossible calendar dates (2022-01 has
    # 1973-04-31); they must become null, not fail the whole job.
    from src.ingestion.spark_session import get_spark
    from src.ingestion.wikidata import header, read_statements, write_rows

    W = "http://www.wikidata.org/entity/"
    P31 = "http://www.wikidata.org/prop/direct/P31"
    rows = [
        (W + "Q1", P31, W + "Q5", "normal", True, "1973-04-31", 11, "2001-02-29", 11, None, None),
        (W + "Q2", P31, W + "Q5", "normal", True, "2000-02-29", 11, None, None, "1999-13-01", 11),
    ]
    path = tmp_path / "s.tsv.gz"
    with gzip.open(path, "wt") as fh:
        fh.write(header())
        write_rows(rows, fh)
    spark = get_spark(app_name="TSARM-wikidata-tests", master="local[2]")
    try:
        got = {r.subject.rsplit("/", 1)[-1]: r
               for r in read_statements(spark, path, "2022-01", datetime(2022, 1, 20)).collect()}
        assert got["Q1"].valid_from is None and got["Q1"].valid_to is None
        assert got["Q1"].start_time == "1973-04-31"  # raw string kept
        assert got["Q2"].valid_from == date(2000, 2, 29)  # a real leap day still parses
    finally:
        spark.stop()


# --- Snapshot loading and the end-to-end Wikidata driver --------------------


def _item(qid, p31=(), p279=(), ended=None):
    """Entity JSON; ``ended`` = {class: end date} adds P582 to that P31 statement."""
    ended = ended or {}
    claims = {}
    if p31:
        claims["P31"] = [
            _statement("P31", c, qualifiers={"P582": _time(f"+{ended[c]}T00:00:00Z")} if c in ended else None)
            for c in p31
        ]
    if p279:
        claims["P279"] = [_statement("P279", c) for c in p279]
    return {"type": "item", "id": qid, "claims": claims}


def _write_snapshot(path, entities):
    from src.ingestion.wikidata import header, write_rows

    with gzip.open(path, "wt") as fh:
        fh.write(header())
        for e in entities:
            write_rows(extract_entity(e), fh)


# Writers (Q100 ⊑ Q200 artist) who are award winners (Q300): all of them in
# 2017, half in 2018, two in 2019 -> the rule Q100 => Q300 decays.
def _series(tmp_path):
    hierarchy = [_item("Q100", p279=["Q200"]), _item("Q200", p279=["Q900"])]
    winners = {"2017-01": range(20), "2018-01": range(10), "2019-01": range(2)}
    for snap, keep in winners.items():
        ents = list(hierarchy)
        for i in range(20):
            ents.append(_item(f"Q{1000 + i}", p31=["Q100", "Q300"] if i in keep else ["Q100"]))
        for i in range(10):
            ents.append(_item(f"Q{2000 + i}", p31=["Q400"]))
        _write_snapshot(tmp_path / f"{snap}.tsv.gz", ents)
    return tmp_path


def test_snapshot_ts_from_name():
    from src.ingestion.wikidata import snapshot_ts_from_name

    assert snapshot_ts_from_name("2017-01.tsv.gz") == datetime(2017, 1, 1)
    assert snapshot_ts_from_name("2021-01-20") == datetime(2021, 1, 20)
    with pytest.raises(ValueError):
        snapshot_ts_from_name("latest.tsv.gz")


def test_load_snapshots_filters(tmp_path):
    from src.ingestion.spark_session import get_spark
    from src.ingestion.wikidata import load_snapshots

    _write_snapshot(tmp_path / "a.tsv.gz", [
        ENTITY_A,  # one preferred + one normal P31 -> only the preferred is best-rank
        _item("Q7", p31=["Q5", "Q6"], ended={"Q6": "2010-01-01"}),  # Q6 ended before the snapshot
        _item("Q8", p279=["Q9"]),
    ] + [_item(f"Q{100 + i}", p31=["Q5"]) for i in range(400)])
    snaps = {"2017-01": (tmp_path / "a.tsv.gz", datetime(2017, 1, 1))}
    spark = get_spark(app_name="TSARM-wikidata-tests", master="local[2]")
    try:
        def objs(df, subject):
            return {r.object.rsplit("/", 1)[-1] for r in df.where(df.subject == ENTITY_NS + subject).collect()}

        truthy = load_snapshots(spark, snaps)
        assert objs(truthy, "Q42") == {"Q5"}
        assert objs(load_snapshots(spark, snaps, best_rank_only=False), "Q42") == {"Q5", "Q36180"}
        assert objs(truthy, "Q7") == {"Q5", "Q6"}
        assert objs(load_snapshots(spark, snaps, valid_at_snapshot=True), "Q7") == {"Q5"}

        sampled = load_snapshots(spark, snaps, sample_fraction=0.25)
        n_p31 = sampled.where(sampled.predicate == DIRECT_NS + "P31").select("subject").distinct().count()
        assert 50 < n_p31 < 160  # ~25% of 402 instances
        assert objs(sampled, "Q8") == {"Q9"}  # hierarchy edges are never sampled away
        again = load_snapshots(spark, snaps, sample_fraction=0.25)
        assert {r.subject for r in sampled.collect()} == {r.subject for r in again.collect()}
        with pytest.raises(ValueError):
            load_snapshots(spark, snaps, sample_fraction=0)
    finally:
        spark.stop()


def test_wikidata_driver_end_to_end(tmp_path):
    (tmp_path / "wd").mkdir()
    data = _series(tmp_path / "wd")
    out = tmp_path / "results"
    proc = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "run_wikidata_temporal.py"),
         "--data-dir", str(data), "--out", str(out), "--tag", "test",
         "--min-support", "0.1", "--min-confidence", "0.3", "--master", "local[2]"],
        capture_output=True, text=True,
    )
    assert proc.returncode == 0, proc.stderr[-3000:]
    report = json.loads((out / "wikidata_temporal_rules_test.json").read_text())
    rules = {
        (tuple(_s.rsplit("/", 1)[-1] for _s in r["antecedent"]),
         tuple(_s.rsplit("/", 1)[-1] for _s in r["consequent"])): r
        for r in report["rules"]
    }
    writer_award = rules[(("Q100",), ("Q300",))]
    assert writer_award["confidence_series"] == pytest.approx([1.0, 0.5])
    assert writer_award["lifecycle"] == "decaying"
    # depth 1: the direct superclass Q200 is an item, Q900 (two hops) is not
    assert (("Q200",), ("Q300",)) in rules
    assert not any("Q900" in a + c for a, c in rules)
    assert (("Q100",), ("Q200",)) not in rules  # taxonomic rule pruned
    assert report["params"]["windows"] == [["2017-01", "2018-01"], ["2018-01", "2019-01"]]


def test_wikidata_driver_valid_time_windows(tmp_path):
    # One dump. Q100 members hold Q300 from 2000 to 2009, and three quarters of them
    # hold Q400 from 2010 on: over valid time, Q100 => Q300 decays and
    # Q100 => Q400 emerges, although the dump is a single snapshot.
    data = tmp_path / "wd"
    data.mkdir()
    ents = []
    for i in range(20):
        claims = [_statement("P31", "Q100"),
                  _statement("P31", "Q300", qualifiers={"P580": _time("+2000-01-01T00:00:00Z"),
                                                        "P582": _time("+2009-12-31T00:00:00Z")})]
        if i < 15:
            claims.append(_statement("P31", "Q400", qualifiers={"P580": _time("+2010-06-01T00:00:00Z")}))
        ents.append({"type": "item", "id": f"Q{1000 + i}", "claims": {"P31": claims}})
    _write_snapshot(data / "2026-11.tsv.gz", ents)
    out = tmp_path / "results"
    proc = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "run_wikidata_temporal.py"),
         "--data-dir", str(data), "--snapshots", "2026-11", "--valid-time", "2000-01,2015-01,60,60",
         "--superclass-depth", "0", "--out", str(out), "--tag", "vt",
         "--min-support", "0.1", "--min-confidence", "0.3", "--master", "local[2]"],
        capture_output=True, text=True,
    )
    assert proc.returncode == 0, proc.stderr[-3000:]
    report = json.loads((out / "wikidata_temporal_rules_vt.json").read_text())
    assert report["params"]["windows"] == [["2000-01-01", "2005-01-01"], ["2005-01-01", "2010-01-01"],
                                           ["2010-01-01", "2015-01-01"]]
    rules = {(r["antecedent"][0].rsplit("/", 1)[-1], r["consequent"][0].rsplit("/", 1)[-1]): r
             for r in report["rules"]}
    assert rules[("Q100", "Q300")]["confidence_series"] == pytest.approx([1.0, 1.0, 0.0])
    assert rules[("Q100", "Q300")]["lifecycle"] == "decaying"
    assert rules[("Q100", "Q400")]["confidence_series"] == pytest.approx([0.0, 0.0, 0.75])
    assert rules[("Q100", "Q400")]["lifecycle"] == "emerging"
