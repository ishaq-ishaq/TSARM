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
