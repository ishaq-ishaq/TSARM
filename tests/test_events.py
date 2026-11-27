"""Tests for E4 candidate events from entity retyping."""

import csv
from datetime import datetime

import pytest

from src.evaluation.events import load_events, retyping_candidates, write_candidates
from src.evaluation.quality import evaluate_events, rule_key
from src.evaluation.synthetic_wikidata import generate
from src.ingestion.spark_session import get_spark
from src.ingestion.wikidata import load_snapshots, snapshot_ts_from_name

WD = "http://www.wikidata.org/entity/"
P31 = "http://www.wikidata.org/prop/direct/P31"


@pytest.fixture(scope="module")
def spark():
    s = get_spark(app_name="TSARM-events-tests", master="local[2]")
    yield s
    s.stop()


def _stmts(spark, rows):
    return spark.createDataFrame(
        [(s, P31, WD + o, snap, datetime.strptime(snap, "%Y-%m")) for s, o, snap in rows],
        "subject string, predicate string, object string, snapshot string, snapshot_ts timestamp",
    )


def test_gain_and_loss_on_persisting_entities(spark):
    rows = []
    for i in range(10):  # 10 entities typed X in both snapshots
        rows += [(f"e{i}", "X", "2020-01"), (f"e{i}", "X", "2021-01")]
        if i < 6:
            rows.append((f"e{i}", "Y", "2021-01"))  # 6 gain Y -> emergence X=>Y, share 0.6
        if i < 4:
            rows.append((f"e{i}", "Z", "2020-01"))  # 4 lose Z -> decay X=>Z, share 0.4
    rows += [(f"n{i}", "X", "2021-01") for i in range(50)]  # new entities: ignored
    rows += [(f"n{i}", "Y", "2021-01") for i in range(50)]
    got = {(r.kind, r.antecedent, r.consequent): (r.n_entities, r.n_antecedent)
           for r in retyping_candidates(_stmts(spark, rows), min_entities=3, min_share=0.3).collect()}
    assert got[("emergence", WD + "X", WD + "Y")] == (6, 10)
    assert got[("decay", WD + "X", WD + "Z")] == (4, 10)
    # Z itself persists for nobody, Y never existed before: no candidates with those antecedents
    assert all(k[1] == WD + "X" for k in got)


def test_recovers_planted_events_and_roundtrip(spark, tmp_path):
    files, planted, _ = generate(tmp_path / "wd", n_entities=6000, n_classes=300)
    snaps = {f.name.split(".")[0]: (f, snapshot_ts_from_name(f.name)) for f in files}
    cands = retyping_candidates(load_snapshots(spark, snaps), min_entities=50, min_share=0.5).collect()
    keys = {(r.kind, rule_key([r.antecedent], [r.consequent]), r.when) for r in cands}
    for ev in planted:
        assert (ev.kind, rule_key([ev.antecedent], [ev.consequent]), ev.when) in keys

    sheet = write_candidates(cands, tmp_path / "cands.csv")
    assert load_events(sheet) == []  # nothing confirmed yet
    text = sheet.read_text().splitlines()
    text = [text[0]] + [line.replace(",,,", ",1,bot request,", 1) if line.endswith(",,,") else line for line in text[1:]]
    sheet.write_text("\n".join(text) + "\n")
    events = load_events(sheet)
    assert len(events) == len(cands)
    # A trajectory that moves in the expected direction is detected.
    windows = {0: (datetime(2019, 1, 1), datetime(2019, 1, 1)), 1: (datetime(2020, 9, 1), datetime(2020, 9, 1))}
    traj = {e.rule_key: ({0: 0.9, 1: 0.1} if e.kind == "decay" else {0: 0.1, 1: 0.9}) for e in events}
    ts, _ = evaluate_events([e for e in events if e.when == datetime(2020, 9, 1)], traj, windows)
    assert ts == 1.0


def test_load_events_reads_excel_saved_sheet(tmp_path):
    """Excel's "CSV UTF-8" adds a byte-order mark and CRLF line ends; the first column is ``kind``."""
    path = tmp_path / "review.csv"
    rows = [{"kind": "emergence", "antecedent": "x", "consequent": "y", "when": "2018-01-01",
             "from_snapshot": "2017-01", "to_snapshot": "2018-01", "n_entities": 5, "n_antecedent": 6,
             "share": 0.8}]
    write_candidates(rows, path)
    with open(path, newline="", encoding="utf-8") as fh:
        filled = list(csv.DictReader(fh))
    filled[0].update(confirmed="1", source="diff")  # filled in by hand, then saved by Excel:
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:  # byte-order mark + CRLF
        w = csv.DictWriter(fh, fieldnames=list(filled[0]), lineterminator="\r\n")
        w.writeheader()
        w.writerows(filled)
    assert load_events(path, confirmed_only=False)[0].kind == "emergence"
    events = load_events(path)
    assert len(events) == 1 and events[0].kind == "emergence" and events[0].rule_key == rule_key(["x"], ["y"])
