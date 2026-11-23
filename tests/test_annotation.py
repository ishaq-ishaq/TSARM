"""Tests for the E3 annotation kit (no Spark)."""

import csv
import json
import subprocess
import sys
from pathlib import Path

import pytest

from src.evaluation.annotation import (
    PooledRule,
    cohen_kappa,
    pool_rules,
    readable_rule,
    score_sheet,
    stratified_sample,
    wilson_interval,
    write_reference,
    write_retest_sheet,
    write_sheet,
)
from src.evaluation.quality import load_reference_rules

ROOT = Path(__file__).resolve().parents[1]
WD = "http://www.wikidata.org/entity/"
DBO = "http://dbpedia.org/ontology/"


def test_readable_rule():
    assert readable_rule(f"{WD}Q5 => {WD}Q215627") == "wd:Q5  =>  wd:Q215627"
    fact = f"http://www.wikidata.org/prop/direct/P31={WD}Q5"
    assert readable_rule(f"{fact},{DBO}Person => {DBO}Agent") == "wdt:P31=wd:Q5 AND dbo:Person  =>  dbo:Agent"


def test_cohen_kappa_and_wilson():
    assert cohen_kappa([1, 1, 0, 0], [1, 1, 0, 0]) == pytest.approx(1.0)
    # po = 0.6, pe = 0.5 -> kappa = 0.2
    a = [1] * 5 + [0] * 5
    b = [1, 1, 1, 0, 0, 0, 0, 0, 1, 1]
    assert cohen_kappa(a, b) == pytest.approx((0.6 - 0.5) / 0.5)
    lo, hi = wilson_interval(8, 10)
    assert lo == pytest.approx(0.4902, abs=1e-3) and hi == pytest.approx(0.9433, abs=1e-3)


def test_pool_merges_systems():
    tsarm = [{"rule_key": "a => b", "lifecycle": "stable", "mean_confidence": 0.8, "confidence_series": [0.8, 0.8]}]
    pool = pool_rules(tsarm, {"amie": {"a => b": 0.9, "c => d": 0.5}})
    assert [(p.key, p.systems) for p in pool] == [("a => b", ["tsarm", "amie"]), ("c => d", ["amie"])]
    assert pool[0].confidence_series == "0.800;0.800"


def test_stratified_sample_keeps_small_strata():
    pool = [PooledRule(f"s{i} => x", ["tsarm"], "stable") for i in range(200)]
    pool += [PooledRule(f"t{i} => x", ["tsarm"], "transient") for i in range(3)]
    got = stratified_sample(pool, 50, seed=1, min_per_stratum=5)
    assert sum(p.lifecycle == "transient" for p in got) == 3
    assert 45 <= len(got) <= 55
    assert stratified_sample(pool[:10], 50) == pool[:10]


def _fill(path, verdicts):
    rows = list(csv.DictReader(open(path, newline="")))
    for r in rows:
        r["judge_a"], r["judge_b"], r["final"] = verdicts[r["rule"]]
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)


def test_sheet_score_reference_roundtrip(tmp_path):
    pool = pool_rules(
        [{"rule_key": k, "lifecycle": "stable"} for k in ("a => b", "c => d", "e => f")],
        {"amie": {"a => b": 0.9, "g => h": 0.4}},
    )
    sheet = write_sheet(pool, tmp_path / "sheet.csv")
    _fill(sheet, {
        "a => b": ("1", "1", ""),   # agreed valid
        "c => d": ("1", "0", "1"),  # disagreement, adjudicated valid
        "e => f": ("0", "0", ""),   # agreed invalid
        "g => h": ("?", "0", ""),   # undecided -> not in the reference
    })
    s = score_sheet(sheet)
    assert s.n_rules == 4 and s.n_both_decided == 3 and s.n_disagreements == 1
    assert s.reference == ["a => b", "c => d"]
    assert s.per_system["tsarm"]["precision"] == pytest.approx(2 / 3)
    assert s.per_system["tsarm"]["relative_recall"] == pytest.approx(1.0)
    assert s.per_system["amie"]["judged"] == 1 and s.per_system["amie"]["relative_recall"] == pytest.approx(0.5)
    ref = write_reference(s.reference, tmp_path / "ref.csv", note="test")
    assert load_reference_rules(ref) == {"a => b", "c => d"}


def test_single_annotator_retest(tmp_path):
    keys = [f"r{i} => x" for i in range(8)]
    pool = pool_rules([{"rule_key": k, "lifecycle": "stable"} for k in keys], {})
    sheet = write_sheet(pool, tmp_path / "sheet.csv")
    first = {k: ("1" if i % 2 == 0 else "0", "", "") for i, k in enumerate(keys)}
    first["r7 => x"] = ("?", "", "")      # undecided: not re-tested, not in the reference
    first["r5 => x"] = ("0", "", "1")     # re-examined after the retest: final overrides judge_a
    _fill(sheet, first)
    retest = write_retest_sheet(sheet, tmp_path / "retest.csv", fraction=0.5, seed=1)
    rows = list(csv.DictReader(open(retest, newline="")))
    assert len(rows) == 4 and set(rows[0]) == {"id", "rule", "readable", "labels", "links", "retest", "note"}
    assert all(r["rule"] != "r7 => x" for r in rows)  # only decided rules are drawn
    for r in rows:                                     # second pass: flip exactly one label
        r["retest"] = first[r["rule"]][0]
    rows[0]["retest"] = "0" if rows[0]["retest"] == "1" else "1"
    with open(retest, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    s = score_sheet(sheet, retest=retest)
    assert s.mode == "intra-rater"
    assert s.n_both_decided == 4 and s.n_disagreements == 1 and s.agreement == pytest.approx(0.75)
    assert s.reference == ["r0 => x", "r2 => x", "r4 => x", "r5 => x", "r6 => x"]
    assert score_sheet(sheet).n_both_decided == 0   # no retest given: no agreement figures


def test_cli(tmp_path):
    export = tmp_path / "rules.json"
    export.write_text(json.dumps({"rules": [
        {"rule_key": f"{WD}Q{i} => {WD}Q5", "lifecycle": "stable", "mean_confidence": 0.9,
         "confidence_series": [0.9]} for i in range(10)
    ]}))
    out = tmp_path / "sheet.csv"
    run = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "annotation_kit.py"), "sheet", "--tsarm", str(export),
         "--sample", "4", "--min-per-stratum", "1", "--out", str(out)],
        capture_output=True, text=True, check=True,
    )
    assert "4 of 10" in run.stdout
    rows = list(csv.DictReader(open(out, newline="")))
    assert rows[0]["links"].startswith("https://www.wikidata.org/wiki/Q")
    assert json.loads(Path(str(out) + ".meta.json").read_text())["pool"] == 10
