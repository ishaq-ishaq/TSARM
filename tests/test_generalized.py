"""Tests for the length-bounded generalized miner with ancestor pruning."""

import random

import pytest

from src.ingestion.spark_session import get_spark
from src.mining.arm import mine_rules
from src.mining.generalized import mine_generalized


@pytest.fixture(scope="module")
def spark():
    s = get_spark(app_name="TSARM-generalized-tests", master="local[2]")
    yield s
    s.stop()


def _tx(spark, rows):
    return spark.createDataFrame(rows, "window_id int, subject string, items array<string>")


def _rules(df):
    return {
        (r.window_id, r.rule_key): (round(r.confidence, 9), round(r.lift, 9), round(r.support, 9))
        for r in df.collect()
    }


def test_matches_fpgrowth_without_hierarchy(spark):
    rng = random.Random(3)
    items = [f"i{k}" for k in range(7)]
    rows = []
    for w in (0, 1):
        for e in range(120):
            k = rng.randint(1, 5)
            # skewed item choice so there are frequent itemsets of every size
            basket = {items[min(int(rng.expovariate(0.6)), 6)] for _ in range(k)}
            rows.append((w, f"e{e}", sorted(basket)))
    tx = _tx(spark, rows)
    _, fp = mine_rules(tx, min_support=0.08, min_confidence=0.3)
    fp_small = fp.where("size(antecedent) <= 2")
    _, gen = mine_generalized(tx, 0.08, 0.3, max_len=3)
    expected, got = _rules(fp_small), _rules(gen)
    assert len(expected) > 20  # the test data exercise all three levels
    assert got == expected

    _, gen2 = mine_generalized(tx, 0.08, 0.3, max_len=2)
    assert _rules(gen2) == _rules(fp.where("size(antecedent) = 1"))


def test_ancestor_pruning(spark):
    PHYS, SCI, PERSON, AWARD = "Phys", "Sci", "Person", "Award"
    rows = [(0, f"p{i}", [PHYS, SCI, PERSON, AWARD]) for i in range(6)]
    rows += [(0, f"q{i}", [PERSON]) for i in range(4)]
    closure = spark.createDataFrame(
        [(0, PHYS, SCI), (0, PHYS, PERSON), (0, SCI, PERSON)], "window_id int, cls string, ancestor string"
    )
    itemsets, rules = mine_generalized(_tx(spark, rows), 0.2, 0.5, closure=closure)
    sets = {tuple(r.items) for r in itemsets.collect()}
    # no itemset holds a class together with one of its ancestors
    assert not any({PHYS, SCI} <= set(s) or {PHYS, PERSON} <= set(s) or {SCI, PERSON} <= set(s) for s in sets)
    keys = {r.rule_key for r in rules.collect()}
    assert {f"{PHYS} => {AWARD}", f"{SCI} => {AWARD}", f"{PERSON} => {AWARD}"} <= keys
    assert f"{PHYS} => {SCI}" not in keys and f"{SCI} => {PHYS}" not in keys
    conf = {r.rule_key: r.confidence for r in rules.collect()}
    assert conf[f"{PERSON} => {AWARD}"] == pytest.approx(0.6)


def test_rejects_bad_length(spark):
    with pytest.raises(ValueError):
        mine_generalized(_tx(spark, [(0, "a", ["x"])]), 0.1, 0.1, max_len=4)
