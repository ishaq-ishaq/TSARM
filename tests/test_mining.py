"""Tests for the distributed semantic ARM stage."""

from datetime import datetime

import pytest
from pyspark.sql import types as T

from src.ingestion.spark_session import get_spark
from src.mining.arm import (
    build_transactions,
    mine,
    mine_rules,
    mine_temporal,
    recount_candidates,
)
from src.windowing.sliding_window import assign_windows

_WINDOWED_SCHEMA = T.StructType(
    [
        T.StructField("subject", T.StringType(), True),
        T.StructField("predicate", T.StringType(), True),
        T.StructField("object", T.StringType(), True),
        T.StructField("object_kind", T.StringType(), True),
        T.StructField("window_id", T.IntegerType(), True),
    ]
)


@pytest.fixture(scope="module")
def spark():
    session = get_spark(app_name="TSARM-mining-tests", master="local[2]")
    yield session
    session.stop()


TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
WORKS = "http://ex.org/worksAt"
ENGINEER = "http://ex.org/Engineer"
ACME = "http://ex.org/AcmeCorp"


def _windowed(spark, rows):
    # rows: (subject, predicate, object, object_kind, window_id)
    return spark.createDataFrame(rows, schema=_WINDOWED_SCHEMA)


def test_build_transactions_groups_items_per_entity(spark):
    df = _windowed(
        spark,
        [
            ("Alice", TYPE, ENGINEER, "iri", 0),
            ("Alice", WORKS, ACME, "iri", 0),
            ("Alice", WORKS, ACME, "iri", 0),  # duplicate -> deduped
        ],
    )
    tx = build_transactions(df).collect()
    assert len(tx) == 1
    row = tx[0]
    assert row.subject == "Alice"
    assert set(row.items) == {f"{TYPE}={ENGINEER}", f"{WORKS}={ACME}"}


def test_build_transactions_excludes_literals_by_default(spark):
    df = _windowed(
        spark,
        [
            ("Alice", TYPE, ENGINEER, "iri", 0),
            ("Alice", "http://ex.org/hireDate", "2023-01-15", "literal", 0),
        ],
    )
    items = build_transactions(df).collect()[0].items
    assert items == [f"{TYPE}={ENGINEER}"]

    items_with_lit = build_transactions(df, include_literals=True).collect()[0].items
    assert set(items_with_lit) == {
        f"{TYPE}={ENGINEER}",
        "http://ex.org/hireDate=2023-01-15",
    }


def test_mine_recovers_known_rule(spark):
    # Two entities in window 0, both Engineers working at Acme:
    # rule {type=Engineer} => {worksAt=Acme} should hold with confidence 1.0.
    df = _windowed(
        spark,
        [
            ("Alice", TYPE, ENGINEER, "iri", 0),
            ("Alice", WORKS, ACME, "iri", 0),
            ("Bob", TYPE, ENGINEER, "iri", 0),
            ("Bob", WORKS, ACME, "iri", 0),
        ],
    )
    itemsets, rules = mine(df, min_support=0.5, min_confidence=0.5)

    # The frequent itemset {type=Engineer, worksAt=Acme} has support 1.0.
    full = [
        r
        for r in itemsets.collect()
        if set(r.items) == {f"{TYPE}={ENGINEER}", f"{WORKS}={ACME}"}
    ]
    assert full and full[0].support == pytest.approx(1.0)

    rule_rows = {
        r.rule_key: r
        for r in rules.collect()
    }
    key = f"{TYPE}={ENGINEER} => {WORKS}={ACME}"
    assert key in rule_rows
    assert rule_rows[key].confidence == pytest.approx(1.0)
    assert rule_rows[key].window_id == 0


def test_mine_rules_tags_each_window(spark):
    df = _windowed(
        spark,
        [
            ("Alice", TYPE, ENGINEER, "iri", 0),
            ("Alice", WORKS, ACME, "iri", 0),
            ("Bob", TYPE, ENGINEER, "iri", 0),
            ("Bob", WORKS, ACME, "iri", 0),
            # window 1: the co-occurrence is broken (Dave works elsewhere)
            ("Carol", TYPE, ENGINEER, "iri", 1),
            ("Carol", WORKS, ACME, "iri", 1),
            ("Dave", TYPE, ENGINEER, "iri", 1),
            ("Dave", WORKS, "http://ex.org/Globex", "iri", 1),
        ],
    )
    _, rules = mine(df, min_support=0.5, min_confidence=0.5)
    windows = {r.window_id for r in rules.collect()}
    assert windows == {0, 1}


def test_mine_raises_on_empty(spark):
    empty = _windowed(spark, []).limit(0)
    tx = build_transactions(empty)
    with pytest.raises(ValueError):
        mine_rules(tx)


def test_mine_end_to_end_through_windowing(spark):
    # Ingestion-shaped rows -> windowing -> mining, to exercise the real path.
    rows = [
        ("Alice", TYPE, ENGINEER, "iri", None, None, "2023", datetime(2023, 1, 1)),
        ("Alice", WORKS, ACME, "iri", None, None, "2023", datetime(2023, 1, 1)),
        ("Bob", TYPE, ENGINEER, "iri", None, None, "2023", datetime(2023, 1, 1)),
        ("Bob", WORKS, ACME, "iri", None, None, "2023", datetime(2023, 1, 1)),
        ("Alice", TYPE, ENGINEER, "iri", None, None, "2024", datetime(2024, 1, 1)),
        ("Alice", WORKS, ACME, "iri", None, None, "2024", datetime(2024, 1, 1)),
    ]
    triples_schema = T.StructType(
        [
            T.StructField("subject", T.StringType(), True),
            T.StructField("predicate", T.StringType(), True),
            T.StructField("object", T.StringType(), True),
            T.StructField("object_kind", T.StringType(), True),
            T.StructField("datatype", T.StringType(), True),
            T.StructField("language", T.StringType(), True),
            T.StructField("snapshot", T.StringType(), True),
            T.StructField("snapshot_ts", T.TimestampType(), True),
        ]
    )
    triples = spark.createDataFrame(rows, schema=triples_schema)
    windowed = assign_windows(spark, triples, width=1, step=1)
    _, rules = mine(windowed, min_support=0.5, min_confidence=0.5)
    assert rules.count() >= 1


# ---------------------------------------------------------------------------
# Candidate recount (proposal Sec. 3.5.4): true per-window measures everywhere
# ---------------------------------------------------------------------------

_TX_SCHEMA = T.StructType(
    [
        T.StructField("window_id", T.IntegerType(), False),
        T.StructField("subject", T.StringType(), False),
        T.StructField("items", T.ArrayType(T.StringType()), False),
    ]
)


def test_recount_matches_fpgrowth_and_fills_non_frequent_windows(spark):
    # Window 0: A=>B holds for 3/4 entities with A (frequent at minSupport 0.5).
    # Window 1: A present in 4 entities, B with only 1 of them -> conf 0.25,
    #           so FP-Growth (minConfidence 0.6) does not emit A=>B there.
    # Window 2: no transactions at all.
    tx = spark.createDataFrame(
        [
            (0, "e1", ["A", "B"]),
            (0, "e2", ["A", "B"]),
            (0, "e3", ["A", "B"]),
            (0, "e4", ["A"]),
            (1, "e1", ["A", "B"]),
            (1, "e2", ["A"]),
            (1, "e3", ["A"]),
            (1, "e4", ["A"]),
            (1, "e5", ["B"]),
        ],
        schema=_TX_SCHEMA,
    )
    _, rules = mine_rules(tx, min_support=0.5, min_confidence=0.6)
    dense = recount_candidates(tx, rules, window_ids=[0, 1, 2])
    rows = {
        r.window_id: r
        for r in dense.where("rule_key = 'A => B'").collect()
    }
    assert set(rows) == {0, 1, 2}

    # Window 0 equals what FP-Growth reported.
    fp = rules.where("rule_key = 'A => B' AND window_id = 0").collect()[0]
    assert rows[0].emitted is True
    assert rows[0].support == pytest.approx(fp.support)  # 3/4
    assert rows[0].confidence == pytest.approx(fp.confidence)  # 3/4
    assert rows[0].lift == pytest.approx(fp.lift)  # 0.75 / 0.75 = 1.0

    # Window 1: not emitted, but measured: N=5, n(A)=4, n(B)=2, n(AB)=1.
    assert rows[1].emitted is False
    assert rows[1].n_transactions == 5
    assert rows[1].antecedent_count == 4
    assert rows[1].consequent_count == 2
    assert rows[1].rule_count == 1
    assert rows[1].support == pytest.approx(1 / 5)
    assert rows[1].confidence == pytest.approx(1 / 4)
    assert rows[1].lift == pytest.approx((1 / 4) / (2 / 5))

    # Window 2: empty -> all zero, no division errors.
    assert rows[2].n_transactions == 0
    assert rows[2].support == 0.0 and rows[2].confidence == 0.0
    assert rows[2].lift == 0.0 and rows[2].emitted is False


def test_recount_multi_item_antecedent(spark):
    tx = spark.createDataFrame(
        [
            (0, "e1", ["A", "B", "C"]),
            (0, "e2", ["A", "B", "C"]),
            (0, "e3", ["A", "B"]),
            (0, "e4", ["C"]),
        ],
        schema=_TX_SCHEMA,
    )
    _, rules = mine_rules(tx, min_support=0.5, min_confidence=0.6)
    dense = recount_candidates(tx, rules, window_ids=[0])
    r = dense.where("rule_key = 'A,B => C'").collect()[0]
    assert r.antecedent_count == 3 and r.rule_count == 2
    assert r.confidence == pytest.approx(2 / 3)


def _reference_itemset_counts(tx, isets):
    """Plain-Python n_w(X) for each itemset X: transactions containing all of X."""
    counts = {}
    for r in tx:
        items = set(r["items"])
        for key, iset in isets.items():
            if iset and set(iset) <= items:
                counts[(r["window_id"], key)] = counts.get((r["window_id"], key), 0) + 1
    return counts


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_recount_matches_brute_force_on_random_data(spark, seed):
    # Random transactions over a skewed vocabulary (incl. duplicate items and
    # empty windows), mined by FP-Growth; every recounted measure must equal a
    # brute-force count over the same transactions.
    import random

    rnd = random.Random(seed)
    vocab = [f"C{i}" for i in range(12)]
    weights = [12 - i for i in range(12)]
    rows = []
    for w in (0, 1, 3):  # window 2 left empty
        for e in range(rnd.randint(30, 60)):
            items = rnd.choices(vocab, weights=weights, k=rnd.randint(1, 5))
            rows.append((w, f"e{e}", items))
    tx = spark.createDataFrame(rows, schema=_TX_SCHEMA)  # may repeat an item
    unique = spark.createDataFrame(
        [(w, e, list(dict.fromkeys(items))) for w, e, items in rows], schema=_TX_SCHEMA
    )
    _, rules = mine_rules(unique, min_support=0.15, min_confidence=0.3)
    dense = recount_candidates(tx, rules, window_ids=[0, 1, 2, 3]).collect()
    assert dense, "the random data should yield some rules"

    tx_rows = [{"window_id": w, "items": items} for w, _, items in rows]
    n_tx = {}
    for r in tx_rows:
        n_tx[r["window_id"]] = n_tx.get(r["window_id"], 0) + 1
    isets = {}
    for r in dense:
        isets[("A",) + tuple(sorted(r.antecedent))] = r.antecedent
        isets[("C",) + tuple(sorted(r.consequent))] = r.consequent
        isets[("F",) + tuple(sorted(set(r.antecedent) | set(r.consequent)))] = list(
            set(r.antecedent) | set(r.consequent)
        )
    ref = _reference_itemset_counts(tx_rows, isets)
    emitted = {(r.window_id, r.rule_key) for r in rules.collect()}
    rule_keys = {r.rule_key for r in dense}
    assert len(dense) == 4 * len(rule_keys)  # dense: every candidate x every window
    for r in dense:
        w = r.window_id
        full = tuple(sorted(set(r.antecedent) | set(r.consequent)))
        assert r.n_transactions == n_tx.get(w, 0)
        assert r.antecedent_count == ref.get((w, ("A",) + tuple(sorted(r.antecedent))), 0)
        assert r.consequent_count == ref.get((w, ("C",) + tuple(sorted(r.consequent))), 0)
        assert r.rule_count == ref.get((w, ("F",) + full), 0)
        assert r.emitted == ((w, r.rule_key) in emitted)


def test_mine_temporal_returns_dense_candidates(spark):
    df = _windowed(
        spark,
        [
            ("Alice", TYPE, ENGINEER, "iri", 0),
            ("Alice", WORKS, ACME, "iri", 0),
            ("Bob", TYPE, ENGINEER, "iri", 0),
            ("Bob", WORKS, ACME, "iri", 0),
            ("Alice", TYPE, ENGINEER, "iri", 1),
            ("Bob", TYPE, ENGINEER, "iri", 1),
        ],
    )
    _, rules, cands = mine_temporal(
        df, window_ids=[0, 1], min_support=0.5, min_confidence=0.6
    )
    key = f"{TYPE}={ENGINEER} => {WORKS}={ACME}"
    got = {r.window_id: r for r in cands.where(cands.rule_key == key).collect()}
    assert got[0].confidence == pytest.approx(1.0) and got[0].emitted
    # Window 1: Engineers no longer work at Acme -> measured confidence 0.
    assert got[1].confidence == pytest.approx(0.0) and not got[1].emitted
