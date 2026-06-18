"""Tests for the distributed semantic ARM stage."""

from datetime import datetime

import pytest
from pyspark.sql import types as T

from src.ingestion.spark_session import get_spark
from src.mining.arm import build_transactions, mine, mine_rules
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
