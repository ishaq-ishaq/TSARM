"""Tests for the temporal significance metrics stage.

Metrics are checked against hand-computed values on small, fully-specified rule
trajectories so the formulas (drift, volatility, persistence) are pinned.
"""

import pytest
from pyspark.sql import types as T

from src.ingestion.spark_session import get_spark
from src.metrics.temporal import compute_metrics

_RULES_SCHEMA = T.StructType(
    [
        T.StructField("rule_key", T.StringType(), False),
        T.StructField("antecedent", T.ArrayType(T.StringType()), True),
        T.StructField("consequent", T.ArrayType(T.StringType()), True),
        T.StructField("window_id", T.IntegerType(), False),
        T.StructField("support", T.DoubleType(), False),
        T.StructField("confidence", T.DoubleType(), False),
    ]
)


@pytest.fixture(scope="module")
def spark():
    session = get_spark(app_name="TSARM-metrics-tests", master="local[2]")
    yield session
    session.stop()


def _rules(spark, rows):
    return spark.createDataFrame(rows, schema=_RULES_SCHEMA)


def test_metrics_single_rule_across_all_windows(spark):
    # Rule R present in all 3 windows; confidence 0.8 -> 0.6 -> 1.0.
    rows = [
        ("R", ["A"], ["B"], 0, 0.5, 0.8),
        ("R", ["A"], ["B"], 1, 0.4, 0.6),
        ("R", ["A"], ["B"], 2, 0.6, 1.0),
    ]
    m = compute_metrics(_rules(spark, rows), n_windows=3, tau=0.6).collect()
    assert len(m) == 1
    r = m[0]

    assert r.occurrences == 3
    assert r.first_window == 0 and r.last_window == 2
    # temporal support = (0.5+0.4+0.6)/3 = 0.5
    assert r.temporal_support == pytest.approx(0.5)
    # mean confidence = (0.8+0.6+1.0)/3
    assert r.mean_confidence == pytest.approx(2.4 / 3)
    assert r.first_confidence == pytest.approx(0.8)
    assert r.last_confidence == pytest.approx(1.0)
    # net drift = 1.0 - 0.8 = 0.2 ; rate = 0.2 / (3-1) = 0.1
    assert r.confidence_drift == pytest.approx(0.2)
    assert r.confidence_drift_rate == pytest.approx(0.1)
    # volatility = (|0.6-0.8| + |1.0-0.6|) / 2 = (0.2+0.4)/2 = 0.3
    assert r.confidence_volatility == pytest.approx(0.3)
    # persistence: windows with conf >= 0.6 are all 3 -> 3/3 = 1.0
    assert r.persistence_score == pytest.approx(1.0)


def test_persistence_counts_only_windows_above_tau(spark):
    # Confidences 0.9, 0.5, 0.7 over 3 windows; tau=0.6 -> 2 of 3 qualify.
    rows = [
        ("R", ["A"], ["B"], 0, 0.5, 0.9),
        ("R", ["A"], ["B"], 1, 0.5, 0.5),
        ("R", ["A"], ["B"], 2, 0.5, 0.7),
    ]
    r = compute_metrics(_rules(spark, rows), n_windows=3, tau=0.6).collect()[0]
    assert r.persistence_score == pytest.approx(2 / 3)


def test_absent_windows_lower_temporal_support_and_persistence(spark):
    # Rule appears in only 2 of 4 total windows; both clear tau.
    rows = [
        ("R", ["A"], ["B"], 0, 0.8, 0.9),
        ("R", ["A"], ["B"], 1, 0.8, 0.9),
    ]
    r = compute_metrics(_rules(spark, rows), n_windows=4, tau=0.6).collect()[0]
    assert r.occurrences == 2
    # temporal support normalised by n=4: (0.8+0.8)/4 = 0.4
    assert r.temporal_support == pytest.approx(0.4)
    # persistence: 2 qualifying windows / 4 total = 0.5
    assert r.persistence_score == pytest.approx(0.5)


def test_single_occurrence_has_zero_drift(spark):
    rows = [("R", ["A"], ["B"], 0, 0.7, 0.85)]
    r = compute_metrics(_rules(spark, rows), n_windows=2, tau=0.6).collect()[0]
    assert r.occurrences == 1
    assert r.confidence_drift == pytest.approx(0.0)
    assert r.confidence_drift_rate == pytest.approx(0.0)
    assert r.confidence_volatility == pytest.approx(0.0)
    assert r.persistence_score == pytest.approx(0.5)  # 1 of 2 windows


def test_multiple_rules_ranked_by_persistence(spark):
    rows = [
        # Persistent rule: high confidence in both windows.
        ("R1", ["A"], ["B"], 0, 0.6, 0.9),
        ("R1", ["A"], ["B"], 1, 0.6, 0.95),
        # Transient rule: appears once.
        ("R2", ["C"], ["D"], 0, 0.6, 0.7),
    ]
    out = compute_metrics(_rules(spark, rows), n_windows=2, tau=0.6).collect()
    assert [r.rule_key for r in out] == ["R1", "R2"]  # ordered by persistence desc
    by_key = {r.rule_key: r for r in out}
    assert by_key["R1"].persistence_score == pytest.approx(1.0)
    assert by_key["R2"].persistence_score == pytest.approx(0.5)


def test_infers_n_windows_when_not_given(spark):
    rows = [
        ("R", ["A"], ["B"], 0, 0.5, 0.9),
        ("R", ["A"], ["B"], 1, 0.5, 0.9),
    ]
    # Only 2 distinct windows in the data -> inferred n=2.
    r = compute_metrics(_rules(spark, rows), tau=0.6).collect()[0]
    assert r.persistence_score == pytest.approx(1.0)
