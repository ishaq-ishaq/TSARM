"""Tests for the temporal significance metrics and lifecycle classification.

Every expected value is hand-computed from the proposal definitions:
TempSupp (Eq. 3.5), Drift (Eq. 3.6), RPS (Eq. 3.7) and Table 3.2.
"""

import pytest
from pyspark.sql import types as T

from src.ingestion.spark_session import get_spark
from src.metrics.temporal import compute_metrics

_SPARSE_SCHEMA = T.StructType(
    [
        T.StructField("rule_key", T.StringType(), False),
        T.StructField("antecedent", T.ArrayType(T.StringType()), True),
        T.StructField("consequent", T.ArrayType(T.StringType()), True),
        T.StructField("window_id", T.IntegerType(), False),
        T.StructField("support", T.DoubleType(), False),
        T.StructField("confidence", T.DoubleType(), False),
    ]
)

_DENSE_SCHEMA = T.StructType(
    _SPARSE_SCHEMA.fields
    + [
        T.StructField("lift", T.DoubleType(), False),
        T.StructField("emitted", T.BooleanType(), False),
    ]
)


@pytest.fixture(scope="module")
def spark():
    session = get_spark(app_name="TSARM-metrics-tests", master="local[2]")
    yield session
    session.stop()


def _sparse(spark, rows):
    return spark.createDataFrame(rows, schema=_SPARSE_SCHEMA)


def _dense(spark, key, confs, supp=0.5):
    """Dense trajectory for one rule: emitted wherever conf > 0."""
    rows = [
        (key, ["A"], ["B"], w, supp if c > 0 else 0.0, c, 1.0, c > 0)
        for w, c in enumerate(confs)
    ]
    return spark.createDataFrame(rows, schema=_DENSE_SCHEMA)


def test_metrics_single_rule_across_all_windows(spark):
    # Confidence 0.8 -> 0.6 -> 1.0, support 0.5, 0.4, 0.6, tau = 0.6.
    rows = [
        ("R", ["A"], ["B"], 0, 0.5, 0.8),
        ("R", ["A"], ["B"], 1, 0.4, 0.6),
        ("R", ["A"], ["B"], 2, 0.6, 1.0),
    ]
    r = compute_metrics(_sparse(spark, rows), n_windows=3, tau=0.6).collect()[0]

    assert r.occurrences == 3
    assert r.first_window == 0 and r.last_window == 2
    # Eq. 3.5: (0.5 + 0.4 + 0.6) / 3
    assert r.temporal_support == pytest.approx(0.5)
    # Eq. 3.6: ((0.6-0.8) + (1.0-0.6)) / (3-1) = 0.1
    assert r.confidence_drift == pytest.approx(0.1)
    # Eq. 3.7: all 3 windows >= 0.6
    assert r.persistence_score == pytest.approx(1.0)
    assert r.mean_confidence == pytest.approx(0.8)
    # (|0.6-0.8| + |1.0-0.6|) / 2
    assert r.confidence_volatility == pytest.approx(0.3)
    assert list(r.confidence_series) == pytest.approx([0.8, 0.6, 1.0])
    # Drift 0.1 > eps -> emerging (amended Table 3.2: direction first).
    assert r.lifecycle == "emerging"


def test_persistence_counts_only_windows_above_tau(spark):
    rows = [
        ("R", ["A"], ["B"], 0, 0.5, 0.9),
        ("R", ["A"], ["B"], 1, 0.5, 0.5),
        ("R", ["A"], ["B"], 2, 0.5, 0.7),
    ]
    r = compute_metrics(_sparse(spark, rows), n_windows=3, tau=0.6).collect()[0]
    assert r.persistence_score == pytest.approx(2 / 3)


def test_default_tau_is_proposal_value(spark):
    # conf 0.69 / 0.70 / 0.71: with tau = 0.70 (Sec. 3.5.5) two windows qualify.
    rows = [
        ("R", ["A"], ["B"], 0, 0.5, 0.69),
        ("R", ["A"], ["B"], 1, 0.5, 0.70),
        ("R", ["A"], ["B"], 2, 0.5, 0.71),
    ]
    r = compute_metrics(_sparse(spark, rows), n_windows=3).collect()[0]
    assert r.persistence_score == pytest.approx(2 / 3)


def test_sparse_input_is_zero_padded_over_all_windows(spark):
    # Emitted in windows 0,1 of 4 -> trajectory [0.9, 0.9, 0, 0].
    rows = [
        ("R", ["A"], ["B"], 0, 0.8, 0.9),
        ("R", ["A"], ["B"], 1, 0.8, 0.9),
    ]
    r = compute_metrics(_sparse(spark, rows), n_windows=4, tau=0.6).collect()[0]
    assert r.occurrences == 2
    assert list(r.window_series) == [0, 1, 2, 3]
    assert list(r.emitted_series) == [True, True, False, False]
    assert r.temporal_support == pytest.approx(0.4)  # 1.6 / 4
    assert r.persistence_score == pytest.approx(0.5)  # 2 / 4
    assert r.confidence_drift == pytest.approx(-0.3)  # (0 - 0.9) / 3
    assert r.mean_confidence == pytest.approx(0.9)  # over emitted windows only


def test_single_window_has_zero_drift(spark):
    rows = [("R", ["A"], ["B"], 0, 0.7, 0.85)]
    r = compute_metrics(_sparse(spark, rows), n_windows=1, tau=0.6).collect()[0]
    assert r.confidence_drift == pytest.approx(0.0)
    assert r.confidence_volatility == pytest.approx(0.0)
    assert r.persistence_score == pytest.approx(1.0)
    assert r.lifecycle == "stable"


@pytest.mark.parametrize(
    "confs, expected_drift, expected_rps, expected_class",
    [
        # |Drift| = 0.01/3 <= eps, RPS = 1.0 -> stable
        ([0.80, 0.82, 0.80, 0.81], 0.01 / 3, 1.0, "stable"),
        # Drift = 0.75/3 = 0.25 > eps, RPS = 1/4 -> emerging
        ([0.0, 0.3, 0.6, 0.75], 0.25, 0.25, "emerging"),
        # Drift = (0.1-0.75)/3 < -eps, RPS = 1/4 -> decaying
        ([0.75, 0.6, 0.3, 0.1], -0.65 / 3, 0.25, "decaying"),
        # RPS = 1/6 < 0.2 -> transient (takes precedence)
        ([0.0, 0.9, 0.0, 0.0, 0.0, 0.0], 0.0, 1 / 6, "transient"),
        # Drift = 0.1, RPS = 1.0 -> reliable but strengthening: emerging
        ([0.7, 0.8, 0.9, 1.0], 0.1, 1.0, "emerging"),
        # Drift = (0.0-0.75)/3, RPS = 0.5 -> collapses after 2 windows: decaying
        # (this case was unclassifiable under the original Table 3.2)
        ([0.75, 0.75, 0.0, 0.0], -0.25, 0.5, "decaying"),
        # Drift = (0-0)/4 = 0, RPS = 2/5 -> on and off, no trend: intermittent
        ([0.0, 0.8, 0.0, 0.8, 0.0], 0.0, 0.4, "intermittent"),
    ],
)
def test_lifecycle_classification(spark, confs, expected_drift, expected_rps, expected_class):
    df = _dense(spark, "R", confs)
    r = compute_metrics(df, n_windows=len(confs), tau=0.70).collect()[0]
    assert r.confidence_drift == pytest.approx(expected_drift)
    assert r.persistence_score == pytest.approx(expected_rps)
    assert r.lifecycle == expected_class


def test_dense_input_uses_true_values_not_zero_padding(spark):
    # Not emitted in window 2, but the recount says conf there was 0.4.
    rows = [
        ("R", ["A"], ["B"], 0, 0.5, 0.9, 1.0, True),
        ("R", ["A"], ["B"], 1, 0.5, 0.8, 1.0, True),
        ("R", ["A"], ["B"], 2, 0.2, 0.4, 1.0, False),
    ]
    df = spark.createDataFrame(rows, schema=_DENSE_SCHEMA)
    r = compute_metrics(df, n_windows=3, tau=0.7).collect()[0]
    assert r.occurrences == 2
    assert r.temporal_support == pytest.approx(1.2 / 3)
    assert r.confidence_drift == pytest.approx((0.4 - 0.9) / 2)
    assert r.persistence_score == pytest.approx(2 / 3)


def test_rules_ranked_by_persistence_then_temporal_support(spark):
    rows = [
        ("R1", ["A"], ["B"], 0, 0.6, 0.9),
        ("R1", ["A"], ["B"], 1, 0.6, 0.95),
        ("R2", ["C"], ["D"], 0, 0.6, 0.9),
        ("R3", ["E"], ["F"], 0, 0.3, 0.9),
    ]
    out = compute_metrics(_sparse(spark, rows), n_windows=2, tau=0.6).collect()
    assert [r.rule_key for r in out] == ["R1", "R2", "R3"]


def test_infers_n_windows_when_not_given(spark):
    rows = [
        ("R", ["A"], ["B"], 0, 0.5, 0.9),
        ("R", ["A"], ["B"], 1, 0.5, 0.9),
    ]
    r = compute_metrics(_sparse(spark, rows), tau=0.6).collect()[0]
    assert r.persistence_score == pytest.approx(1.0)
