"""Tests for the sliding-window module.

`build_windows` is pure Python (no Spark); `assign_windows` / `ordered_snapshots`
use a local SparkSession.
"""

from datetime import datetime

import pytest

from src.ingestion.spark_session import get_spark
from src.windowing.sliding_window import (
    assign_windows,
    build_windows,
    ordered_snapshots,
)


# --- pure-Python window construction ----------------------------------------


def test_build_windows_basic_overlap():
    windows = build_windows(["s0", "s1", "s2", "s3"], width=2, step=1)
    assert [w.snapshots for w in windows] == [
        ("s0", "s1"),
        ("s1", "s2"),
        ("s2", "s3"),
    ]
    assert [w.window_id for w in windows] == [0, 1, 2]


def test_build_windows_step_two():
    windows = build_windows(["s0", "s1", "s2", "s3"], width=2, step=2)
    assert [w.snapshots for w in windows] == [("s0", "s1"), ("s2", "s3")]


def test_build_windows_drops_partial_by_default():
    windows = build_windows(["s0", "s1", "s2"], width=2, step=2)
    assert [w.snapshots for w in windows] == [("s0", "s1")]  # trailing s2 dropped


def test_build_windows_keeps_partial_when_requested():
    windows = build_windows(["s0", "s1", "s2"], width=2, step=2, include_partial=True)
    assert [w.snapshots for w in windows] == [("s0", "s1"), ("s2",)]


def test_build_windows_validates_args():
    with pytest.raises(ValueError):
        build_windows(["s0"], width=0)
    with pytest.raises(ValueError):
        build_windows(["s0"], width=1, step=0)


# --- Spark-backed assignment -------------------------------------------------


@pytest.fixture(scope="module")
def spark():
    session = get_spark(app_name="TSARM-windowing-tests", master="local[2]")
    yield session
    session.stop()


def _triples(spark, rows):
    # rows: list of (subject, predicate, object, snapshot, snapshot_ts)
    return spark.createDataFrame(
        rows, schema=["subject", "predicate", "object", "snapshot", "snapshot_ts"]
    )


def test_ordered_snapshots_by_timestamp(spark):
    df = _triples(
        spark,
        [
            ("a", "p", "b", "2024", datetime(2024, 1, 1)),
            ("a", "p", "b", "2022", datetime(2022, 1, 1)),
            ("a", "p", "b", "2023", datetime(2023, 1, 1)),
        ],
    )
    assert ordered_snapshots(df) == ["2022", "2023", "2024"]


def test_assign_windows_explodes_overlap(spark):
    df = _triples(
        spark,
        [
            ("a", "p", "b", "2022", datetime(2022, 1, 1)),
            ("a", "p", "b", "2023", datetime(2023, 1, 1)),
            ("a", "p", "b", "2024", datetime(2024, 1, 1)),
        ],
    )
    tagged = assign_windows(spark, df, width=2, step=1)

    # 3 snapshots, width 2, step 1 -> windows {2022,2023},{2023,2024}.
    # 2022 -> w0 ; 2023 -> w0,w1 ; 2024 -> w1  => 4 rows.
    assert tagged.count() == 4
    by_window = {
        r.window_id: r.cnt
        for r in tagged.groupBy("window_id").count().withColumnRenamed("count", "cnt").collect()
    }
    assert by_window == {0: 2, 1: 2}

    w1_snaps = {r.snapshot for r in tagged.where("window_id = 1").collect()}
    assert w1_snaps == {"2023", "2024"}
