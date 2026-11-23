"""Tests for valid-time (calendar interval) windows."""

from datetime import date

import pytest

from src.windowing.valid_time import assign_valid_time_windows, build_time_windows


def test_build_time_windows():
    ws = build_time_windows(date(2000, 3, 15), date(2010, 1, 1), width_months=36, step_months=24)
    assert [(w.start, w.end) for w in ws] == [
        (date(2000, 3, 1), date(2003, 3, 1)),
        (date(2002, 3, 1), date(2005, 3, 1)),
        (date(2004, 3, 1), date(2007, 3, 1)),
        (date(2006, 3, 1), date(2009, 3, 1)),
    ]
    with pytest.raises(ValueError):
        build_time_windows(date(2000, 1, 1), date(2000, 6, 1), 12, 12)
    with pytest.raises(ValueError):
        build_time_windows(date(2000, 1, 1), date(2010, 1, 1), 0, 12)


def test_assign_valid_time_windows():
    from src.ingestion.spark_session import get_spark

    spark = get_spark(app_name="TSARM-valid-time-tests", master="local[2]")
    try:
        df = spark.createDataFrame(
            [
                ("ended", date(2001, 6, 1), date(2003, 6, 1)),    # overlaps w0 and w1
                ("started", date(2004, 1, 1), None),              # still holds; starts at w1 end (exclusive): w2 only
                ("point", date(2005, 5, 5), date(2005, 5, 5)),    # P585: w2 only
                ("before", None, date(1999, 1, 1)),               # ended before all windows
                ("always", None, None),                           # no qualifier
                ("edge", date(2002, 1, 1), date(2002, 1, 1)),     # w0 end is exclusive -> w1 only
            ],
            "subject string, valid_from date, valid_to date",
        )
        windows = build_time_windows(date(2000, 1, 1), date(2006, 1, 1), width_months=24, step_months=24)
        # w0 = [2000, 2002), w1 = [2002, 2004), w2 = [2004, 2006)
        got = {(r.subject, r.window_id) for r in assign_valid_time_windows(spark, df, windows).collect()}
        assert got == {
            ("ended", 0), ("ended", 1),
            ("started", 2),
            ("point", 2),
            ("always", 0), ("always", 1), ("always", 2),
            ("edge", 1),
        }
        dropped = {r.subject for r in assign_valid_time_windows(spark, df, windows, unqualified="drop").collect()}
        assert "always" not in dropped and "ended" in dropped
    finally:
        spark.stop()
