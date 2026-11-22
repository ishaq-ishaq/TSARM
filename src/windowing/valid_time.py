"""Valid-time windows: calendar intervals over the time at which facts held.

The snapshot windows of :mod:`src.windowing.sliding_window` group facts by
*transaction time*: the release in which they were recorded. This module
implements the interval windows of proposal Equation 3.1 on *valid time*: the
real-world period in which a fact held, as recorded by Wikidata's P580 (start
time), P582 (end time) and P585 (point in time) qualifiers (loaded as
``valid_from`` / ``valid_to`` by :func:`src.ingestion.wikidata.read_statements`).

Window ``w_i`` covers ``[start + i*step, start + i*step + width)`` (months). A
statement belongs to every window its valid interval overlaps::

    valid_from < window_end  and  (valid_to is null or valid_to >= window_start)

A missing ``valid_from`` means "since an unknown time" and a missing
``valid_to`` means "still holds". Statements with neither qualifier carry no
valid time: by default they are kept in every window (they are treated as
holding throughout); with ``unqualified="drop"`` only time-qualified statements
are used.

Use one snapshot (normally the latest, which has the most complete history), so
that a fact recorded in several releases is not counted several times.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


@dataclass(frozen=True)
class TimeWindow:
    window_id: int
    start: date  # inclusive
    end: date  # exclusive


def _add_months(d: date, months: int) -> date:
    y, m = divmod(d.month - 1 + months, 12)
    return date(d.year + y, m + 1, 1)


def build_time_windows(start: date, end: date, width_months: int, step_months: int) -> List[TimeWindow]:
    """Windows of ``width_months`` starting every ``step_months`` from ``start``
    (normalised to the first of its month), up to the last window ending at or
    before ``end``."""
    if width_months < 1 or step_months < 1:
        raise ValueError("width_months and step_months must be >= 1")
    start = date(start.year, start.month, 1)
    out, i = [], 0
    while True:
        ws = _add_months(start, i * step_months)
        we = _add_months(ws, width_months)
        if we > end:
            break
        out.append(TimeWindow(i, ws, we))
        i += 1
    if not out:
        raise ValueError("No complete window fits between start and end.")
    return out


def assign_valid_time_windows(
    spark: SparkSession, df: DataFrame, windows: List[TimeWindow], unqualified: str = "keep"
) -> DataFrame:
    """Tag statements with every valid-time window they overlap (``window_id``).

    Args:
        df: statements with ``valid_from`` / ``valid_to`` date columns.
        windows: from :func:`build_time_windows`.
        unqualified: ``"keep"`` (in every window) or ``"drop"`` statements with
            no valid-time qualifier.
    """
    if unqualified not in ("keep", "drop"):
        raise ValueError("unqualified must be 'keep' or 'drop'")
    win = spark.createDataFrame(
        [(w.window_id, w.start, w.end) for w in windows],
        T.StructType([
            T.StructField("window_id", T.IntegerType(), False),
            T.StructField("_ws", T.DateType(), False),
            T.StructField("_we", T.DateType(), False),
        ]),
    )
    vf, vt = F.col("valid_from"), F.col("valid_to")
    no_time = vf.isNull() & vt.isNull()
    if unqualified == "drop":
        df = df.where(~no_time)
    overlaps = (vf.isNull() | (vf < F.col("_we"))) & (vt.isNull() | (vt >= F.col("_ws")))
    return df.join(F.broadcast(win), overlaps, "inner").drop("_ws", "_we")
