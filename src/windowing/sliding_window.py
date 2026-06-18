"""Sliding temporal windows over snapshot-partitioned RDF triples.

This stage groups the ordered KG snapshots produced by the ingestion layer into
overlapping **sliding windows**, the unit over which the mining stage computes
per-window support/confidence and the metrics stage computes temporal
significance (temporal support, confidence drift, rule persistence).

Windowing mode: **snapshot-count** (chosen for the prototype).
A window spans ``width`` consecutive snapshots and the start advances by
``step`` snapshots each time. With ``width=2, step=1`` over snapshots
``[s0, s1, s2, s3]`` the windows are::

    w0 = {s0, s1}
    w1 = {s1, s2}
    w2 = {s2, s3}

A single triple that occurs in a snapshot belonging to several windows is
assigned to each of them (the output is exploded by ``window_id``), so each
window can be mined independently.

Snapshots are ordered by ``snapshot_ts`` (then ``snapshot`` id as a stable
tie-break), making the window index meaningful regardless of ingestion order.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


@dataclass(frozen=True)
class Window:
    """Metadata for one sliding window."""

    window_id: int
    snapshots: Tuple[str, ...]  # member snapshot ids, in temporal order

    @property
    def size(self) -> int:
        return len(self.snapshots)


def ordered_snapshots(df: DataFrame) -> List[str]:
    """Return the distinct snapshot ids in temporal order.

    Ordered by ``snapshot_ts`` ascending, breaking ties on the ``snapshot`` id
    so the result is deterministic. The distinct-snapshot count equals the
    number of ingested dumps (small), so collecting to the driver is safe.
    """
    rows = (
        df.select("snapshot", "snapshot_ts")
        .distinct()
        .orderBy(F.col("snapshot_ts").asc(), F.col("snapshot").asc())
        .collect()
    )
    return [r["snapshot"] for r in rows]


def build_windows(
    snapshots: List[str],
    width: int,
    step: int = 1,
    include_partial: bool = False,
) -> List[Window]:
    """Compute sliding windows over an ordered snapshot list.

    Args:
        snapshots: Snapshot ids in temporal order (see :func:`ordered_snapshots`).
        width: Number of consecutive snapshots per window (``>= 1``).
        step: Snapshots to advance the window start each time (``>= 1``).
        include_partial: If ``True``, keep a trailing window shorter than
            ``width`` when the snapshots do not divide evenly; if ``False``
            (default), only full ``width``-sized windows are emitted.

    Returns:
        A list of :class:`Window`, ``window_id`` numbered from 0.
    """
    if width < 1:
        raise ValueError("width must be >= 1")
    if step < 1:
        raise ValueError("step must be >= 1")

    windows: List[Window] = []
    wid = 0
    start = 0
    n = len(snapshots)
    while start < n:
        members = snapshots[start : start + width]
        if len(members) < width and not include_partial:
            break
        if not members:
            break
        windows.append(Window(window_id=wid, snapshots=tuple(members)))
        wid += 1
        start += step
    return windows


def assign_windows(
    spark: SparkSession,
    df: DataFrame,
    width: int,
    step: int = 1,
    include_partial: bool = False,
) -> DataFrame:
    """Tag triples with their sliding ``window_id`` (exploded for overlap).

    Each input triple row is replicated once per window that contains its
    snapshot, with a ``window_id`` column added. The output is the input
    schema plus ``window_id`` (int).

    Args:
        spark: Active SparkSession (used to build the assignment table).
        df: Snapshot-tagged triple DataFrame (must have a ``snapshot`` column).
        width: Snapshots per window.
        step: Window slide in snapshots.
        include_partial: Keep a short trailing window (see :func:`build_windows`).

    Returns:
        The triples DataFrame joined to its window assignments. Triples whose
        snapshot falls in no emitted window (e.g. a dropped partial tail) are
        excluded by the inner join.
    """
    snapshots = ordered_snapshots(df)
    windows = build_windows(snapshots, width=width, step=step, include_partial=include_partial)

    # Flatten to (snapshot, window_id) pairs and broadcast-join onto the triples.
    pairs = [
        (snap, w.window_id) for w in windows for snap in w.snapshots
    ]
    schema = T.StructType(
        [
            T.StructField("snapshot", T.StringType(), False),
            T.StructField("window_id", T.IntegerType(), False),
        ]
    )
    assignments = spark.createDataFrame(pairs, schema=schema)

    return df.join(F.broadcast(assignments), on="snapshot", how="inner")
