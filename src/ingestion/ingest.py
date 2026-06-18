"""Temporal RDF ingestion pipeline for TSARM.

This module turns raw RDF snapshot dumps into a canonical, time-partitioned
Parquet store -- the input for the windowing, mining and metrics stages.

Temporal model (snapshot model)
-------------------------------
TSARM operates over *time-partitioned* RDF. Each input file is treated as a
**snapshot** of the knowledge graph valid at a known timestamp (e.g. a Wikidata
weekly dump or a DBpedia release). Every triple is tagged with its snapshot
timestamp, and the Parquet output is partitioned by snapshot so that the
sliding-window stage can read only the snapshots a window touches.

The canonical schema is::

    subject     string   -- IRI or blank-node label
    predicate   string   -- IRI
    object      string   -- IRI, blank-node label, or literal lexical form
    object_kind string   -- "iri" | "blank" | "literal"
    datatype    string   -- literal datatype IRI (null otherwise)
    language    string   -- literal language tag (null otherwise)
    snapshot    string   -- snapshot id (partition column)
    snapshot_ts timestamp-- snapshot validity time

The N-Triples path is distributed: lines are read with ``spark.read.text`` and
parsed with Spark SQL regex functions, so no triple data passes through the
driver. Non-line formats fall back to the in-memory RDFLib parser.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Mapping, Optional, Union

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .rdf_parser import BLANK, IRI, LITERAL, parse_with_rdflib

# Columns that the rest of the pipeline depends on.
TRIPLE_SCHEMA = T.StructType(
    [
        T.StructField("subject", T.StringType(), False),
        T.StructField("predicate", T.StringType(), False),
        T.StructField("object", T.StringType(), False),
        T.StructField("object_kind", T.StringType(), False),
        T.StructField("datatype", T.StringType(), True),
        T.StructField("language", T.StringType(), True),
    ]
)

# Single N-Triples grammar regex, shared by every Spark regexp_extract call.
# Group indices (see _spark_parse_lines):
#   1 subj IRI  2 subj blank  3 pred IRI
#   4 obj IRI   5 obj blank   6 obj literal  7 obj datatype  8 obj lang
_NT_REGEX = (
    r'^\s*(?:<([^>]*)>|(_:\S+))\s+<([^>]*)>\s+'
    r'(?:<([^>]*)>|(_:\S+)|"((?:[^"\\]|\\.)*)"'
    r'(?:\^\^<([^>]*)>|@([A-Za-z]+(?:-[A-Za-z0-9]+)*))?)\s*\.\s*$'
)


def _nonempty(col: Column) -> Column:
    """regexp_extract yields '' on no-match; turn that into NULL for coalesce."""
    return F.when(col != "", col)


def _spark_parse_lines(lines: DataFrame) -> DataFrame:
    """Parse a DataFrame with a single ``value`` (line) column into triples."""

    def grp(idx: int) -> Column:
        return _nonempty(F.regexp_extract("value", _NT_REGEX, idx))

    subj_iri, subj_blank = grp(1), grp(2)
    pred_iri = grp(3)
    obj_iri, obj_blank, obj_lit = grp(4), grp(5), grp(6)
    obj_dtype, obj_lang = grp(7), grp(8)

    parsed = lines.select(
        F.coalesce(subj_iri, subj_blank).alias("subject"),
        pred_iri.alias("predicate"),
        F.coalesce(obj_iri, obj_blank, obj_lit).alias("object"),
        F.when(obj_iri.isNotNull(), F.lit(IRI))
        .when(obj_blank.isNotNull(), F.lit(BLANK))
        .otherwise(F.lit(LITERAL))
        .alias("object_kind"),
        obj_dtype.alias("datatype"),
        obj_lang.alias("language"),
    )

    # Drop comment/blank lines (predicate is the reliable required IRI). A
    # genuinely malformed line also fails the predicate match and is dropped,
    # mirroring the non-strict behaviour of the streaming parser.
    return parsed.where(F.col("predicate").isNotNull())


def read_ntriples(spark: SparkSession, path: Union[str, Path]) -> DataFrame:
    """Distributed N-Triples read -> triple DataFrame (no snapshot columns yet).

    ``path`` may be a single ``.nt`` file or a directory/glob of them; Spark
    handles the fan-out.
    """
    lines = spark.read.text(str(path))
    return _spark_parse_lines(lines)


def read_rdflib(
    spark: SparkSession, path: Union[str, Path], fmt: Optional[str] = None
) -> DataFrame:
    """In-memory parse of a non-line RDF format -> triple DataFrame.

    Suitable for small Turtle/RDF-XML/JSON-LD samples only.
    """
    rows = [
        (t.subject, t.predicate, t.obj, t.object_kind, t.datatype, t.language)
        for t in parse_with_rdflib(path, fmt=fmt)
    ]
    return spark.createDataFrame(rows, schema=TRIPLE_SCHEMA)


def _tag_snapshot(df: DataFrame, snapshot: str, snapshot_ts: datetime) -> DataFrame:
    """Attach snapshot id + validity timestamp partition columns."""
    return df.withColumn("snapshot", F.lit(snapshot)).withColumn(
        "snapshot_ts", F.lit(snapshot_ts).cast(T.TimestampType())
    )


def ingest_snapshot(
    spark: SparkSession,
    path: Union[str, Path],
    snapshot: str,
    snapshot_ts: datetime,
    fmt: Optional[str] = None,
) -> DataFrame:
    """Read one snapshot file and tag it with its temporal coordinates.

    Args:
        spark: Active SparkSession.
        path: Path to the snapshot RDF file.
        snapshot: Human-readable snapshot id, used as the Parquet partition
            value (e.g. ``"2024-01"`` or ``"wikidata-20240101"``).
        snapshot_ts: The timestamp at which this snapshot is valid.
        fmt: If given (e.g. ``"turtle"``), use the RDFLib path; otherwise the
            file is treated as N-Triples and read with the distributed parser.

    Returns:
        A DataFrame following :data:`TRIPLE_SCHEMA` plus ``snapshot`` and
        ``snapshot_ts`` columns.
    """
    triples = (
        read_rdflib(spark, path, fmt=fmt)
        if fmt is not None
        else read_ntriples(spark, path)
    )
    return _tag_snapshot(triples, snapshot, snapshot_ts)


def ingest_snapshots(
    spark: SparkSession,
    snapshots: Mapping[str, "tuple[Union[str, Path], datetime]"],
    fmt: Optional[str] = None,
) -> DataFrame:
    """Ingest several snapshots and union them into one temporal DataFrame.

    Args:
        spark: Active SparkSession.
        snapshots: Mapping of ``snapshot_id -> (path, snapshot_ts)``.
        fmt: Optional RDFLib format applied to every snapshot (e.g. all Turtle).

    Returns:
        The union of all tagged snapshot DataFrames.
    """
    frames = [
        ingest_snapshot(spark, path, snap, ts, fmt=fmt)
        for snap, (path, ts) in snapshots.items()
    ]
    if not frames:
        raise ValueError("No snapshots provided to ingest.")
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result


def write_parquet(
    df: DataFrame, output_dir: Union[str, Path], mode: str = "overwrite"
) -> None:
    """Persist a tagged triple DataFrame as snapshot-partitioned Parquet.

    Partitioning by ``snapshot`` lets the windowing stage prune to only the
    snapshots a sliding window overlaps.
    """
    (
        df.write.mode(mode)
        .partitionBy("snapshot")
        .parquet(str(output_dir))
    )


def read_parquet(spark: SparkSession, input_dir: Union[str, Path]) -> DataFrame:
    """Load a previously written temporal triple store."""
    return spark.read.parquet(str(input_dir))
