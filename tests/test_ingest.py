"""Integration tests for the Spark-native ingestion pipeline.

A module-scoped local SparkSession is reused across tests to amortise the JVM
startup cost.
"""

from datetime import datetime

import pytest

from src.ingestion.ingest import (
    ingest_snapshot,
    ingest_snapshots,
    read_ntriples,
    read_parquet,
    write_parquet,
)
from src.ingestion.spark_session import get_spark

XSD_DATE = "http://www.w3.org/2001/XMLSchema#date"


@pytest.fixture(scope="module")
def spark():
    session = get_spark(app_name="TSARM-tests", master="local[2]")
    yield session
    session.stop()


def _write_sample(tmp_path, name, lines):
    path = tmp_path / name
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def test_read_ntriples_distributed(spark, tmp_path):
    path = _write_sample(
        tmp_path,
        "s.nt",
        [
            "# comment line",
            "<http://ex.org/Alice> <http://ex.org/knows> <http://ex.org/Bob> .",
            f'<http://ex.org/Alice> <http://ex.org/hireDate> "2023-01-15"^^<{XSD_DATE}> .',
            '<http://ex.org/Alice> <http://ex.org/label> "Alice"@en .',
        ],
    )
    df = read_ntriples(spark, path)
    rows = {(r.subject, r.predicate, r.object_kind): r for r in df.collect()}

    assert df.count() == 3  # comment dropped
    iri_row = rows[("http://ex.org/Alice", "http://ex.org/knows", "iri")]
    assert iri_row.object == "http://ex.org/Bob"
    lit_row = rows[("http://ex.org/Alice", "http://ex.org/hireDate", "literal")]
    assert lit_row.object == "2023-01-15"
    assert lit_row.datatype == XSD_DATE
    lang_row = rows[("http://ex.org/Alice", "http://ex.org/label", "literal")]
    assert lang_row.language == "en"


def test_ingest_snapshot_tags_temporal_columns(spark, tmp_path):
    path = _write_sample(
        tmp_path,
        "snap.nt",
        ["<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> ."],
    )
    ts = datetime(2023, 1, 1)
    df = ingest_snapshot(spark, path, snapshot="2023", snapshot_ts=ts)
    row = df.collect()[0]
    assert row.snapshot == "2023"
    assert row.snapshot_ts == ts


def test_ingest_and_roundtrip_parquet(spark, tmp_path):
    s1 = _write_sample(
        tmp_path, "y23.nt", ["<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> ."]
    )
    s2 = _write_sample(
        tmp_path,
        "y24.nt",
        [
            "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .",
            "<http://ex.org/a> <http://ex.org/p> <http://ex.org/c> .",
        ],
    )
    df = ingest_snapshots(
        spark,
        {
            "2023": (s1, datetime(2023, 1, 1)),
            "2024": (s2, datetime(2024, 1, 1)),
        },
    )
    assert df.count() == 3
    assert {r.snapshot for r in df.select("snapshot").distinct().collect()} == {
        "2023",
        "2024",
    }

    out = tmp_path / "parquet"
    write_parquet(df, out)
    reloaded = read_parquet(spark, out)
    assert reloaded.count() == 3
    # Partition pruning: a single snapshot reads back correctly.
    assert reloaded.where("snapshot = '2024'").count() == 2
