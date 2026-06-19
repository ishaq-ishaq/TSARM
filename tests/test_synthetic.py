"""Tests for the synthetic temporal-RDF generator.

Generation is pure Python (no Spark). One Spark-backed test runs the full
pipeline and asserts the injected persistent/transient patterns are recovered by
the temporal metrics -- i.e. that the generator and the metrics agree on
ground truth.
"""

from pyspark.sql import functions as F

import pytest

from src.evaluation.synthetic import (
    PERSISTENT_RULE,
    TRANSIENT_RULE,
    generate_dataset,
)
from src.ingestion.ingest import ingest_snapshots
from src.ingestion.spark_session import get_spark
from src.metrics.temporal import compute_metrics
from src.mining.arm import mine
from src.windowing.sliding_window import (
    assign_windows,
    build_windows,
    ordered_snapshots,
)


def test_generate_dataset_writes_snapshots(tmp_path):
    dataset, total = generate_dataset(
        tmp_path, n_entities=10, n_snapshots=3, n_classes=5, n_orgs=5
    )
    assert len(dataset.snapshots) == 3
    # 2 triples per entity (type + worksAt) * 10 entities * 3 snapshots.
    assert total == 2 * 10 * 3
    for snap in dataset.snapshots:
        assert snap.path.exists()
        lines = snap.path.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 20  # 10 entities * 2 triples
        assert all(line.endswith(" .") for line in lines)


def test_generation_is_deterministic(tmp_path):
    d1, _ = generate_dataset(tmp_path / "a", n_entities=50, seed=7)
    d2, _ = generate_dataset(tmp_path / "b", n_entities=50, seed=7)
    for s1, s2 in zip(d1.snapshots, d2.snapshots):
        assert s1.path.read_text() == s2.path.read_text()


@pytest.fixture(scope="module")
def spark():
    session = get_spark(app_name="TSARM-synthetic-tests", master="local[2]")
    yield session
    session.stop()


def test_injected_patterns_recovered_by_metrics(spark, tmp_path):
    # Enough entities per class for the patterns to clear support thresholds.
    dataset, _ = generate_dataset(
        tmp_path,
        n_entities=500,
        n_snapshots=4,
        n_classes=5,
        n_orgs=5,
        transient_until=2,
        seed=1,
    )
    snap_map = {s.snapshot_id: (s.path, s.timestamp) for s in dataset.snapshots}
    triples = ingest_snapshots(spark, snap_map)
    snaps = ordered_snapshots(triples)
    n_windows = len(build_windows(snaps, width=1, step=1))
    windowed = assign_windows(spark, triples, width=1, step=1)
    _, rules = mine(windowed, min_support=0.05, min_confidence=0.5)
    metrics = compute_metrics(rules, n_windows=n_windows, tau=0.5)

    def find(rule):
        rows = metrics.where(
            F.array_contains("antecedent", rule["antecedent"])
            & F.array_contains("consequent", rule["consequent"])
            & (F.size("antecedent") == 1)
            & (F.size("consequent") == 1)
        ).collect()
        return rows[0] if rows else None

    persistent = find(PERSISTENT_RULE)
    transient = find(TRANSIENT_RULE)

    # The persistent rule must be found and hold across all windows.
    assert persistent is not None
    assert persistent["persistence_score"] == pytest.approx(1.0)

    # The transient rule decays: it is strictly less persistent than the
    # persistent rule (it holds only in the first transient_until snapshots).
    transient_persistence = transient["persistence_score"] if transient else 0.0
    assert transient_persistence < persistent["persistence_score"]
