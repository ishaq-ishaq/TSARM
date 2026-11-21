"""Tests for rule trajectory embeddings (Amendment 4)."""

import pytest
from pyspark.sql import types as T

from src.ingestion.spark_session import get_spark
from src.metrics.embeddings import (
    adjusted_rand_index,
    cluster_lifecycle_table,
    cluster_rules,
    project_2d,
    similar_rules,
    trajectory_embeddings,
)
from src.metrics.temporal import compute_metrics

_DENSE = T.StructType(
    [
        T.StructField("rule_key", T.StringType(), False),
        T.StructField("antecedent", T.ArrayType(T.StringType()), True),
        T.StructField("consequent", T.ArrayType(T.StringType()), True),
        T.StructField("window_id", T.IntegerType(), False),
        T.StructField("support", T.DoubleType(), False),
        T.StructField("confidence", T.DoubleType(), False),
        T.StructField("lift", T.DoubleType(), False),
        T.StructField("emitted", T.BooleanType(), False),
    ]
)

# Three well-separated lifecycle shapes, three rules each (small jitter).
SHAPES = {
    "stable": [[0.90, 0.91, 0.90, 0.90], [0.88, 0.89, 0.88, 0.89], [0.92, 0.91, 0.92, 0.92]],
    "emerging": [[0.10, 0.40, 0.70, 0.95], [0.12, 0.42, 0.68, 0.93], [0.08, 0.38, 0.72, 0.96]],
    "decaying": [[0.95, 0.70, 0.40, 0.10], [0.93, 0.68, 0.42, 0.12], [0.96, 0.72, 0.38, 0.08]],
}


@pytest.fixture(scope="module")
def spark():
    session = get_spark(app_name="TSARM-embedding-tests", master="local[2]")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def metrics(spark):
    rows = []
    for shape, series_list in SHAPES.items():
        for j, confs in enumerate(series_list):
            key = f"{shape}-{j}"
            for w, c in enumerate(confs):
                rows.append((key, [key + "-A"], [key + "-B"], w, round(c / 2, 4), c, 1.0, True))
    df = spark.createDataFrame(rows, schema=_DENSE)
    return compute_metrics(df, n_windows=4, tau=0.7).cache()


def test_embedding_layout(metrics):
    row = trajectory_embeddings(metrics).where("rule_key = 'emerging-0'").first()
    conf, supp = [0.10, 0.40, 0.70, 0.95], [0.05, 0.20, 0.35, 0.475]
    diffs = [0.30, 0.30, 0.25]
    assert row.embedding == pytest.approx(conf + supp + diffs)
    assert len(row.embedding) == 3 * 4 - 1


def test_lifecycle_classes_of_fixture(metrics):
    got = {r.rule_key.split("-")[0] for r in metrics.collect() if r.lifecycle == r.rule_key.split("-")[0]}
    assert got == {"stable", "emerging", "decaying"}


def test_clusters_recover_lifecycle_classes(metrics):
    clustered = cluster_rules(metrics, k=3, seed=7).collect()
    ari = adjusted_rand_index([r.cluster for r in clustered], [r.lifecycle for r in clustered])
    assert ari == pytest.approx(1.0)


def test_cluster_lifecycle_table(metrics):
    table = cluster_lifecycle_table(cluster_rules(metrics, k=3, seed=7))
    assert sum(r["n"] for r in table) == 9
    assert {r["lifecycle"] for r in table} == {"stable", "emerging", "decaying"}


def test_similar_rules_are_same_shape(metrics):
    near = [r.rule_key for r in similar_rules(metrics, "stable-0", top_k=2).collect()]
    assert sorted(near) == ["stable-1", "stable-2"]
    with pytest.raises(ValueError):
        similar_rules(metrics, "no-such-rule")


def test_project_2d_separates_shapes(metrics):
    pts = {r.rule_key: (r.pc1, r.pc2) for r in project_2d(metrics).collect()}
    assert len(pts) == 9 and all(v[0] is not None for v in pts.values())

    def centroid(shape):
        xs = [pts[f"{shape}-{j}"] for j in range(3)]
        return (sum(x for x, _ in xs) / 3, sum(y for _, y in xs) / 3)

    def spread(shape):
        cx, cy = centroid(shape)
        return max(((pts[f"{shape}-{j}"][0] - cx) ** 2 + (pts[f"{shape}-{j}"][1] - cy) ** 2) ** 0.5 for j in range(3))

    # Each group is tight relative to the distance between group centroids.
    (ax, ay), (bx, by) = centroid("emerging"), centroid("decaying")
    assert ((ax - bx) ** 2 + (ay - by) ** 2) ** 0.5 > 10 * max(spread("emerging"), spread("decaying"))


def test_adjusted_rand_index_hand_cases():
    assert adjusted_rand_index([0, 0, 1, 1], ["a", "a", "b", "b"]) == pytest.approx(1.0)
    assert adjusted_rand_index([1, 1, 0, 0], ["a", "a", "b", "b"]) == pytest.approx(1.0)
    # Every item its own cluster vs. one class: ARI = 0
    assert adjusted_rand_index([0, 1, 2, 3], ["a", "a", "a", "a"]) == pytest.approx(0.0)
    with pytest.raises(ValueError):
        adjusted_rand_index([0], [0, 1])


def test_adjusted_rand_index_matches_sklearn():
    sklearn = pytest.importorskip("sklearn.metrics")
    a = [0, 0, 1, 1, 2, 2, 2, 0, 1, 2]
    b = ["x", "x", "y", "z", "z", "z", "y", "x", "y", "x"]
    assert adjusted_rand_index(a, b) == pytest.approx(sklearn.adjusted_rand_score(a, b))
