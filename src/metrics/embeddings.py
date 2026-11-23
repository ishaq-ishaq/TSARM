"""Rule trajectory embeddings (proposal Objective ii, as amended: Amendment 4).

Temporal embedding is realised at the level of **rules**: for each candidate
rule ``r`` over the ordered window set ``W`` (``n = |W|``), the vector

    e(r) = [ conf_w1 .. conf_wn | supp_w1 .. supp_wn | d_1 .. d_{n-1} ]

concatenates the per-window confidence and support series with the first
differences ``d_i = conf_{w_{i+1}} - conf_{w_i}`` of the confidence trajectory.
All components already lie in [0, 1] or [-1, 1], so no rescaling is applied
and every dimension keeps its plain meaning.

On these vectors the module provides:

A shape-only variant (``shape_only=True``) drops the level and the support
series so that rules group by trajectory shape alone.

* :func:`cluster_rules`: k-means (Spark MLlib) grouping of rules with similar
  lifecycles; :func:`adjusted_rand_index` scores agreement between clusters and
  the Table 3.2 lifecycle classes (a data-driven check on §3.5.6).
* :func:`project_2d`: PCA to two dimensions for visualisation.
* :func:`similar_rules`: nearest rules to a given rule (Euclidean distance).

Input is the output of :func:`src.metrics.temporal.compute_metrics`.
"""

from __future__ import annotations

from collections import Counter
from math import comb
from typing import Dict, Hashable, List, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

EMBEDDING_COL = "embedding"


def trajectory_embeddings(metrics: DataFrame, shape_only: bool = False) -> DataFrame:
    """Add an ``embedding`` (array<double>) column built from the series.

    With ``shape_only`` the vector is ``[conf_wi - mean(conf) | d_1 .. d_{n-1}]``:
    the confidence trajectory centred on its own mean, plus its first
    differences, without the support series. Rules then group by the *shape*
    of their trajectory (flat, rising, falling, spike) regardless of how
    strong they are, which is what the lifecycle classes describe. The default
    vector also carries the level, so k-means on it tends to split rules by
    confidence level first.
    """
    diffs = F.expr(
        "CASE WHEN size(confidence_series) > 1 THEN "
        "transform(sequence(1, size(confidence_series) - 1), "
        "i -> element_at(confidence_series, i + 1) - element_at(confidence_series, i)) "
        "ELSE array() END"
    ).cast("array<double>")
    if shape_only:
        centred = F.expr(
            "transform(confidence_series, c -> c - aggregate(confidence_series, 0D, (a, x) -> a + x)"
            " / size(confidence_series))"
        ).cast("array<double>")
        return metrics.withColumn(EMBEDDING_COL, F.concat(centred, diffs))
    return metrics.withColumn(
        EMBEDDING_COL,
        F.concat(
            F.col("confidence_series").cast("array<double>"),
            F.col("support_series").cast("array<double>"),
            diffs,
        ),
    )


def _with_vector(df: DataFrame) -> DataFrame:
    from pyspark.ml.functions import array_to_vector

    if EMBEDDING_COL not in df.columns:
        df = trajectory_embeddings(df)
    return df.withColumn("_vec", array_to_vector(EMBEDDING_COL))


def cluster_rules(metrics: DataFrame, k: int = 4, seed: int = 42) -> DataFrame:
    """k-means over trajectory embeddings; adds integer column ``cluster``."""
    from pyspark.ml.clustering import KMeans

    df = _with_vector(metrics)
    n = df.count()
    k_eff = max(1, min(k, n))
    model = KMeans(k=k_eff, seed=seed, featuresCol="_vec", predictionCol="cluster").fit(df)
    return model.transform(df).drop("_vec")


def project_2d(metrics: DataFrame) -> DataFrame:
    """PCA to 2 dimensions; adds ``pc1`` and ``pc2`` for plotting."""
    from pyspark.ml.feature import PCA
    from pyspark.ml.functions import vector_to_array

    df = _with_vector(metrics)
    dims = len(df.select(EMBEDDING_COL).first()[0])
    k = min(2, dims)
    model = PCA(k=k, inputCol="_vec", outputCol="_pca").fit(df)
    out = model.transform(df).withColumn("_pc", vector_to_array("_pca"))
    out = out.withColumn("pc1", F.col("_pc")[0])
    out = out.withColumn("pc2", F.col("_pc")[1] if k > 1 else F.lit(0.0))
    return out.drop("_vec", "_pca", "_pc")


def similar_rules(metrics: DataFrame, rule_key: str, top_k: int = 5) -> DataFrame:
    """The ``top_k`` rules nearest to ``rule_key`` by Euclidean distance."""
    emb = trajectory_embeddings(metrics) if EMBEDDING_COL not in metrics.columns else metrics
    query = emb.where(F.col("rule_key") == rule_key).select(EMBEDDING_COL).first()
    if query is None:
        raise ValueError(f"Unknown rule_key: {rule_key!r}")
    q = F.array(*[F.lit(float(x)) for x in query[0]])
    dist = F.sqrt(
        F.aggregate(
            F.zip_with(F.col(EMBEDDING_COL), q, lambda a, b: (a - b) * (a - b)),
            F.lit(0.0),
            lambda acc, x: acc + x,
        )
    )
    return (
        emb.where(F.col("rule_key") != rule_key)
        .withColumn("distance", dist)
        .orderBy("distance", "rule_key")
        .limit(top_k)
    )


def adjusted_rand_index(labels_a: Sequence[Hashable], labels_b: Sequence[Hashable]) -> float:
    """Adjusted Rand index between two labelings (Hubert & Arabie, 1985).

    1.0 = identical partitions, ~0 = chance agreement. Used to score how well
    unsupervised trajectory clusters recover the Table 3.2 lifecycle classes.
    """
    if len(labels_a) != len(labels_b):
        raise ValueError("Labelings must have equal length.")
    n = len(labels_a)
    if n < 2:
        return 1.0
    pairs = Counter(zip(labels_a, labels_b))
    sum_ij = sum(comb(c, 2) for c in pairs.values())
    sum_a = sum(comb(c, 2) for c in Counter(labels_a).values())
    sum_b = sum(comb(c, 2) for c in Counter(labels_b).values())
    expected = sum_a * sum_b / comb(n, 2)
    max_index = (sum_a + sum_b) / 2
    if max_index == expected:
        return 1.0
    return (sum_ij - expected) / (max_index - expected)


def cluster_lifecycle_table(clustered: DataFrame) -> List[Dict]:
    """Per-cluster size, lifecycle mix and mean drift/RPS (for reporting)."""
    rows = (
        clustered.groupBy("cluster", "lifecycle")
        .agg(
            F.count("*").alias("n"),
            F.avg("confidence_drift").alias("mean_drift"),
            F.avg("persistence_score").alias("mean_rps"),
        )
        .orderBy("cluster", "lifecycle")
        .collect()
    )
    return [r.asDict() for r in rows]
