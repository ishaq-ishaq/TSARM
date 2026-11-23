#!/usr/bin/env python3
"""Cluster rule trajectory embeddings and compare the clusters with lifecycle classes.

Reads a TSARM JSON export (any driver), builds the embedding of
``src.metrics.embeddings`` for every rule, runs k-means for each k in
``--k`` and reports, per k, the silhouette score and the adjusted Rand index
(ARI) between clusters and the Table 3.2 lifecycle classes. For the k with the
best silhouette it also writes the per-cluster lifecycle mix, mean drift and
RPS, and 2-D PCA coordinates for a figure.

    python3 scripts/cluster_trajectories.py results/wikidata_temporal_rules_h1.json \\
        --k 2,3,4,5,6,8 --out results/clusters_wikidata_h1.json

A high ARI means that the unsupervised grouping of trajectories rediscovers the
rule-based lifecycle classes, which is evidence that the classes describe real
shapes in the data. A low ARI combined with a high silhouette means the
trajectories form groups that the classes do not separate. Those groups are
worth reading rule by rule.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _frame(spark, rules):
    rows = [
        (r["rule_key"], r["lifecycle"], float(r["confidence_drift"]), float(r["persistence_score"]),
         [float(x) for x in r["confidence_series"]], [float(x) for x in r["support_series"]])
        for r in rules
    ]
    return spark.createDataFrame(
        rows,
        "rule_key string, lifecycle string, confidence_drift double, persistence_score double, "
        "confidence_series array<double>, support_series array<double>",
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("report", help="TSARM JSON export")
    ap.add_argument("--k", default="2,3,4,5,6", help="comma-separated cluster counts")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--shape-only", action="store_true",
                    help="embed only the centred confidence trajectory and its differences (no level, no support)")
    ap.add_argument("--master", default="local[*]")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    from pyspark.ml.evaluation import ClusteringEvaluator
    from pyspark.ml.functions import array_to_vector

    from src.ingestion.spark_session import get_spark
    from src.metrics.embeddings import (
        adjusted_rand_index,
        cluster_lifecycle_table,
        cluster_rules,
        project_2d,
        trajectory_embeddings,
    )

    report = json.loads(Path(args.report).read_text())
    rules = report["rules"]
    if len(rules) < 3:
        sys.exit(f"{len(rules)} rules: too few to cluster")
    spark = get_spark(app_name="TSARM-cluster-trajectories", master=args.master)
    metrics = trajectory_embeddings(_frame(spark, rules), shape_only=args.shape_only).cache()
    n = metrics.count()
    lifecycle_mix = dict(Counter(r["lifecycle"] for r in rules))
    evaluator = ClusteringEvaluator(featuresCol="_vec", predictionCol="cluster")

    sweep = []
    for k in sorted({int(x) for x in args.k.split(",")}):
        if not 2 <= k < n:
            continue
        clustered = cluster_rules(metrics, k=k, seed=args.seed).cache()
        got = clustered.select("lifecycle", "cluster").collect()
        if len({r.cluster for r in got}) < 2:  # k-means collapsed: silhouette undefined
            sweep.append({"k": k, "silhouette": None, "ari": None, "clusters_used": 1})
            continue
        sil = evaluator.evaluate(clustered.withColumn("_vec", array_to_vector("embedding")))
        ari = adjusted_rand_index([r.lifecycle for r in got], [r.cluster for r in got])
        sweep.append({"k": k, "silhouette": sil, "ari": ari, "clusters_used": len({r.cluster for r in got})})
        print(f"k={k:2d}  silhouette {sil:6.3f}  ARI vs lifecycle {ari:6.3f}")
    valid = [s for s in sweep if s["silhouette"] is not None]
    if not valid:
        sys.exit("no k produced two or more clusters")
    best = max(valid, key=lambda s: s["silhouette"])
    k_lc = len(lifecycle_mix)
    at_k_lc = next((s for s in valid if s["k"] == k_lc), None)

    clustered = cluster_rules(metrics, k=best["k"], seed=args.seed)
    table = cluster_lifecycle_table(clustered)
    coords = project_2d(clustered).select("rule_key", "lifecycle", "cluster", "pc1", "pc2").collect()
    out = {
        "report": args.report, "shape_only": args.shape_only, "n_rules": n, "n_windows": len(rules[0]["confidence_series"]),
        "lifecycle_mix": lifecycle_mix, "sweep": sweep, "best_k": best["k"],
        "ari_at_k_equal_classes": at_k_lc["ari"] if at_k_lc else None,
        "cluster_table": table, "points": [r.asDict() for r in coords],
    }
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(out, indent=2))
    print(f"best k={best['k']} (silhouette {best['silhouette']:.3f}, ARI {best['ari']:.3f}); "
          f"lifecycle classes present: {lifecycle_mix}")
    for row in table:
        print(f"  cluster {row['cluster']}: {row['n']:5d} {row['lifecycle']:12s} "
              f"drift {row['mean_drift']:+.3f}  RPS {row['mean_rps']:.2f}")
    print(f"wrote {args.out}")
    spark.stop()


if __name__ == "__main__":
    main()
