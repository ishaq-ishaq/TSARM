"""Spark session factory for TSARM.

Provides a single place to construct a tuned local SparkSession so that every
module (ingestion, windowing, mining, metrics) shares consistent configuration.
Defaults are tuned for the local development target described in CLAUDE.md
(MacBook Pro M1, 16 GB RAM) and are intended to be overridden when running on
Google Cloud Dataproc or AWS EMR.
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import SparkSession

# Conservative local defaults: leave headroom for the OS and the driver JVM on a
# 16 GB machine. Override via the `config` argument for cluster deployment.
_LOCAL_DEFAULTS = {
    "spark.driver.memory": "6g",
    "spark.sql.shuffle.partitions": "16",
    "spark.sql.parquet.compression.codec": "snappy",
    # Arrow speeds up pandas <-> Spark conversion used in notebooks/tests.
    "spark.sql.execution.arrow.pyspark.enabled": "true",
    # Quieter, reproducible local runs.
    "spark.ui.showConsoleProgress": "false",
}


def get_spark(
    app_name: str = "TSARM",
    master: str = "local[*]",
    config: Optional[dict] = None,
) -> SparkSession:
    """Build (or fetch the existing) SparkSession.

    Args:
        app_name: Spark application name shown in the UI / logs.
        master: Cluster master URL. ``local[*]`` uses all local cores; set to
            ``yarn`` (or leave unset and rely on ``spark-submit``) on a cluster.
        config: Extra Spark configuration entries that override the local
            defaults. Use this to bump memory/partitions on Dataproc/EMR.

    Returns:
        An active :class:`~pyspark.sql.SparkSession`.
    """
    builder = SparkSession.builder.appName(app_name).master(master)

    merged = dict(_LOCAL_DEFAULTS)
    if config:
        merged.update(config)
    for key, value in merged.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
