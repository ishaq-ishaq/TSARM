"""Spark session factory for TSARM.

Provides a single place to construct a tuned local SparkSession so that every
module (ingestion, windowing, mining, metrics) shares consistent configuration.
Defaults are tuned for the local development target (MacBook Pro M5 Pro,
15 cores, 24 GB RAM) and can be overridden per run via the ``config`` argument
or the ``TSARM_DRIVER_MEMORY`` / ``TSARM_SHUFFLE_PARTITIONS`` environment
variables (e.g. on a smaller VM or a Dataproc/EMR cluster).
"""

from __future__ import annotations

import os
from typing import Optional

from pyspark.sql import SparkSession

# Local defaults: in local mode the whole job runs inside the driver JVM, so give
# it about half of a 24 GB machine and leave the rest for the OS, Python workers
# and the page cache. Shuffle partitions ~2x the 15 cores.
_LOCAL_DEFAULTS = {
    "spark.driver.memory": os.environ.get("TSARM_DRIVER_MEMORY", "12g"),
    "spark.sql.shuffle.partitions": os.environ.get("TSARM_SHUFFLE_PARTITIONS", "32"),
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
