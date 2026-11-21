"""Temporal rule significance metrics and lifecycle classification for TSARM.

This is Layer 5 of the proposal architecture (Fig. 3.4): it reads the
per-window rule measures produced by the mining layer, computes the three
temporal significance metrics of proposal Sec. 3.5.5 and assigns each rule a
lifecycle class per Table 3.2 (Sec. 3.5.6).

Notation
--------
``W = {w_1, ..., w_n}`` is the full ordered window set (``n = |W|``). For a
candidate rule ``r: X => Y`` the mining layer supplies ``supp_w(r)`` and
``conf_w(r)`` (Eq. 3.2, 3.3) for **every** window ``w in W`` -- see
:func:`src.mining.arm.recount_candidates`. A rule is a *candidate* if it was
frequent (``supp_w(r) >= sigma``) in at least one window.

Metrics (proposal Sec. 3.5.5)
-----------------------------
* **Temporal support** (Eq. 3.5)::

      TempSupp(r) = (1/|W|) * sum_{w in W} supp_w(r)

* **Temporal confidence drift** (Eq. 3.6) -- mean first difference of the
  confidence trajectory::

      Drift(r) = sum_{i=1}^{|W|-1} [conf_{w_{i+1}}(r) - conf_{w_i}(r)] / (|W| - 1)

  ``Drift(r) = 0`` when ``|W| < 2``. Positive = strengthening, negative =
  weakening.

* **Rule Persistence Score** (Eq. 3.7)::

      RPS(r) = (1/|W|) * |{ w in W : conf_w(r) >= tau }|,   tau = 0.70 default

Lifecycle classes (proposal Table 3.2 as amended, epsilon = 0.05)
-----------------------------------------------------------------
Evaluated in this order; the classes are exhaustive and mutually exclusive::

    transient     RPS(r) < 0.20                         (any drift)
    emerging      Drift(r) >  +eps
    decaying      Drift(r) <  -eps
    stable        |Drift(r)| <= eps and RPS(r) >= 0.50
    intermittent  |Drift(r)| <= eps and 0.20 <= RPS(r) < 0.50

The original Table 3.2 left some (Drift, RPS) combinations unclassified (e.g. a
rule with RPS = 0.5 whose confidence collapses). The amended table keeps the
thresholds but gives direction of change priority over persistence and adds the
*intermittent* class, so every candidate rule receives exactly one class.

Supplementary descriptors (not proposal metrics)
------------------------------------------------
``mean_confidence`` (mean over windows where the rule was emitted by the
miner), ``confidence_volatility`` (mean |consecutive change| over ``W``) and
``occurrences`` (number of windows where the rule was emitted) are kept for
analysis and baseline comparison. The full per-window series are kept so
trajectories can be plotted (Sec. 3.7.5) or fed to temporal-embedding work.
"""

from __future__ import annotations

from typing import Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Defaults from proposal Sec. 3.5.5, 3.5.6 and 3.7.4.
DEFAULT_TAU = 0.70  # RPS confidence threshold (Eq. 3.7)
DEFAULT_EPSILON = 0.05  # drift stability band (Table 3.2)
DEFAULT_RPS_STABLE = 0.50  # RPS boundary between stable and emerging/decaying
DEFAULT_RPS_TRANSIENT = 0.20  # RPS below which a rule is transient

LIFECYCLE_CLASSES = ("transient", "emerging", "decaying", "stable", "intermittent")


def _densify(rules: DataFrame, window_ids: Sequence[int]) -> DataFrame:
    """Give every rule a row for every window.

    Input produced by :func:`src.mining.arm.recount_candidates` is already
    dense (it has an ``emitted`` column) and is returned unchanged. Sparse input
    (only the windows where the miner emitted the rule) is padded with
    ``support = confidence = 0`` for the missing windows -- an approximation of
    the true per-window values, kept for backwards compatibility.
    """
    if "emitted" in rules.columns:
        out = rules
    else:
        spark = rules.sparkSession
        windows = spark.createDataFrame(
            [(int(w),) for w in window_ids], "window_id int"
        )
        keys = rules.select("rule_key", "antecedent", "consequent").dropDuplicates(
            ["rule_key"]
        )
        observed = rules.select(
            "rule_key",
            "window_id",
            "support",
            "confidence",
            *(["lift"] if "lift" in rules.columns else []),
        ).withColumn("emitted", F.lit(True))
        out = (
            keys.crossJoin(F.broadcast(windows))
            .join(observed, ["rule_key", "window_id"], "left")
            .fillna(0.0, subset=["support", "confidence"])
            .fillna(False, subset=["emitted"])
        )
    if "lift" not in out.columns:
        out = out.withColumn("lift", F.lit(None).cast("double"))
    return out


def rule_trajectories(rules: DataFrame) -> DataFrame:
    """Collect each rule's per-window measures, ordered by ``window_id``.

    Returns one row per rule with column ``obs``: an array of
    ``(window_id, support, confidence, lift, emitted)`` structs sorted by
    ``window_id``.
    """
    return rules.groupBy("rule_key", "antecedent", "consequent").agg(
        F.sort_array(
            F.collect_list(
                F.struct(
                    F.col("window_id").cast("int").alias("window_id"),
                    F.col("support").cast("double").alias("support"),
                    F.col("confidence").cast("double").alias("confidence"),
                    F.col("lift").cast("double").alias("lift"),
                    F.col("emitted").alias("emitted"),
                )
            )
        ).alias("obs")
    )


def lifecycle_class(
    drift_col: str = "confidence_drift",
    rps_col: str = "persistence_score",
    epsilon: float = DEFAULT_EPSILON,
    rps_stable: float = DEFAULT_RPS_STABLE,
    rps_transient: float = DEFAULT_RPS_TRANSIENT,
):
    """Column expression assigning the (amended) Table 3.2 lifecycle class."""
    drift = F.col(drift_col)
    rps = F.col(rps_col)
    return (
        F.when(rps < rps_transient, F.lit("transient"))
        .when(drift > epsilon, F.lit("emerging"))
        .when(drift < -epsilon, F.lit("decaying"))
        .when(rps >= rps_stable, F.lit("stable"))
        .otherwise(F.lit("intermittent"))
    )


def compute_metrics(
    rules: DataFrame,
    n_windows: Optional[int] = None,
    tau: float = DEFAULT_TAU,
    epsilon: float = DEFAULT_EPSILON,
    rps_stable: float = DEFAULT_RPS_STABLE,
    rps_transient: float = DEFAULT_RPS_TRANSIENT,
) -> DataFrame:
    """Compute TempSupp, Drift, RPS and the lifecycle class for every rule.

    Args:
        rules: Per-window rule measures. Preferably the dense output of
            :func:`src.mining.arm.recount_candidates` (true values in every
            window); sparse per-window rules from
            :func:`src.mining.arm.mine_rules` are accepted and zero-padded.
        n_windows: ``|W|``, the total number of sliding windows (window ids are
            ``0..n-1``). Pass it from the windowing stage; if ``None`` it is
            inferred from the distinct ``window_id`` values present, which
            undercounts when some windows produced no rules.
        tau: RPS confidence threshold (Eq. 3.7).
        epsilon, rps_stable, rps_transient: Table 3.2 thresholds.

    Returns:
        One row per rule, ranked by ``persistence_score`` desc, then
        ``temporal_support`` desc, then ``rule_key``. Columns:
        ``rule_key, antecedent, consequent, lifecycle, temporal_support,
        confidence_drift, persistence_score, mean_confidence,
        confidence_volatility, occurrences, first_window, last_window,
        window_series, support_series, confidence_series, lift_series,
        emitted_series``. ``first_window``/``last_window`` are the first and
        last windows where the rule was emitted.

    Raises:
        ValueError: if the effective ``n_windows`` is not positive.
    """
    if n_windows is None:
        n_windows = rules.select("window_id").distinct().count()
    if n_windows <= 0:
        raise ValueError("n_windows must be positive.")
    n = int(n_windows)

    dense = _densify(rules, range(n))
    traj = rule_trajectories(dense)

    series = traj.select(
        "rule_key",
        "antecedent",
        "consequent",
        F.transform("obs", lambda x: x["window_id"]).alias("window_series"),
        F.transform("obs", lambda x: x["support"]).alias("support_series"),
        F.transform("obs", lambda x: x["confidence"]).alias("confidence_series"),
        F.transform("obs", lambda x: x["lift"]).alias("lift_series"),
        F.transform("obs", lambda x: x["emitted"]).alias("emitted_series"),
        F.transform(F.filter("obs", lambda x: x["emitted"]), lambda x: x["window_id"]).alias(
            "_emitted_windows"
        ),
        F.transform(F.filter("obs", lambda x: x["emitted"]), lambda x: x["confidence"]).alias(
            "_emitted_conf"
        ),
    )

    # First differences of the confidence trajectory, d_i = c_{i+1} - c_i.
    first_diffs = (
        "transform(sequence(1, size(confidence_series) - 1), "
        "i -> element_at(confidence_series, i + 1) - element_at(confidence_series, i))"
    )

    metrics = series.selectExpr(
        "rule_key",
        "antecedent",
        "consequent",
        # Eq. 3.5 -- temporal support.
        f"aggregate(support_series, 0.0D, (acc, x) -> acc + x) / {n}D "
        "as temporal_support",
        # Eq. 3.6 -- mean first difference of confidence over all |W| windows.
        f"CASE WHEN {n} > 1 THEN "
        f"aggregate({first_diffs}, 0.0D, (acc, x) -> acc + x) / {n - 1}D "
        "ELSE 0.0D END as confidence_drift",
        # Eq. 3.7 -- rule persistence score.
        f"size(filter(confidence_series, x -> x >= {tau}D)) / {n}D "
        "as persistence_score",
        # Supplementary descriptors.
        "CASE WHEN size(_emitted_conf) > 0 THEN "
        "aggregate(_emitted_conf, 0.0D, (acc, x) -> acc + x) / size(_emitted_conf) "
        "ELSE 0.0D END as mean_confidence",
        f"CASE WHEN {n} > 1 THEN "
        f"aggregate(transform({first_diffs}, d -> abs(d)), 0.0D, (acc, x) -> acc + x) "
        f"/ {n - 1}D ELSE 0.0D END as confidence_volatility",
        "size(_emitted_windows) as occurrences",
        "element_at(_emitted_windows, 1) as first_window",
        "element_at(_emitted_windows, -1) as last_window",
        "window_series",
        "support_series",
        "confidence_series",
        "lift_series",
        "emitted_series",
    )

    metrics = metrics.withColumn(
        "lifecycle",
        lifecycle_class(
            epsilon=epsilon, rps_stable=rps_stable, rps_transient=rps_transient
        ),
    )

    ordered = [
        "rule_key",
        "antecedent",
        "consequent",
        "lifecycle",
        "temporal_support",
        "confidence_drift",
        "persistence_score",
        "mean_confidence",
        "confidence_volatility",
        "occurrences",
        "first_window",
        "last_window",
        "window_series",
        "support_series",
        "confidence_series",
        "lift_series",
        "emitted_series",
    ]
    return metrics.select(*ordered).orderBy(
        F.col("persistence_score").desc(),
        F.col("temporal_support").desc(),
        F.col("rule_key").asc(),
    )
