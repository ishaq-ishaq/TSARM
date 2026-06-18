"""Temporal rule significance metrics for TSARM (research objective ii).

Given the per-window association rules from :mod:`src.mining` (one row per rule
per window in which it was frequent), this stage tracks each rule *across* the
window sequence and computes temporal significance metrics. Rules are matched
across windows by their order-independent ``rule_key``.

Notation
--------
Let ``W = {w_0, ..., w_{n-1}}`` be the full ordered set of sliding windows
(``n`` total). For a rule ``r`` let ``O(r) ⊆ W`` be the windows in which ``r``
was emitted by the miner (i.e. met the mining support/confidence thresholds),
with per-window support ``supp_w(r)`` and confidence ``conf_w(r)``. Windows
``w ∉ O(r)`` are treated as ``supp_w(r) = conf_w(r) = 0`` (the rule was not
frequent there).

Metrics
-------
* **Temporal support** -- time-averaged support over the whole horizon::

      TS(r) = (1/n) * Σ_{w ∈ W} supp_w(r)
            = (1/n) * Σ_{w ∈ O(r)} supp_w(r)

* **Mean confidence** -- average confidence over windows where ``r`` is present::

      mean_conf(r) = (1/|O(r)|) * Σ_{w ∈ O(r)} conf_w(r)

* **Temporal confidence drift** -- net change in confidence from the first to
  the last window in which ``r`` appears::

      drift(r)      = conf_{last}(r) - conf_{first}(r)
      drift_rate(r) = drift(r) / (|O(r)| - 1)          (0 if |O(r)| < 2)

  and **confidence volatility**, the mean absolute change between consecutive
  observed windows (captures instability that a single net drift hides)::

      vol(r) = (1/(|O(r)|-1)) * Σ_{i=1}^{|O(r)|-1} |conf_{i}(r) - conf_{i-1}(r)|

* **Rule Persistence Score** (from the project spec) -- fraction of *all*
  windows in which the rule's confidence stays at or above a threshold ``tau``::

      RPS(r) = (1/n) * |{ w ∈ W : conf_w(r) >= tau }|

  Because absent windows have ``conf_w(r) = 0``, this counts only windows where
  ``r`` was emitted *and* clears ``tau``; it therefore requires ``tau`` to be
  >= the mining ``min_confidence`` to be meaningful.

The output also carries the raw ``window_series`` / ``support_series`` /
``confidence_series`` arrays (ordered by window) so trajectories can be plotted
or fed to downstream temporal-embedding experiments.
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

DEFAULT_TAU = 0.6


def rule_trajectories(rules: DataFrame) -> DataFrame:
    """Collect each rule's per-window observations, ordered by ``window_id``.

    Args:
        rules: Per-window rules (output of :func:`src.mining.arm.mine_rules`);
            must have ``rule_key``, ``antecedent``, ``consequent``,
            ``window_id``, ``support`` and ``confidence``.

    Returns:
        One row per rule with column ``obs``: an array of
        ``(window_id, support, confidence)`` structs sorted ascending by
        ``window_id``.
    """
    return rules.groupBy("rule_key", "antecedent", "consequent").agg(
        F.sort_array(
            F.collect_list(F.struct("window_id", "support", "confidence"))
        ).alias("obs")
    )


def compute_metrics(
    rules: DataFrame,
    n_windows: Optional[int] = None,
    tau: float = DEFAULT_TAU,
) -> DataFrame:
    """Compute temporal significance metrics per rule.

    Args:
        rules: Per-window rules from the mining stage.
        n_windows: Total number of sliding windows ``n`` in the analysis. This
            should come from the windowing stage (``len(build_windows(...))``).
            If ``None``, it is inferred as the number of distinct ``window_id``
            values present in ``rules`` -- which *undercounts* if some windows
            produced no rules at all, so pass it explicitly for correct
            normalisation.
        tau: Confidence threshold for the Rule Persistence Score.

    Returns:
        One row per rule with the trajectory arrays and all metrics described in
        the module docstring, ordered by ``persistence_score`` descending.

    Raises:
        ValueError: if the effective ``n_windows`` is not positive.
    """
    if n_windows is None:
        n_windows = rules.select("window_id").distinct().count()
    if n_windows <= 0:
        raise ValueError("n_windows must be positive.")
    n = float(n_windows)

    traj = rule_trajectories(rules)

    # Materialise the ordered per-metric series so the SQL expressions below can
    # reference them by name.
    series = (
        traj.select(
            "rule_key",
            "antecedent",
            "consequent",
            F.transform("obs", lambda x: x["window_id"]).alias("window_series"),
            F.transform("obs", lambda x: x["support"]).alias("support_series"),
            F.transform("obs", lambda x: x["confidence"]).alias(
                "confidence_series"
            ),
        )
        .withColumn("occurrences", F.size("confidence_series"))
    )

    return series.selectExpr(
        "rule_key",
        "antecedent",
        "consequent",
        "window_series",
        "support_series",
        "confidence_series",
        "occurrences",
        "element_at(window_series, 1)  as first_window",
        "element_at(window_series, -1) as last_window",
        # Temporal support: time-averaged over all n windows.
        f"aggregate(support_series, 0.0D, (acc, x) -> acc + x) / {n}D "
        "as temporal_support",
        # Mean confidence over observed windows.
        "aggregate(confidence_series, 0.0D, (acc, x) -> acc + x) / occurrences "
        "as mean_confidence",
        "element_at(confidence_series, 1)  as first_confidence",
        "element_at(confidence_series, -1) as last_confidence",
        # Net confidence drift and its per-step rate.
        "element_at(confidence_series, -1) - element_at(confidence_series, 1) "
        "as confidence_drift",
        "CASE WHEN occurrences > 1 THEN "
        "(element_at(confidence_series, -1) - element_at(confidence_series, 1)) "
        "/ (occurrences - 1) ELSE 0.0D END as confidence_drift_rate",
        # Confidence volatility: mean |consecutive change|.
        "CASE WHEN occurrences > 1 THEN "
        "aggregate("
        "  transform(sequence(2, occurrences), "
        "            i -> abs(element_at(confidence_series, i) "
        "                     - element_at(confidence_series, i - 1))), "
        "  0.0D, (acc, x) -> acc + x) / (occurrences - 1) "
        "ELSE 0.0D END as confidence_volatility",
        # Rule Persistence Score: RPS(r) = (1/n)|{w : conf_w(r) >= tau}|.
        f"size(filter(confidence_series, x -> x >= {tau}D)) / {n}D "
        "as persistence_score",
    ).orderBy(F.col("persistence_score").desc(), F.col("rule_key").asc())
