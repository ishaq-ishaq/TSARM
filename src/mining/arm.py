"""Distributed semantic association rule mining for TSARM.

This stage mines association rules independently within each sliding window
produced by :mod:`src.windowing`, using Spark MLlib's FP-Growth. The per-window
support/confidence it emits is the raw material the metrics stage aggregates
into temporal significance (temporal support, confidence drift, persistence).

Semantic item generation
-------------------------
Following the "semantic item generation" objective, RDF triples are turned into
a transactional model:

* **Transaction** = an *entity* (the triple ``subject``), scoped to one window.
* **Item** = a ``predicate=object`` assertion about that entity, e.g.
  ``http://ex.org/worksAt=http://ex.org/AcmeCorp`` or ``...#type=...Engineer``.

So an entity's transaction is the set of semantic facts asserted about it within
the window. FP-Growth then finds co-occurring fact-sets and rules such as
``{type=Engineer} => {worksAt=AcmeCorp}``.

By default literal-valued objects are excluded: literals such as dates are
typically near-unique and make poor association items. Pass
``include_literals=True`` to keep them.

Outputs
-------
``build_transactions`` -> DataFrame ``(window_id, subject, items: array<string>)``.

``mine_rules`` -> two DataFrames:

* *itemsets*: ``window_id, items, freq, support`` (support relative to the
  window's transaction count).
* *rules*: ``window_id, antecedent, consequent, confidence, lift, support,
  rule_key`` where ``rule_key`` is a stable, order-independent identifier so the
  same rule can be tracked across windows by the metrics stage.
"""

from __future__ import annotations

from typing import List, Tuple

from pyspark.ml.fpm import FPGrowth
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

ITEM_SEP = "="  # joins predicate and object into a single item token

LITERAL_KIND = "literal"


def build_transactions(
    windowed_df: DataFrame, include_literals: bool = False
) -> DataFrame:
    """Generate per-window, per-entity transactions of semantic items.

    Args:
        windowed_df: Window-tagged triples (output of
            :func:`src.windowing.sliding_window.assign_windows`); must have
            ``window_id``, ``subject``, ``predicate``, ``object`` and
            ``object_kind`` columns.
        include_literals: If ``False`` (default), drop triples with literal
            objects before building items.

    Returns:
        DataFrame ``(window_id, subject, items)`` where ``items`` is a
        duplicate-free ``array<string>`` of ``predicate=object`` tokens. The
        de-duplication (via ``collect_set``) also satisfies FP-Growth's
        requirement that transactions contain no repeated items.
    """
    df = windowed_df
    if not include_literals:
        df = df.where(F.col("object_kind") != LITERAL_KIND)

    items = df.withColumn(
        "item", F.concat_ws(ITEM_SEP, F.col("predicate"), F.col("object"))
    )
    return items.groupBy("window_id", "subject").agg(
        F.collect_set("item").alias("items")
    )


def _rule_key(antecedent: Column, consequent: Column) -> Column:
    """Order-independent rule identifier: sorted(ante) '=>' sorted(cons)."""
    ante = F.concat_ws(",", F.array_sort(antecedent))
    cons = F.concat_ws(",", F.array_sort(consequent))
    return F.concat(ante, F.lit(" => "), cons)


def _window_ids(transactions: DataFrame) -> List[int]:
    rows = transactions.select("window_id").distinct().collect()
    return sorted(r["window_id"] for r in rows)


def mine_rules(
    transactions: DataFrame,
    min_support: float = 0.3,
    min_confidence: float = 0.6,
) -> Tuple[DataFrame, DataFrame]:
    """Mine frequent itemsets and association rules within each window.

    FP-Growth has no native "group by" mode, so each window is mined as an
    independent FP-Growth fit over that window's transactions. The window count
    equals the number of sliding windows (small), so the driver-side loop is
    cheap; each fit itself runs distributed.

    Args:
        transactions: Output of :func:`build_transactions`.
        min_support: Minimum *relative* support (fraction of the window's
            transactions) for an itemset to be frequent.
        min_confidence: Minimum confidence for a generated rule.

    Returns:
        ``(itemsets_df, rules_df)`` -- see module docstring for schemas. Both
        carry a ``window_id`` column. Windows yielding no frequent itemsets
        simply contribute no rows.

    Raises:
        ValueError: if ``transactions`` contains no windows.
    """
    window_ids = _window_ids(transactions)
    if not window_ids:
        raise ValueError("No windows to mine; `transactions` is empty.")

    itemset_frames: List[DataFrame] = []
    rule_frames: List[DataFrame] = []

    for wid in window_ids:
        window_tx = transactions.where(F.col("window_id") == wid).select("items")
        # Cache: FP-Growth makes multiple passes over the transactions.
        window_tx = window_tx.cache()

        fpgrowth = FPGrowth(
            itemsCol="items",
            minSupport=min_support,
            minConfidence=min_confidence,
        )
        model = fpgrowth.fit(window_tx)

        n_tx = window_tx.count()
        itemsets = model.freqItemsets.withColumn(
            "window_id", F.lit(wid)
        ).withColumn(
            "support", F.col("freq") / F.lit(float(n_tx))
        )
        rules = model.associationRules.withColumn("window_id", F.lit(wid))

        itemset_frames.append(itemsets)
        rule_frames.append(rules)
        window_tx.unpersist()

    itemsets_df = itemset_frames[0]
    for frame in itemset_frames[1:]:
        itemsets_df = itemsets_df.unionByName(frame)

    rules_df = rule_frames[0]
    for frame in rule_frames[1:]:
        rules_df = rules_df.unionByName(frame)

    rules_df = rules_df.withColumn(
        "rule_key", _rule_key(F.col("antecedent"), F.col("consequent"))
    )

    # Stable column order for downstream stages.
    itemsets_df = itemsets_df.select("window_id", "items", "freq", "support")
    rules_df = rules_df.select(
        "window_id",
        "rule_key",
        "antecedent",
        "consequent",
        "confidence",
        "lift",
        "support",
    )
    return itemsets_df, rules_df


def mine(
    windowed_df: DataFrame,
    min_support: float = 0.3,
    min_confidence: float = 0.6,
    include_literals: bool = False,
) -> Tuple[DataFrame, DataFrame]:
    """Convenience wrapper: build transactions then mine per-window rules."""
    transactions = build_transactions(windowed_df, include_literals=include_literals)
    return mine_rules(
        transactions, min_support=min_support, min_confidence=min_confidence
    )
