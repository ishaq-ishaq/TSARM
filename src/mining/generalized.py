"""Length-bounded generalized itemset mining with ancestor pruning (Spark).

Class items with their superclasses (Algorithm 3.1) break FP-Growth: an
ancestor always co-occurs with its subclass, so for an entity whose classes
have *k* ancestors every subset of the ancestor chain is a frequent itemset,
and Spark MLlib's FP-Growth (which has neither a length bound nor a way to
prune such itemsets) enumerates all of them. On three DBpedia releases with the
DBpedia ontology's own hierarchy (depth 6) it exhausted a 12 GB heap.

This module mines the same rules level-wise, in the manner of Apriori
(Agrawal and Srikant, 1994), with two changes that bound the cost:

* **Ancestor pruning** (Srikant and Agrawal, 1995): an itemset containing a
  class and one of its ancestors is never generated. Its support equals that of
  the itemset without the ancestor, so it is redundant, and every rule it would
  yield is either taxonomic ({Person} => {Agent}) or a restatement of a rule
  over the more specific class.
* **A length bound** ``max_len`` (default 3: rules with up to two antecedent
  items and one consequent, the same bound used for the AMIE+ and RDFRules
  baselines).

Each level is a distributed group-by over item combinations generated inside
each transaction with Spark's higher-order array functions; nothing is
collected to the driver. Output schemas match :func:`src.mining.arm.mine_rules`
(rules have one consequent item, as MLlib's FP-Growth produces), so
:func:`src.mining.arm.recount_candidates` and the metrics stage apply unchanged.
On data without a hierarchy the rules equal FP-Growth's rules of length at most
``max_len`` (checked in the tests).
"""

from __future__ import annotations

from typing import Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

_PAIRS = "flatten(transform(sequence(0, size(items) - 2), i -> transform(sequence(i + 1, size(items) - 1), j -> array(items[i], items[j]))))"
_TRIPLES = (
    "flatten(flatten(transform(sequence(0, size(items) - 3), i -> transform(sequence(i + 1, size(items) - 2), "
    "j -> transform(sequence(j + 1, size(items) - 1), k -> array(items[i], items[j], items[k]))))))"
)


def _count(combos: DataFrame, n_tx: DataFrame, min_support: float) -> DataFrame:
    return (
        combos.groupBy("window_id", "items").agg(F.count("*").alias("freq"))
        .join(n_tx, "window_id")
        .withColumn("support", F.col("freq") / F.col("n_tx"))
        .where(F.col("support") >= F.lit(min_support))
        .select("window_id", "items", "freq", "support")
    )


def mine_generalized(
    transactions: DataFrame,
    min_support: float,
    min_confidence: float,
    max_len: int = 3,
    closure: Optional[DataFrame] = None,
) -> Tuple[DataFrame, DataFrame]:
    """Frequent itemsets (size <= ``max_len``) and single-consequent rules per window.

    Args:
        transactions: ``(window_id, subject, items)``.
        closure: ``(window_id, cls, ancestor)``; itemsets containing a pair
            related by it are pruned. ``None`` disables pruning.

    Returns:
        ``(itemsets, rules)`` with the schemas of :func:`src.mining.arm.mine_rules`.
    """
    from src.mining.arm import _rule_key

    if max_len not in (2, 3):
        raise ValueError("max_len must be 2 or 3")
    tx = transactions.select("window_id", "subject", F.array_sort(F.array_distinct("items")).alias("items"))
    n_tx = tx.groupBy("window_id").agg(F.count("*").alias("n_tx")).localCheckpoint()

    single = tx.select("window_id", F.explode("items").alias("_i")).select("window_id", F.array("_i").alias("items"))
    l1 = _count(single, n_tx, min_support).localCheckpoint()

    # Keep only frequent items in each transaction (anti-monotonicity).
    freq_items = l1.select("window_id", F.col("items")[0].alias("item"))
    tx = (
        tx.select("window_id", "subject", F.explode("items").alias("item"))
        .join(freq_items, ["window_id", "item"])
        .groupBy("window_id", "subject")
        .agg(F.array_sort(F.collect_set("item")).alias("items"))
        .where(F.size("items") >= 2)
        .localCheckpoint()
    )

    pairs = tx.select("window_id", F.explode(F.expr(_PAIRS)).alias("items"))
    if closure is not None:
        rel = closure.select("window_id", F.col("cls").alias("a"), F.col("ancestor").alias("b"))
        rel = rel.unionByName(rel.select("window_id", F.col("b").alias("a"), F.col("a").alias("b")))
        rel = rel.select("window_id", F.array_sort(F.array("a", "b")).alias("items")).distinct()
        pairs = pairs.join(rel, ["window_id", "items"], "left_anti")
    l2 = _count(pairs, n_tx, min_support).localCheckpoint()
    levels = [l1, l2]

    if max_len >= 3:
        # Candidate triples: every 2-subset must be a frequent (and unpruned) pair.
        trip = tx.where(F.size("items") >= 3).select("window_id", F.explode(F.expr(_TRIPLES)).alias("items"))
        keys = l2.select("window_id", F.col("items").alias("_p"))
        for a, b in ((0, 1), (0, 2), (1, 2)):
            sub = F.array(F.col("items")[a], F.col("items")[b])
            trip = trip.withColumn("_sub", sub).join(
                keys.withColumnRenamed("_p", "_sub"), ["window_id", "_sub"], "left_semi"
            ).drop("_sub")
        levels.append(_count(trip, n_tx, min_support).localCheckpoint())

    itemsets = levels[0]
    for lv in levels[1:]:
        itemsets = itemsets.unionByName(lv)

    # Rules: for each itemset of size >= 2, each item in turn is the consequent.
    multi = itemsets.where(F.size("items") >= 2)
    cand = multi.select(
        "window_id", F.col("items").alias("_full"), F.col("freq").alias("_freq"),
        F.col("support"), F.explode("items").alias("_cons"),
    ).withColumn("antecedent", F.array_sort(F.array_except("_full", F.array("_cons"))))
    ante = itemsets.select("window_id", F.col("items").alias("antecedent"), F.col("freq").alias("_afreq"))
    cons = l1.select("window_id", F.col("items")[0].alias("_cons"), F.col("support").alias("_csupp"))
    rules = (
        cand.join(ante, ["window_id", "antecedent"])
        .join(cons, ["window_id", "_cons"])
        .withColumn("confidence", F.col("_freq") / F.col("_afreq"))
        .where(F.col("confidence") >= F.lit(min_confidence))
        .withColumn("lift", F.col("confidence") / F.col("_csupp"))
        .withColumn("consequent", F.array("_cons"))
        .withColumn("rule_key", _rule_key(F.col("antecedent"), F.col("consequent")))
        .select("window_id", "rule_key", "antecedent", "consequent", "confidence", "lift", "support")
    )
    return itemsets.select("window_id", "items", "freq", "support"), rules
