"""Ontology-grounded semantic item generation (proposal Algorithm 3.1, §3.6.3).

In the proposal's semantic item model, an entity ``e`` in window ``w`` contributes
the transaction ``C(e)``: the ontological classes it belongs to, *including
every superclass* reachable through the class hierarchy. Rules are therefore
expressed at the level of types (e.g. ``{Scientist} => {Award_Winner}``)
rather than individuals.

Class membership comes from ``rdf:type`` (DBpedia) or ``wdt:P31`` (Wikidata),
and the hierarchy from ``rdfs:subClassOf`` / ``wdt:P279`` triples **inside the
same window**, so the ontology itself may evolve over time.

The class-hierarchy closure (CLASS_HIERARCHY in Algorithm 3.1) is provided in
two interchangeable ways:

* :func:`subclass_closure_sparql`: a SPARQL 1.1 property-path query
  (``?c (rdfs:subClassOf)+ ?a``), as specified in §3.6.3. It runs in memory with
  RDFLib (Apache Jena ARQ evaluates the identical query) and suits ontologies
  of up to a few hundred thousand subclass edges, e.g. the DBpedia ontology.
* :func:`subclass_closure_spark`: a distributed iterative transitive closure
  (semi-naive evaluation) for large hierarchies such as Wikidata's millions of
  P279 edges.

Superclass items make taxonomic rules (``{Person} => {Agent}``) hold with
confidence 1 by construction. :func:`drop_taxonomic_rules` removes rules whose
consequent is implied by the antecedent through the hierarchy (Srikant &
Agrawal, 1995, generalized association rules).
"""

from __future__ import annotations

from typing import Iterable, Optional, Sequence, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_SUBCLASS = "http://www.w3.org/2000/01/rdf-schema#subClassOf"
WDT_P31 = "http://www.wikidata.org/prop/direct/P31"
WDT_P279 = "http://www.wikidata.org/prop/direct/P279"

TYPE_PREDICATES: Tuple[str, ...] = (RDF_TYPE, WDT_P31)
SUBCLASS_PREDICATES: Tuple[str, ...] = (RDFS_SUBCLASS, WDT_P279)


# ---------------------------------------------------------------------------
# Class-hierarchy closure
# ---------------------------------------------------------------------------


def subclass_edges(
    windowed_df: DataFrame, subclass_predicates: Sequence[str] = SUBCLASS_PREDICATES
) -> DataFrame:
    """``(window_id, cls, parent)`` direct subclass edges per window (no self-loops)."""
    return (
        windowed_df.where(F.col("predicate").isin(list(subclass_predicates)))
        .where(F.col("object_kind") == "iri")
        .where(F.col("subject") != F.col("object"))
        .select("window_id", F.col("subject").alias("cls"), F.col("object").alias("parent"))
        .distinct()
    )


def subclass_closure_spark(edges: DataFrame, max_depth: int = 64) -> DataFrame:
    """Transitive closure ``(window_id, cls, ancestor)`` by semi-naive iteration.

    Each round extends only the pairs discovered in the previous round (the
    *delta*) by one edge, so work is proportional to new facts. Stops at a fixpoint
    or after ``max_depth`` rounds. Cycles in the hierarchy are harmless: pairs
    are de-duplicated against everything found so far.
    """
    e = edges.select("window_id", "cls", F.col("parent").alias("ancestor")).distinct()
    closure = e.cache()
    delta = e
    for _ in range(max_depth):
        step = (
            delta.alias("d")
            .join(
                e.alias("e"),
                (F.col("d.window_id") == F.col("e.window_id"))
                & (F.col("d.ancestor") == F.col("e.cls")),
            )
            .select(F.col("d.window_id"), F.col("d.cls"), F.col("e.ancestor"))
            .where(F.col("cls") != F.col("ancestor"))
            .distinct()
        )
        new = step.join(closure, ["window_id", "cls", "ancestor"], "left_anti").cache()
        if new.limit(1).count() == 0:
            new.unpersist()
            break
        closure = closure.unionByName(new).cache()
        delta = new
    return closure


SPARQL_CLOSURE = """
SELECT DISTINCT ?c ?a WHERE {{
    ?c ({path})+ ?a .
    FILTER (?c != ?a)
}}
"""


def subclass_closure_sparql(
    edges: Iterable[Tuple[str, str]], subclass_predicate: str = RDFS_SUBCLASS
) -> set:
    """Transitive closure of ``(cls, parent)`` pairs via a SPARQL property path.

    Builds an in-memory RDF graph of the subclass edges and evaluates
    ``SELECT ?c ?a WHERE { ?c (<subClassOf>)+ ?a }``. Returns ``{(cls, ancestor)}``.
    """
    from rdflib import Graph, URIRef

    g = Graph()
    pred = URIRef(subclass_predicate)
    for cls, parent in edges:
        g.add((URIRef(cls), pred, URIRef(parent)))
    query = SPARQL_CLOSURE.format(path=f"<{subclass_predicate}>")
    return {(str(row.c), str(row.a)) for row in g.query(query)}


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------


def build_class_transactions(
    windowed_df: DataFrame,
    include_superclasses: bool = True,
    closure: Optional[DataFrame] = None,
    type_predicates: Sequence[str] = TYPE_PREDICATES,
    subclass_predicates: Sequence[str] = SUBCLASS_PREDICATES,
) -> DataFrame:
    """Per-window, per-entity transactions of ontological classes.

    Args:
        windowed_df: Window-tagged triples (``window_id, subject, predicate,
            object, object_kind``).
        include_superclasses: Add every ancestor class (Algorithm 3.1).
        closure: Precomputed ``(window_id, cls, ancestor)``; computed from the
            window's own subclass triples when omitted.

    Returns:
        ``(window_id, subject, items)`` with ``items`` the duplicate-free array
        of class IRIs, the same schema as
        :func:`src.mining.arm.build_transactions`, so it plugs into
        :func:`src.mining.arm.mine_rules` unchanged.
    """
    types = (
        windowed_df.where(F.col("predicate").isin(list(type_predicates)))
        .where(F.col("object_kind") == "iri")
        .select("window_id", "subject", F.col("object").alias("cls"))
        .distinct()
    )
    memberships = types
    if include_superclasses:
        if closure is None:
            closure = subclass_closure_spark(subclass_edges(windowed_df, subclass_predicates))
        inherited = types.join(closure, ["window_id", "cls"]).select(
            "window_id", "subject", F.col("ancestor").alias("cls")
        )
        memberships = types.unionByName(inherited)
    return memberships.groupBy("window_id", "subject").agg(
        F.array_sort(F.collect_set("cls")).alias("items")
    )


def drop_taxonomic_rules(rules: DataFrame, closure: DataFrame) -> DataFrame:
    """Remove rules whose every consequent class is an ancestor of an antecedent class.

    Such rules (``{Person} => {Agent}``) hold by definition once superclasses
    are items. ``closure`` is ``(window_id, cls, ancestor)``. A rule counts as
    taxonomic if it is taxonomic in any window: rule identity (``rule_key``) is
    shared across windows, and the hierarchy rarely loses edges.
    """
    exploded = rules.select(
        "rule_key",
        F.explode("antecedent").alias("cls"),
        F.col("consequent"),
    ).distinct()
    implied = (
        exploded.join(closure.select("cls", "ancestor").distinct(), "cls")
        .groupBy("rule_key", "consequent")
        .agg(F.collect_set("ancestor").alias("_implied"))
        .where(F.size(F.array_except("consequent", "_implied")) == 0)
        .select("rule_key")
        .distinct()
    )
    return rules.join(implied, "rule_key", "left_anti")
