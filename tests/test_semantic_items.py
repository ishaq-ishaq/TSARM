"""Tests for ontology-grounded semantic item generation (Algorithm 3.1)."""

import pytest
from pyspark.sql import types as T

from src.ingestion.spark_session import get_spark
from src.mining.arm import mine_temporal
from src.mining.semantic_items import (
    RDF_TYPE,
    RDFS_SUBCLASS,
    WDT_P31,
    WDT_P279,
    build_class_transactions,
    subclass_closure_spark,
    subclass_closure_sparql,
    subclass_edges,
)

EX = "http://ex.org/"
AGENT, PERSON, SCI, PHYS, ORG, AWARD = (EX + n for n in ("Agent", "Person", "Scientist", "Physicist", "Org", "AwardWinner"))

ONTOLOGY = [(SCI, PERSON), (PERSON, AGENT), (PHYS, SCI), (ORG, AGENT)]

_SCHEMA = T.StructType(
    [
        T.StructField("subject", T.StringType(), False),
        T.StructField("predicate", T.StringType(), False),
        T.StructField("object", T.StringType(), False),
        T.StructField("object_kind", T.StringType(), False),
        T.StructField("window_id", T.IntegerType(), False),
    ]
)


@pytest.fixture(scope="module")
def spark():
    session = get_spark(app_name="TSARM-semantic-items-tests", master="local[2]")
    yield session
    session.stop()


def _df(spark, rows):
    return spark.createDataFrame(rows, schema=_SCHEMA)


def _ontology_rows(window_id, edges=ONTOLOGY, pred=RDFS_SUBCLASS):
    return [(c, pred, p, "iri", window_id) for c, p in edges]


def _closure_set(df):
    return {(r.cls, r.ancestor) for r in df.collect()}


EXPECTED_CLOSURE = {
    (SCI, PERSON), (SCI, AGENT), (PERSON, AGENT),
    (PHYS, SCI), (PHYS, PERSON), (PHYS, AGENT), (ORG, AGENT),
}


def test_sparql_closure():
    assert subclass_closure_sparql(ONTOLOGY) == EXPECTED_CLOSURE


def test_spark_closure_matches_sparql(spark):
    edges = subclass_edges(_df(spark, _ontology_rows(0)))
    assert _closure_set(subclass_closure_spark(edges)) == subclass_closure_sparql(ONTOLOGY)


def test_spark_closure_handles_cycles_and_per_window_hierarchies(spark):
    rows = _ontology_rows(0, [(EX + "A", EX + "B"), (EX + "B", EX + "A")])
    rows += _ontology_rows(1, [(PHYS, SCI)])  # window 1 lacks Scientist ⊑ Person
    rows += _ontology_rows(1, [(SCI, PERSON)], pred=WDT_P279)  # ...but has it via P279
    got = {(r.window_id, r.cls, r.ancestor) for r in subclass_closure_spark(subclass_edges(_df(spark, rows))).collect()}
    assert got == {
        (0, EX + "A", EX + "B"),
        (0, EX + "B", EX + "A"),
        (1, PHYS, SCI),
        (1, SCI, PERSON),
        (1, PHYS, PERSON),
    }


def test_class_transactions_include_superclasses(spark):
    rows = _ontology_rows(0) + [
        ("e1", RDF_TYPE, PHYS, "iri", 0),
        ("e2", WDT_P31, ORG, "iri", 0),  # Wikidata-style typing works too
        ("e3", EX + "name", "Ann", "literal", 0),  # untyped entity -> no transaction
    ]
    tx = {r.subject: set(r.items) for r in build_class_transactions(_df(spark, rows)).collect()}
    assert tx == {"e1": {PHYS, SCI, PERSON, AGENT}, "e2": {ORG, AGENT}}

    direct = {
        r.subject: set(r.items)
        for r in build_class_transactions(_df(spark, rows), include_superclasses=False).collect()
    }
    assert direct == {"e1": {PHYS}, "e2": {ORG}}


def test_class_mining_finds_semantic_rule_and_drops_taxonomic_ones(spark):
    rows = _ontology_rows(0) + _ontology_rows(1)
    for i in range(6):
        rows.append((f"p{i}", RDF_TYPE, PHYS, "iri", 0))
        rows.append((f"p{i}", RDF_TYPE, PHYS, "iri", 1))
        rows.append((f"p{i}", RDF_TYPE, AWARD, "iri", 0))  # physicists win awards in w0
        if i < 2:
            rows.append((f"p{i}", RDF_TYPE, AWARD, "iri", 1))  # ...less so in w1
    for i in range(4):
        rows.append((f"o{i}", RDF_TYPE, ORG, "iri", 0))
        rows.append((f"o{i}", RDF_TYPE, ORG, "iri", 1))

    _, rules, cands = mine_temporal(
        _df(spark, rows), window_ids=[0, 1], min_support=0.2, min_confidence=0.6, item_mode="class"
    )
    keys = {r.rule_key for r in rules.collect()}
    assert f"{PHYS} => {AWARD}" in keys
    # Taxonomic consequences of the hierarchy are pruned.
    for taxonomic in (f"{PHYS} => {SCI}", f"{PHYS} => {AGENT}", f"{ORG} => {AGENT}"):
        assert taxonomic not in keys
    # A non-taxonomic rule with a superclass in the antecedent survives
    # ({Person} => {AwardWinner}), as does one ending in a class that is not an ancestor.
    assert f"{PERSON} => {AWARD}" in keys

    traj = {r.window_id: r.confidence for r in cands.where(cands.rule_key == f"{PHYS} => {AWARD}").collect()}
    assert traj == {0: pytest.approx(1.0), 1: pytest.approx(2 / 6)}


def test_rejects_unknown_item_mode(spark):
    with pytest.raises(ValueError):
        mine_temporal(_df(spark, _ontology_rows(0)), [0], item_mode="nonsense")


def test_superclass_depth_limits_ancestors(spark):
    rows = _ontology_rows(0)
    for i in range(5):
        rows.append((f"p{i}", RDF_TYPE, PHYS, "iri", 0))
        rows.append((f"p{i}", RDF_TYPE, AWARD, "iri", 0))
    for i in range(3):
        rows.append((f"o{i}", RDF_TYPE, ORG, "iri", 0))
    _, rules, _ = mine_temporal(
        _df(spark, rows), [0], min_support=0.2, min_confidence=0.6,
        item_mode="class", superclass_depth=1,
    )
    keys = {r.rule_key for r in rules.collect()}
    # depth 1: Physicist gains Scientist (direct parent) but not Person / Agent.
    assert f"{PHYS} => {AWARD}" in keys and f"{SCI} => {AWARD}" in keys
    assert f"{PERSON} => {AWARD}" not in keys
    with pytest.raises(ValueError):
        mine_temporal(_df(spark, rows), [0], item_mode="class", superclass_depth=0)
