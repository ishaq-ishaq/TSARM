"""Tests for loading an OWL class hierarchy and attaching it to snapshots."""

from datetime import datetime

import pytest

from src.ingestion.ontology import RDFS_SUBCLASS, load_subclass_edges, with_hierarchy

DBO = "http://dbpedia.org/ontology/"
OWL = """<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
         xmlns:owl="http://www.w3.org/2002/07/owl#">
  <owl:Class rdf:about="http://dbpedia.org/ontology/Agent">
    <rdfs:subClassOf rdf:resource="http://www.w3.org/2002/07/owl#Thing"/>
  </owl:Class>
  <owl:Class rdf:about="http://dbpedia.org/ontology/Person">
    <rdfs:subClassOf rdf:resource="http://dbpedia.org/ontology/Agent"/>
    <rdfs:subClassOf>
      <owl:Restriction>
        <owl:onProperty rdf:resource="http://dbpedia.org/ontology/name"/>
        <owl:minCardinality>1</owl:minCardinality>
      </owl:Restriction>
    </rdfs:subClassOf>
  </owl:Class>
  <owl:Class rdf:about="http://dbpedia.org/ontology/Loop">
    <rdfs:subClassOf rdf:resource="http://dbpedia.org/ontology/Loop"/>
  </owl:Class>
</rdf:RDF>
"""


def test_load_subclass_edges_skips_restrictions_and_self_loops(tmp_path):
    p = tmp_path / "onto.owl"
    p.write_text(OWL)
    assert load_subclass_edges(p) == [
        (DBO + "Agent", "http://www.w3.org/2002/07/owl#Thing"),
        (DBO + "Person", DBO + "Agent"),
    ]


def test_namespace_filter_keeps_own_hierarchy_only(tmp_path):
    p = tmp_path / "onto.owl"
    p.write_text(OWL)
    assert load_subclass_edges(p, namespace=DBO) == [(DBO + "Person", DBO + "Agent")]


def test_with_hierarchy_adds_edges_to_every_snapshot():
    from src.ingestion.spark_session import get_spark

    spark = get_spark(app_name="TSARM-ontology-tests", master="local[2]")
    try:
        triples = spark.createDataFrame(
            [("e1", "type", DBO + "Person", "iri", None, "2015-10", datetime(2015, 10, 1)),
             ("e1", "type", DBO + "Person", "iri", None, "2016-10", datetime(2016, 10, 1))],
            "subject string, predicate string, object string, object_kind string, language string, "
            "snapshot string, snapshot_ts timestamp",
        )
        out = with_hierarchy(spark, triples, [(DBO + "Person", DBO + "Agent")])
        assert out.columns == triples.columns
        added = {(r.snapshot, r.subject, r.object) for r in out.where(out.predicate == RDFS_SUBCLASS).collect()}
        assert added == {("2015-10", DBO + "Person", DBO + "Agent"), ("2016-10", DBO + "Person", DBO + "Agent")}
        assert with_hierarchy(spark, triples, []) is triples
    finally:
        spark.stop()
