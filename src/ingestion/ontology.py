"""Load an ontology's class hierarchy and attach it to every snapshot.

Instance dumps such as DBpedia's ``instance_types`` carry only ``rdf:type``
statements; the class hierarchy (``rdfs:subClassOf``) is published separately
as an OWL ontology. Class-mode mining (:mod:`src.mining.semantic_items`) reads
the hierarchy from the windowed triples, so the ontology's subclass edges are
added to every snapshot here, in the same schema as the ingested triples.
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Tuple, Union

RDFS_SUBCLASS = "http://www.w3.org/2000/01/rdf-schema#subClassOf"


def load_subclass_edges(
    path: Union[str, Path], fmt: Optional[str] = None, namespace: Optional[str] = None
) -> List[Tuple[str, str]]:
    """``(class, parent)`` IRI pairs of ``rdfs:subClassOf`` in an ontology file.

    Blank-node parents (OWL restrictions) are skipped, as are self-loops. With
    ``namespace``, only edges whose class *and* parent are in that namespace
    are kept: the DBpedia ontology, for example, also declares its classes
    subclasses of schema.org, DOLCE and Wikidata classes, which are mappings to
    other vocabularies rather than part of its own hierarchy.
    ``fmt`` is an RDFLib format name; by default it is guessed from the file
    extension (``.owl`` is read as RDF/XML).
    """
    from rdflib import Graph, URIRef
    from rdflib.namespace import RDFS

    path = Path(path)
    if fmt is None:
        fmt = {".owl": "xml", ".rdf": "xml", ".ttl": "turtle", ".nt": "nt"}.get(path.suffix.lower())
    g = Graph()
    g.parse(str(path), format=fmt)
    edges = {
        (str(c), str(p))
        for c, p in g.subject_objects(RDFS.subClassOf)
        if isinstance(c, URIRef) and isinstance(p, URIRef) and c != p
        and (namespace is None or (str(c).startswith(namespace) and str(p).startswith(namespace)))
    }
    return sorted(edges)


def with_hierarchy(spark, triples, edges: List[Tuple[str, str]]):
    """``triples`` plus the subclass ``edges`` repeated in every snapshot.

    The added rows have ``predicate = rdfs:subClassOf`` and ``object_kind =
    'iri'``; any other columns of ``triples`` are null.
    """
    from pyspark.sql import functions as F

    if not edges:
        return triples
    edge_df = spark.createDataFrame(edges, "subject string, object string")
    snaps = triples.select("snapshot", "snapshot_ts").distinct()
    added = edge_df.crossJoin(snaps).withColumn("predicate", F.lit(RDFS_SUBCLASS)).withColumn(
        "object_kind", F.lit("iri")
    )
    cols = []
    for field in triples.schema.fields:
        if field.name in added.columns:
            cols.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            cols.append(F.lit(None).cast(field.dataType).alias(field.name))
    return triples.unionByName(added.select(cols))
