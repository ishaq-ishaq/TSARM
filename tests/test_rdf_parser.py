"""Unit tests for the dependency-free N-Triples parser.

These run without Spark so they stay fast and CI-friendly.
"""

import pytest

from src.ingestion.rdf_parser import (
    BLANK,
    IRI,
    LITERAL,
    parse_ntriples,
    parse_ntriples_line,
)

XSD_DATE = "http://www.w3.org/2001/XMLSchema#date"


def test_iri_object():
    t = parse_ntriples_line(
        "<http://example.org/Alice> <http://ex.org/knows> <http://example.org/Bob> ."
    )
    assert t is not None
    assert t.subject == "http://example.org/Alice"
    assert t.predicate == "http://ex.org/knows"
    assert t.obj == "http://example.org/Bob"
    assert t.object_kind == IRI
    assert t.datatype is None and t.language is None


def test_typed_literal_object():
    t = parse_ntriples_line(
        f'<http://example.org/Alice> <http://ex.org/hireDate> "2023-01-15"^^<{XSD_DATE}> .'
    )
    assert t.object_kind == LITERAL
    assert t.obj == "2023-01-15"
    assert t.datatype == XSD_DATE


def test_language_literal_object():
    t = parse_ntriples_line(
        '<http://example.org/Alice> <http://www.w3.org/2000/01/rdf-schema#label> "Alice"@en .'
    )
    assert t.object_kind == LITERAL
    assert t.obj == "Alice"
    assert t.language == "en"


def test_blank_node_object():
    t = parse_ntriples_line("<http://ex.org/s> <http://ex.org/p> _:b0 .")
    assert t.object_kind == BLANK
    assert t.obj == "_:b0"


def test_escaped_literal():
    t = parse_ntriples_line(
        '<http://ex.org/s> <http://ex.org/p> "line1\\nline2 \\"q\\"" .'
    )
    assert t.obj == 'line1\nline2 "q"'


def test_comment_and_blank_lines_return_none():
    assert parse_ntriples_line("# a comment") is None
    assert parse_ntriples_line("   ") is None


def test_malformed_line_raises():
    with pytest.raises(ValueError):
        parse_ntriples_line("this is not a triple")


def test_stream_file_skips_comments(tmp_path):
    nt = tmp_path / "s.nt"
    nt.write_text(
        "# header comment\n"
        "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n"
        "\n"
        '<http://ex.org/a> <http://ex.org/label> "A"@en .\n',
        encoding="utf-8",
    )
    triples = list(parse_ntriples(nt))
    assert len(triples) == 2
    assert triples[0].object_kind == IRI
    assert triples[1].language == "en"


def test_stream_non_strict_skips_bad_line(tmp_path):
    nt = tmp_path / "bad.nt"
    nt.write_text(
        "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n"
        "garbage line\n",
        encoding="utf-8",
    )
    assert len(list(parse_ntriples(nt, strict=False))) == 1
    with pytest.raises(ValueError):
        list(parse_ntriples(nt, strict=True))
