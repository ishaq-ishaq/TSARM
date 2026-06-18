"""RDF parsing utilities for TSARM ingestion.

Two parsing paths are provided:

* :func:`parse_ntriples` -- a fast, dependency-free, streaming line parser for
  the N-Triples (``.nt``) format. N-Triples is line-oriented (one triple per
  line), which makes it the format of choice for large dumps because it can be
  read without materialising the whole graph in memory. This is the hot path
  for the Wikidata/DBpedia snapshots.

* :func:`parse_with_rdflib` -- an RDFLib-backed parser for richer serialisations
  (Turtle, RDF/XML, JSON-LD, ...). Convenient for small samples but loads the
  whole graph into memory, so it is unsuitable for multi-GB dumps.

Both yield :class:`ParsedTriple` records with object typing preserved, which the
ingestion layer needs to detect temporal literals (xsd:date / xsd:dateTime).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional, Union

# Object kinds, kept as short strings so they survive cleanly into Parquet.
IRI = "iri"
BLANK = "blank"
LITERAL = "literal"


@dataclass(frozen=True)
class ParsedTriple:
    """A single RDF statement with enough typing to support temporal logic."""

    subject: str
    predicate: str
    obj: str
    object_kind: str  # one of IRI / BLANK / LITERAL
    datatype: Optional[str] = None  # IRI of the literal datatype, if any
    language: Optional[str] = None  # BCP-47 language tag, for lang-strings


# --- N-Triples regex ---------------------------------------------------------
# A term is an IRI <...>, a blank node _:label, or a literal "..." with an
# optional ^^<datatype> or @lang suffix. We capture each component so the object
# can be typed. Escaped quotes inside literals are handled by the [^"\\] / \\.
# alternation.
_IRIREF = r"<([^>]*)>"
_BLANK = r"(_:[A-Za-z0-9_][A-Za-z0-9_.-]*)"
_LITERAL = r'"((?:[^"\\]|\\.)*)"(?:\^\^<([^>]*)>|@([A-Za-z]+(?:-[A-Za-z0-9]+)*))?'

_TERM = rf"(?:{_IRIREF}|{_BLANK})"
_NT_LINE = re.compile(
    rf"^\s*{_TERM}\s+{_IRIREF}\s+(?:{_TERM}|{_LITERAL})\s*\.\s*$"
)

# Unescaping map for N-Triples literal escapes.
_ESCAPES = {"\\": "\\", '"': '"', "n": "\n", "r": "\r", "t": "\t"}


def _unescape(value: str) -> str:
    """Resolve N-Triples backslash escapes (incl. \\uXXXX / \\UXXXXXXXX)."""
    out = []
    i = 0
    n = len(value)
    while i < n:
        ch = value[i]
        if ch == "\\" and i + 1 < n:
            nxt = value[i + 1]
            if nxt in _ESCAPES:
                out.append(_ESCAPES[nxt])
                i += 2
                continue
            if nxt == "u":
                out.append(chr(int(value[i + 2 : i + 6], 16)))
                i += 6
                continue
            if nxt == "U":
                out.append(chr(int(value[i + 2 : i + 10], 16)))
                i += 10
                continue
        out.append(ch)
        i += 1
    return "".join(out)


def parse_ntriples_line(line: str) -> Optional[ParsedTriple]:
    """Parse a single N-Triples line into a :class:`ParsedTriple`.

    Returns ``None`` for blank lines and comments (lines starting with ``#``).
    Raises :class:`ValueError` for malformed, non-comment content so that data
    quality problems surface instead of being silently dropped.
    """
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None

    match = _NT_LINE.match(line)
    if not match:
        raise ValueError(f"Malformed N-Triples line: {line!r}")

    (s_iri, s_blank, p_iri, o_iri, o_blank, o_lit, o_dtype, o_lang) = match.groups()

    subject = s_iri if s_iri is not None else s_blank
    predicate = p_iri

    if o_iri is not None:
        return ParsedTriple(subject, predicate, o_iri, IRI)
    if o_blank is not None:
        return ParsedTriple(subject, predicate, o_blank, BLANK)
    # Literal object.
    return ParsedTriple(
        subject,
        predicate,
        _unescape(o_lit),
        LITERAL,
        datatype=o_dtype,
        language=o_lang,
    )


def parse_ntriples(
    path: Union[str, Path], strict: bool = False
) -> Iterator[ParsedTriple]:
    """Stream :class:`ParsedTriple` records from an N-Triples file.

    Args:
        path: Path to a ``.nt`` file (UTF-8).
        strict: If ``True``, a malformed line raises; if ``False`` (default),
            malformed lines are skipped so a single bad row does not abort a
            multi-million-line dump. Counts are the caller's concern.

    Yields:
        One :class:`ParsedTriple` per valid statement.
    """
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            try:
                triple = parse_ntriples_line(line)
            except ValueError:
                if strict:
                    raise
                continue
            if triple is not None:
                yield triple


def parse_with_rdflib(
    path: Union[str, Path], fmt: Optional[str] = None
) -> Iterator[ParsedTriple]:
    """Parse any RDFLib-supported serialisation into :class:`ParsedTriple`.

    Loads the whole graph into memory; intended for small samples and formats
    that are not line-oriented (Turtle, RDF/XML, JSON-LD). For large ``.nt``
    dumps prefer :func:`parse_ntriples`.

    Args:
        path: Path to the RDF file.
        fmt: RDFLib format string (e.g. ``"turtle"``). If ``None``, RDFLib
            guesses from the file extension.
    """
    from rdflib import BNode, Graph, Literal, URIRef

    graph = Graph()
    graph.parse(str(path), format=fmt)

    for s, p, o in graph:
        subject = str(s)
        predicate = str(p)
        if isinstance(o, Literal):
            yield ParsedTriple(
                subject,
                predicate,
                str(o),
                LITERAL,
                datatype=str(o.datatype) if o.datatype else None,
                language=o.language,
            )
        elif isinstance(o, BNode):
            yield ParsedTriple(subject, predicate, f"_:{o}", BLANK)
        else:  # URIRef
            assert isinstance(o, URIRef)
            yield ParsedTriple(subject, predicate, str(o), IRI)
