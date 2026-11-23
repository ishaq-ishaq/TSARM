"""Tests for the AMIE wrapper's pooling and output parsing (no Java needed)."""

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("amie_mine", ROOT / "scripts" / "amie_mine.py")
amie_mine = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(amie_mine)

AMIE_STDOUT = """Assuming 9 as type relation
Loading files...
Starting the mining phase... Rule\tHead Coverage\tStd Confidence\tPCA Confidence\tPositive Examples\tBody size\tPCA Body size\tFunctional variable
Using 4 threads
?a  <http://ex.org/worksAt>  <http://ex.org/Org0>   => ?a  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://ex.org/Class0>\t0.25\t0.8\t0.9\t40\t50\t44\t?a
?b  <http://ex.org/p>  ?a   => ?a  <http://ex.org/q>  ?b\t0.1\t0.5\t0.6\t10\t20\t17\t?a
Mining done in 1.2 s
Total time 2.3 s
"""


def test_parse_amie_output():
    rules = amie_mine.parse_amie_output(AMIE_STDOUT)
    assert len(rules) == 2
    first = rules[0]
    assert first["rule"].startswith("?a  <http://ex.org/worksAt>")
    assert (first["support"], first["confidence"], first["pca_confidence"]) == (0.25, 0.8, 0.9)
    assert first["positive_examples"] == 40.0


def test_parse_ignores_text_before_header():
    assert amie_mine.parse_amie_output("x => y\t1\t2\n") == []


def test_pool_to_tsv_dedupes_across_snapshots_and_skips_literals(tmp_path):
    a = tmp_path / "s1.nt"
    b = tmp_path / "s2.nt"
    a.write_text(
        "<http://e/x> <http://p/type> <http://e/C> .\n"
        '<http://e/x> <http://p/name> "X"@en .\n'
        "_:b0 <http://p/type> <http://e/C> .\n"
    )
    b.write_text(
        "<http://e/x> <http://p/type> <http://e/C> .\n"  # duplicate across snapshots
        "<http://e/y> <http://p/type> <http://e/D> .\n"
    )
    out = tmp_path / "pooled.tsv"
    assert amie_mine.pool_to_tsv([a, b], out) == 2
    lines = out.read_text().splitlines()
    assert lines == [
        "<http://e/x>\t<http://p/type>\t<http://e/C>",
        "<http://e/y>\t<http://p/type>\t<http://e/D>",
    ]
