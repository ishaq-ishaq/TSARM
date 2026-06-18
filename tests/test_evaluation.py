"""Tests for the baseline comparison harness.

The external-baseline tests use a fake command (a Python one-liner that writes a
known rules CSV), so the harness is fully exercised without SANSA/RDFRules
installed. One Spark-backed test runs the real TSARM adapter on the samples.
"""

import sys
from datetime import datetime
from pathlib import Path

import pytest

from src.evaluation.adapters import (
    Dataset,
    ExternalCommandBaseline,
    Snapshot,
    TSARMBaseline,
    parse_rules_file,
    rdfrules_baseline,
    sansa_baseline,
)
from src.evaluation.benchmark import run_benchmark
from src.evaluation.report import to_csv, to_markdown
from src.ingestion.spark_session import get_spark

RAW = Path(__file__).resolve().parent.parent / "data" / "raw"


def _sample_dataset() -> Dataset:
    return Dataset(
        name="sample",
        snapshots=[
            Snapshot("2023", RAW / "sample_2023.nt", datetime(2023, 1, 1)),
            Snapshot("2024", RAW / "sample_2024.nt", datetime(2024, 1, 1)),
        ],
    )


# --- rules-file parsing ------------------------------------------------------


def test_parse_rules_file_with_header(tmp_path):
    (tmp_path / "rules.csv").write_text(
        "antecedent,consequent,support,confidence\n"
        "A,B,0.5,0.8\n"
        "C,D,0.3,0.6\n",
        encoding="utf-8",
    )
    parsed = parse_rules_file(tmp_path)
    assert parsed["n_rules"] == 2
    assert parsed["mean_confidence"] == pytest.approx(0.7)
    assert parsed["mean_support"] == pytest.approx(0.4)


def test_parse_rules_file_without_columns(tmp_path):
    (tmp_path / "rules.txt").write_text("A => B\nC => D\nE => F\n", encoding="utf-8")
    parsed = parse_rules_file(tmp_path)
    assert parsed["n_rules"] == 3
    assert parsed["mean_confidence"] is None
    assert parsed["mean_support"] is None


# --- external command baseline (fake command) -------------------------------


def _fake_command_env(monkeypatch, env_var):
    # A command that writes a 2-rule CSV into {output}, ignoring {input}.
    script = (
        "import sys,os;"
        "open(os.path.join(sys.argv[1],'rules.csv'),'w')"
        ".write('antecedent,consequent,support,confidence\\n"
        "A,B,0.5,0.9\\nC,D,0.4,0.7\\n')"
    )
    monkeypatch.setenv(
        env_var, f"{sys.executable} -c {script!r} {{output}}"
    )


def test_external_baseline_skipped_when_unset(monkeypatch):
    monkeypatch.delenv("SANSA_CMD", raising=False)
    baseline = sansa_baseline()
    assert not baseline.is_available()
    result = baseline.run(_sample_dataset())
    assert result.status == "skipped"
    assert "SANSA_CMD" in result.note


def test_external_baseline_runs_fake_command(monkeypatch):
    _fake_command_env(monkeypatch, "RDFRULES_CMD")
    baseline = rdfrules_baseline()
    assert baseline.is_available()
    result = baseline.run(_sample_dataset())
    assert result.status == "ok"
    assert result.system == "RDFRules"
    assert result.n_rules == 2
    assert result.mean_confidence == pytest.approx(0.8)
    assert result.runtime_sec is not None and result.runtime_sec >= 0
    assert result.supports_temporal is False


def test_external_baseline_reports_command_failure(monkeypatch):
    monkeypatch.setenv("SANSA_CMD", "false")  # exits non-zero
    baseline = sansa_baseline()
    result = baseline.run(_sample_dataset())
    assert result.status == "error"


def test_external_baseline_placeholders(monkeypatch):
    # Echo the formatted command into a file to assert placeholders resolve.
    out_marker = "import sys,os;open(os.path.join(sys.argv[1],'rules.csv'),'w').write('x,y\\n1,2\\n')"
    monkeypatch.setenv(
        "RDFRULES_CMD",
        f"{sys.executable} -c {out_marker!r} {{output}} # ms={{min_support}} mc={{min_confidence}} in={{input}}",
    )
    baseline = rdfrules_baseline(min_support=0.25, min_confidence=0.75)
    result = baseline.run(_sample_dataset())
    assert result.status == "ok"
    # No support/confidence header -> both lines counted as rules (conservative).
    assert result.n_rules == 2


# --- benchmark matrix + report ----------------------------------------------


def test_run_benchmark_mixes_skipped_and_ok(monkeypatch):
    monkeypatch.delenv("SANSA_CMD", raising=False)
    _fake_command_env(monkeypatch, "RDFRULES_CMD")
    results = run_benchmark(
        [sansa_baseline(), rdfrules_baseline()], [_sample_dataset()]
    )
    by_system = {r.system: r for r in results}
    assert by_system["SANSA"].status == "skipped"
    assert by_system["RDFRules"].status == "ok"


def test_report_markdown_and_csv(tmp_path, monkeypatch):
    _fake_command_env(monkeypatch, "RDFRULES_CMD")
    results = run_benchmark([rdfrules_baseline()], [_sample_dataset()])
    md = to_markdown(results)
    assert "| System |" in md
    assert "RDFRules" in md

    csv_path = tmp_path / "out.csv"
    to_csv(results, csv_path)
    content = csv_path.read_text(encoding="utf-8")
    assert "system" in content and "RDFRules" in content


# --- TSARM adapter (Spark) ---------------------------------------------------


@pytest.fixture(scope="module")
def spark():
    session = get_spark(app_name="TSARM-eval-tests", master="local[2]")
    yield session
    session.stop()


def test_tsarm_baseline_runs_on_samples(spark):
    baseline = TSARMBaseline(spark, min_support=0.5, min_confidence=0.5)
    assert baseline.is_available()
    result = baseline.run(_sample_dataset())
    assert result.status == "ok"
    assert result.supports_temporal is True
    assert result.n_input_triples == 15
    assert result.n_rules and result.n_rules > 0
    assert result.runtime_sec and result.runtime_sec > 0
    assert "mean_persistence" in result.extra
    assert result.extra["n_windows"] == 2
