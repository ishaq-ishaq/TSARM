"""Baseline adapters for the TSARM comparison harness (research objective iv).

Defines a common interface so heterogeneous rule-mining systems can be run over
the same datasets and compared on **computational scalability** (runtime,
throughput), **rule quality** (rule count, support/confidence) and **temporal
sensitivity** (only TSARM tracks rules across time).

Adapters
--------
* :class:`TSARMBaseline` -- runs the full TSARM pipeline (ingest -> window ->
  mine -> temporal metrics). Always available.
* :class:`ExternalCommandBaseline` -- wraps an external miner (SANSA, RDFRules)
  that is invoked as a shell command and writes a rules file. The command is
  supplied via an environment variable, so no third-party JAR is bundled; if the
  variable is unset the baseline reports itself unavailable and is skipped.
* :func:`sansa_baseline` / :func:`rdfrules_baseline` -- pre-configured
  :class:`ExternalCommandBaseline` instances for the two systems named in the
  research proposal (Lehmann et al., 2017; Zeman, Kliegr and Svetek, 2021).

The external command template supports the placeholders ``{input}`` (space-
joined snapshot paths), ``{output}`` (a fresh directory the tool must write its
rules file into), ``{min_support}`` and ``{min_confidence}``. The produced rules
file is parsed by :func:`parse_rules_file` (a delimited file with, ideally,
``support`` and ``confidence`` columns).
"""

from __future__ import annotations

import csv
import glob
import os
import shlex
import subprocess
import tempfile
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


# --- shared data structures --------------------------------------------------


@dataclass
class Snapshot:
    """One RDF snapshot file valid at a timestamp."""

    snapshot_id: str
    path: Path
    timestamp: datetime


@dataclass
class Dataset:
    """A named, ordered set of temporal RDF snapshots."""

    name: str
    snapshots: List[Snapshot]

    @property
    def paths(self) -> List[Path]:
        return [s.path for s in self.snapshots]


@dataclass
class BenchmarkResult:
    """Comparable outcome of running one system on one dataset."""

    system: str
    dataset: str
    status: str  # "ok" | "skipped" | "error"
    supports_temporal: bool = False
    n_input_triples: Optional[int] = None
    runtime_sec: Optional[float] = None
    n_rules: Optional[int] = None
    mean_confidence: Optional[float] = None
    mean_support: Optional[float] = None
    note: str = ""
    extra: Dict[str, float] = field(default_factory=dict)


# --- rules-file parsing (shared by external adapters) ------------------------


def parse_rules_file(output_dir: Path, delimiter: str = ",") -> Dict[str, Optional[float]]:
    """Count rules and average support/confidence from a tool's output dir.

    Reads every ``*.csv`` / ``*.tsv`` / ``*.txt`` file in ``output_dir`` as a
    delimited table. If a header row contains ``support`` and/or ``confidence``
    columns (case-insensitive) those are averaged; otherwise only the row count
    is returned.

    Returns a dict with ``n_rules``, ``mean_confidence`` and ``mean_support``
    (the latter two may be ``None`` if the columns are absent).
    """
    files = sorted(
        f
        for pattern in ("*.csv", "*.tsv", "*.txt")
        for f in glob.glob(str(output_dir / pattern))
    )
    n_rules = 0
    conf_sum = supp_sum = 0.0
    conf_n = supp_n = 0

    for path in files:
        with open(path, newline="", encoding="utf-8") as handle:
            reader = csv.reader(handle, delimiter=delimiter)
            rows = list(reader)
        if not rows:
            continue
        header = [c.strip().lower() for c in rows[0]]
        conf_idx = header.index("confidence") if "confidence" in header else None
        supp_idx = header.index("support") if "support" in header else None
        has_header = conf_idx is not None or supp_idx is not None
        data_rows = rows[1:] if has_header else rows

        for row in data_rows:
            if not any(cell.strip() for cell in row):
                continue
            n_rules += 1
            if conf_idx is not None and conf_idx < len(row):
                try:
                    conf_sum += float(row[conf_idx])
                    conf_n += 1
                except ValueError:
                    pass
            if supp_idx is not None and supp_idx < len(row):
                try:
                    supp_sum += float(row[supp_idx])
                    supp_n += 1
                except ValueError:
                    pass

    return {
        "n_rules": n_rules,
        "mean_confidence": conf_sum / conf_n if conf_n else None,
        "mean_support": supp_sum / supp_n if supp_n else None,
    }


# --- adapter interface -------------------------------------------------------


class Baseline(ABC):
    """A rule-mining system that can be benchmarked on a :class:`Dataset`."""

    name: str = "baseline"
    supports_temporal: bool = False

    def __init__(self, min_support: float = 0.1, min_confidence: float = 0.5):
        self.min_support = min_support
        self.min_confidence = min_confidence

    @abstractmethod
    def is_available(self) -> bool:
        """Whether the system can run in the current environment."""

    @abstractmethod
    def run(self, dataset: Dataset) -> BenchmarkResult:
        """Mine ``dataset`` and return a comparable result."""


class TSARMBaseline(Baseline):
    """Runs the full TSARM pipeline and reports temporal metrics."""

    name = "TSARM"
    supports_temporal = True

    def __init__(
        self,
        spark,
        min_support: float = 0.1,
        min_confidence: float = 0.5,
        tau: float = 0.6,
        width: int = 1,
        step: int = 1,
        include_literals: bool = False,
    ):
        super().__init__(min_support, min_confidence)
        self.spark = spark
        self.tau = tau
        self.width = width
        self.step = step
        self.include_literals = include_literals

    def is_available(self) -> bool:
        return True

    def run(self, dataset: Dataset) -> BenchmarkResult:
        # Imported lazily so the harness module can be imported without Spark.
        from pyspark.sql import functions as F

        from src.ingestion.ingest import ingest_snapshots
        from src.metrics.temporal import compute_metrics
        from src.mining.arm import mine
        from src.windowing.sliding_window import (
            assign_windows,
            build_windows,
            ordered_snapshots,
        )

        start = time.perf_counter()
        snap_map = {
            s.snapshot_id: (s.path, s.timestamp) for s in dataset.snapshots
        }
        triples = ingest_snapshots(self.spark, snap_map)
        n_input = triples.count()

        snaps = ordered_snapshots(triples)
        n_windows = len(build_windows(snaps, width=self.width, step=self.step))
        windowed = assign_windows(self.spark, triples, width=self.width, step=self.step)

        _, rules = mine(
            windowed,
            min_support=self.min_support,
            min_confidence=self.min_confidence,
            include_literals=self.include_literals,
        )
        metrics = compute_metrics(rules, n_windows=n_windows, tau=self.tau)

        quality = metrics.agg(
            F.count("*").alias("n"),
            F.avg("mean_confidence").alias("conf"),
            F.avg("temporal_support").alias("supp"),
            F.avg("persistence_score").alias("persist"),
            F.avg(F.abs("confidence_drift")).alias("drift"),
        ).collect()[0]
        runtime = time.perf_counter() - start

        return BenchmarkResult(
            system=self.name,
            dataset=dataset.name,
            status="ok",
            supports_temporal=True,
            n_input_triples=n_input,
            runtime_sec=runtime,
            n_rules=int(quality["n"]),
            mean_confidence=quality["conf"],
            mean_support=quality["supp"],
            extra={
                "n_windows": float(n_windows),
                "mean_persistence": quality["persist"] or 0.0,
                "mean_abs_drift": quality["drift"] or 0.0,
            },
        )


class ExternalCommandBaseline(Baseline):
    """Wraps an external miner invoked as a shell command writing a rules file.

    The command template comes from the environment variable ``command_env``
    (e.g. ``SANSA_CMD``). It is unavailable -- and therefore skipped -- when that
    variable is unset.
    """

    def __init__(
        self,
        name: str,
        command_env: str,
        min_support: float = 0.1,
        min_confidence: float = 0.5,
        delimiter: str = ",",
        timeout_sec: int = 3600,
    ):
        super().__init__(min_support, min_confidence)
        self.name = name
        self.command_env = command_env
        self.delimiter = delimiter
        self.timeout_sec = timeout_sec

    def command_template(self) -> Optional[str]:
        return os.environ.get(self.command_env)

    def is_available(self) -> bool:
        return bool(self.command_template())

    def run(self, dataset: Dataset) -> BenchmarkResult:
        template = self.command_template()
        if not template:
            return BenchmarkResult(
                system=self.name,
                dataset=dataset.name,
                status="skipped",
                supports_temporal=self.supports_temporal,
                note=f"Set ${self.command_env} to the command that runs {self.name}.",
            )

        with tempfile.TemporaryDirectory(prefix=f"{self.name}_out_") as out_dir:
            command = template.format(
                input=" ".join(shlex.quote(str(p)) for p in dataset.paths),
                output=shlex.quote(out_dir),
                min_support=self.min_support,
                min_confidence=self.min_confidence,
            )
            start = time.perf_counter()
            try:
                proc = subprocess.run(
                    command,
                    shell=True,
                    capture_output=True,
                    text=True,
                    timeout=self.timeout_sec,
                )
            except subprocess.TimeoutExpired:
                return BenchmarkResult(
                    system=self.name,
                    dataset=dataset.name,
                    status="error",
                    supports_temporal=self.supports_temporal,
                    note=f"Timed out after {self.timeout_sec}s.",
                )
            runtime = time.perf_counter() - start

            if proc.returncode != 0:
                return BenchmarkResult(
                    system=self.name,
                    dataset=dataset.name,
                    status="error",
                    supports_temporal=self.supports_temporal,
                    runtime_sec=runtime,
                    note=f"Exit {proc.returncode}: {proc.stderr.strip()[:300]}",
                )

            parsed = parse_rules_file(Path(out_dir), delimiter=self.delimiter)

        return BenchmarkResult(
            system=self.name,
            dataset=dataset.name,
            status="ok",
            supports_temporal=self.supports_temporal,
            runtime_sec=runtime,
            n_rules=parsed["n_rules"],
            mean_confidence=parsed["mean_confidence"],
            mean_support=parsed["mean_support"],
            note="snapshot-based (no temporal metrics)",
        )


def sansa_baseline(**kwargs) -> ExternalCommandBaseline:
    """SANSA adapter (Lehmann et al., 2017). Configure via ``$SANSA_CMD``.

    Example ``$SANSA_CMD`` (a spark-submit invocation of a SANSA mining job)::

        spark-submit --class your.SansaArmJob sansa-arm.jar \\
            --input {input} --output {output} \\
            --min-support {min_support} --min-confidence {min_confidence}
    """
    return ExternalCommandBaseline(name="SANSA", command_env="SANSA_CMD", **kwargs)


def rdfrules_baseline(**kwargs) -> ExternalCommandBaseline:
    """RDFRules adapter (Zeman, Kliegr and Svetek, 2021). Configure via ``$RDFRULES_CMD``.

    Example ``$RDFRULES_CMD`` (an RDFRules console/script invocation that exports
    mined rules to ``{output}``)::

        java -jar rdfrules.jar run-script mine.json \\
            -Dinput={input} -Doutput={output} \\
            -DminSupport={min_support} -DminConfidence={min_confidence}
    """
    return ExternalCommandBaseline(name="RDFRules", command_env="RDFRULES_CMD", **kwargs)
