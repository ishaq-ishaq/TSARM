"""Render benchmark results as a Markdown table or CSV (pure Python)."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import List, Optional, Union

from .adapters import BenchmarkResult

_COLUMNS = [
    ("system", "System"),
    ("dataset", "Dataset"),
    ("status", "Status"),
    ("supports_temporal", "Temporal"),
    ("n_input_triples", "Input triples"),
    ("runtime_sec", "Runtime (s)"),
    ("n_rules", "#Rules"),
    ("mean_confidence", "Mean conf"),
    ("mean_support", "Mean supp"),
    ("note", "Notes"),
]


def _fmt(value: object) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return f"{value:.4g}"
    return str(value)


def _cell(result: BenchmarkResult, key: str) -> str:
    return _fmt(getattr(result, key))


def to_markdown(results: List[BenchmarkResult]) -> str:
    """Render results as a GitHub-flavoured Markdown comparison table."""
    headers = [label for _, label in _COLUMNS]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for result in results:
        lines.append(
            "| " + " | ".join(_cell(result, key) for key, _ in _COLUMNS) + " |"
        )
    return "\n".join(lines)


def to_csv(results: List[BenchmarkResult], path: Union[str, Path]) -> None:
    """Write results (including the ``extra`` dict, flattened) to a CSV file."""
    extra_keys = sorted({k for r in results for k in r.extra})
    base_keys = [key for key, _ in _COLUMNS]
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(base_keys + extra_keys)
        for result in results:
            row = [getattr(result, key) for key in base_keys]
            row += [result.extra.get(k) for k in extra_keys]
            writer.writerow(row)


def temporal_addendum(results: List[BenchmarkResult]) -> Optional[str]:
    """Markdown note summarising the temporal metrics only TSARM produces."""
    temporal = [
        r for r in results if r.supports_temporal and r.status == "ok" and r.extra
    ]
    if not temporal:
        return None
    lines = [
        "Temporal sensitivity (TSARM only; snapshot-based baselines cannot "
        "track rules across time):",
        "",
        "| System | Dataset | Windows | Mean persistence | Mean |drift| |",
        "| --- | --- | --- | --- | --- |",
    ]
    for r in temporal:
        lines.append(
            f"| {r.system} | {r.dataset} | "
            f"{_fmt(r.extra.get('n_windows'))} | "
            f"{_fmt(r.extra.get('mean_persistence'))} | "
            f"{_fmt(r.extra.get('mean_abs_drift'))} |"
        )
    return "\n".join(lines)
