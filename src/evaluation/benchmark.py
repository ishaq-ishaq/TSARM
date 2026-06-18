"""Benchmark runner: execute baselines over datasets and collect results."""

from __future__ import annotations

from typing import List

from .adapters import Baseline, BenchmarkResult, Dataset


def run_benchmark(
    baselines: List[Baseline], datasets: List[Dataset]
) -> List[BenchmarkResult]:
    """Run every available baseline on every dataset.

    Unavailable baselines (e.g. an external miner whose command env var is
    unset) yield a ``"skipped"`` result instead of being run; exceptions during
    a run are captured as ``"error"`` results so one failure does not abort the
    whole matrix.
    """
    results: List[BenchmarkResult] = []
    for dataset in datasets:
        for baseline in baselines:
            if not baseline.is_available():
                results.append(
                    BenchmarkResult(
                        system=baseline.name,
                        dataset=dataset.name,
                        status="skipped",
                        supports_temporal=baseline.supports_temporal,
                        note="not available in this environment",
                    )
                )
                continue
            try:
                results.append(baseline.run(dataset))
            except Exception as exc:  # noqa: BLE001 - report, don't crash the matrix
                results.append(
                    BenchmarkResult(
                        system=baseline.name,
                        dataset=dataset.name,
                        status="error",
                        supports_temporal=baseline.supports_temporal,
                        note=f"{type(exc).__name__}: {exc}",
                    )
                )
    return results
