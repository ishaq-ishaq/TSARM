"""Tests for the scalability formulas (proposal Sec. 3.7.3 i) -- no Spark needed."""

import math

import pytest

from src.evaluation.scaling import (
    efficiency,
    hardware_fingerprint,
    karp_flatt,
    speedup,
    strong_scaling_table,
    summarize,
    throughput,
    weak_scaling_efficiency,
    weak_scaling_ratio,
    weak_scaling_table,
)


def test_basic_formulas():
    assert throughput(1000, 4.0) == 250.0
    assert speedup(100.0, 25.0) == 4.0
    assert efficiency(4.0, 8) == 0.5
    assert weak_scaling_ratio(200.0, 380.0) == pytest.approx(1.9)
    assert weak_scaling_efficiency(200.0, 380.0, 2) == pytest.approx(0.95)
    # Degenerate inputs do not divide by zero.
    assert throughput(10, 0) == 0.0 and speedup(1, 0) == 0.0
    assert efficiency(1, 0) == 0.0 and weak_scaling_ratio(0, 1) == 0.0


def test_karp_flatt():
    # Perfect speedup -> no serial fraction.
    assert karp_flatt(4.0, 4) == pytest.approx(0.0)
    # Amdahl with f = 0.1 at p = 10: S = 1 / (0.1 + 0.9/10) -> e = 0.1
    s = 1 / (0.1 + 0.9 / 10)
    assert karp_flatt(s, 10) == pytest.approx(0.1)
    assert math.isnan(karp_flatt(1.0, 1))


def test_summarize_mean_sd_and_t_interval():
    s = summarize([10.0, 12.0, 14.0])
    assert s.n == 3 and s.mean == pytest.approx(12.0)
    assert s.sd == pytest.approx(2.0)
    # t_{0.975, 2} = 4.302653 ; half-width = 4.302653 * 2 / sqrt(3)
    half = 4.302652729911275 * 2.0 / math.sqrt(3)
    assert s.ci_low == pytest.approx(12.0 - half)
    assert s.ci_high == pytest.approx(12.0 + half)


def test_summarize_single_value_has_degenerate_interval():
    s = summarize([5.0])
    assert (s.mean, s.sd, s.ci_low, s.ci_high) == (5.0, 0.0, 5.0, 5.0)
    with pytest.raises(ValueError):
        summarize([])


def test_strong_scaling_table_relative_to_one_core():
    rows = strong_scaling_table({1: [100.0, 100.0], 2: [50.0, 50.0], 4: [40.0, 40.0]})
    by_p = {r["p"]: r for r in rows}
    assert by_p[1]["speedup"] == pytest.approx(1.0)
    assert by_p[2]["speedup"] == pytest.approx(2.0)
    assert by_p[2]["efficiency"] == pytest.approx(1.0)
    assert by_p[4]["speedup"] == pytest.approx(2.5)
    assert by_p[4]["efficiency"] == pytest.approx(0.625)


def test_strong_scaling_table_when_smallest_p_is_not_one():
    # Baseline at p=2: speedup is scaled so that S_2 = 2 (assumes ideal 1->2).
    rows = strong_scaling_table({2: [50.0], 4: [25.0]})
    by_p = {r["p"]: r for r in rows}
    assert by_p[2]["speedup"] == pytest.approx(2.0)
    assert by_p[4]["speedup"] == pytest.approx(4.0)


def test_weak_scaling_table():
    rows = weak_scaling_table({1: [1000.0], 2: [1900.0], 4: [3600.0]})
    by_p = {r["p"]: r for r in rows}
    assert by_p[1]["throughput_ratio"] == pytest.approx(1.0)
    assert by_p[4]["throughput_ratio"] == pytest.approx(3.6)
    # Per worker: 3.6 / 4 = 0.9 of ideal.
    assert by_p[4]["weak_scaling_efficiency"] == pytest.approx(0.9)
    assert by_p[2]["weak_scaling_efficiency"] == pytest.approx(0.95)


def test_hardware_fingerprint_has_core_fields():
    info = hardware_fingerprint()
    for key in ("platform", "machine", "python", "logical_cpus", "spark"):
        assert key in info
