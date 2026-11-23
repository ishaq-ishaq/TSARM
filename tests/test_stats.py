"""Tests for the Table 3.4 statistical comparison -- no Spark needed."""

import pytest
from scipy import stats

from src.evaluation.stats import cohens_d, compare_paired, rank_biserial


def test_cohens_d_hand_computed():
    # means 2 and 3, both variances 1 -> pooled SD 1 -> d = -1
    assert cohens_d([1, 2, 3], [2, 3, 4]) == pytest.approx(-1.0)
    assert cohens_d([1], [2]) == 0.0
    assert cohens_d([2, 2], [2, 2]) == 0.0  # zero pooled SD


def test_rank_biserial_hand_computed():
    # diffs 4,4,4,-1 -> ranks of |d|: 1 -> 1 ; 4,4,4 -> 2,3,4 (avg 3)
    # W+ = 9, W- = 1 -> r = (9 - 1) / 10 = 0.8
    assert rank_biserial([5, 6, 7, 8], [1, 2, 3, 9]) == pytest.approx(0.8)
    # all TSARM values lower -> r = -1
    assert rank_biserial([1, 2, 3], [4, 5, 6]) == pytest.approx(-1.0)
    # zero differences are dropped
    assert rank_biserial([1, 1], [1, 1]) == 0.0


def test_normal_differences_use_paired_t_and_cohens_d():
    a = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0]
    b = [12.1, 12.9, 14.2, 14.8, 16.1, 17.0]  # diffs roughly -2 +/- small noise
    c = compare_paired(a, b)
    assert c.normal is True
    assert c.test == "paired_t" and c.effect_size_name == "cohens_d"
    ref = stats.ttest_rel(a, b)
    assert c.statistic == pytest.approx(ref.statistic)
    assert c.p_value == pytest.approx(ref.pvalue)
    assert c.significant is True
    assert c.effect_size == pytest.approx(cohens_d(a, b))
    assert c.low_power is False


def test_skewed_differences_use_wilcoxon_and_rank_biserial():
    b = [10.0] * 8
    diffs = [0.1, 0.1, 0.11, 0.1, 0.12, 0.1, 0.1, 9.0]  # one large outlier
    a = [x + d for x, d in zip(b, diffs)]
    c = compare_paired(a, b)
    assert c.normal is False
    assert c.test == "wilcoxon_signed_rank"
    assert c.effect_size_name == "rank_biserial"
    assert c.effect_size == pytest.approx(1.0)  # every difference positive
    ref = stats.wilcoxon(a, b)
    assert c.p_value == pytest.approx(ref.pvalue)


def test_three_runs_flagged_low_power():
    c = compare_paired([10.0, 11.0, 12.5], [20.0, 21.5, 22.0])
    assert c.n == 3 and c.low_power is True
    assert "low statistical power" in c.note


def test_rejects_unpaired_input():
    with pytest.raises(ValueError):
        compare_paired([1.0, 2.0], [1.0])
    with pytest.raises(ValueError):
        compare_paired([1.0], [2.0])
