"""Statistical comparison of TSARM against a baseline (proposal Table 3.4).

Implements the analysis plan for paired performance observations (e.g. the
runtime of TSARM and of a baseline on the same dataset and repetition):

1. Descriptive statistics: mean +/- SD, 95% t-interval
   (:func:`src.evaluation.scaling.summarize`).
2. Normality check: Shapiro-Wilk on the paired differences.
3. If the differences look normal: **paired t-test** with **Cohen's d**.
   Otherwise: **Wilcoxon signed-rank test** with the **matched-pairs
   rank-biserial correlation** ``r = (W+ - W-) / (W+ + W-)``.

Cohen's d follows the proposal's definition
``d = (mu_TSARM - mu_baseline) / s_pooled`` with the pooled SD of the two
samples. With very few runs (the proposal uses 3), the tests have little
power. The result says so (``low_power``) rather than over-claiming.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import asdict, dataclass
from typing import Dict, Optional, Sequence

ALPHA = 0.05
MIN_N_FOR_SHAPIRO = 3  # scipy requires n >= 3
LOW_POWER_N = 5


def cohens_d(a: Sequence[float], b: Sequence[float]) -> float:
    """``(mean(a) - mean(b)) / s_pooled`` with the pooled sample SD."""
    na, nb = len(a), len(b)
    if na < 2 or nb < 2:
        return 0.0
    va, vb = statistics.variance(a), statistics.variance(b)
    pooled = math.sqrt(((na - 1) * va + (nb - 1) * vb) / (na + nb - 2))
    if pooled == 0:
        return 0.0
    return (statistics.fmean(a) - statistics.fmean(b)) / pooled


def rank_biserial(a: Sequence[float], b: Sequence[float]) -> float:
    """Matched-pairs rank-biserial correlation ``(W+ - W-) / (W+ + W-)``.

    Differences are ``a_i - b_i``; zero differences are dropped (Wilcoxon's
    convention). Ranks of tied |differences| are averaged.
    """
    diffs = [x - y for x, y in zip(a, b) if x != y]
    if not diffs:
        return 0.0
    order = sorted(range(len(diffs)), key=lambda i: abs(diffs[i]))
    ranks = [0.0] * len(diffs)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and abs(diffs[order[j + 1]]) == abs(diffs[order[i]]):
            j += 1
        avg = (i + j) / 2 + 1  # ranks are 1-based
        for k in range(i, j + 1):
            ranks[order[k]] = avg
        i = j + 1
    w_plus = sum(r for r, d in zip(ranks, diffs) if d > 0)
    w_minus = sum(r for r, d in zip(ranks, diffs) if d < 0)
    total = w_plus + w_minus
    return (w_plus - w_minus) / total if total else 0.0


@dataclass
class Comparison:
    metric: str
    n: int
    mean_tsarm: float
    mean_baseline: float
    sd_tsarm: float
    sd_baseline: float
    shapiro_p: Optional[float]
    normal: Optional[bool]
    test: str
    statistic: Optional[float]
    p_value: Optional[float]
    significant: Optional[bool]
    effect_size_name: str
    effect_size: float
    low_power: bool
    note: str

    def as_dict(self) -> Dict:
        return asdict(self)


def compare_paired(
    tsarm: Sequence[float],
    baseline: Sequence[float],
    metric: str = "runtime_sec",
    alpha: float = ALPHA,
) -> Comparison:
    """Run the Table 3.4 plan on paired observations of one metric.

    Args:
        tsarm, baseline: Same-length sequences; element ``i`` of each comes
            from the same dataset/repetition.
        metric: Name recorded in the result.
        alpha: Significance level for both the normality and the main test.
    """
    from scipy import stats

    a = [float(x) for x in tsarm]
    b = [float(x) for x in baseline]
    if len(a) != len(b):
        raise ValueError("Paired comparison needs equal-length samples.")
    n = len(a)
    if n < 2:
        raise ValueError("Need at least 2 paired observations.")

    diffs = [x - y for x, y in zip(a, b)]
    notes = []

    shapiro_p: Optional[float] = None
    normal: Optional[bool] = None
    if n >= MIN_N_FOR_SHAPIRO and len(set(diffs)) > 1:
        shapiro_p = float(stats.shapiro(diffs).pvalue)
        normal = shapiro_p > alpha
    else:
        notes.append("normality not testable (n < 3 or constant differences)")

    statistic: Optional[float] = None
    p_value: Optional[float] = None
    if normal is False:
        test, eff_name, eff = "wilcoxon_signed_rank", "rank_biserial", rank_biserial(a, b)
        if any(d != 0 for d in diffs):
            res = stats.wilcoxon(a, b)
            statistic, p_value = float(res.statistic), float(res.pvalue)
    else:
        # Normal, or untestable: default to the parametric test.
        test, eff_name, eff = "paired_t", "cohens_d", cohens_d(a, b)
        if len(set(diffs)) > 1:
            res = stats.ttest_rel(a, b)
            statistic, p_value = float(res.statistic), float(res.pvalue)
        else:
            notes.append("identical differences: t statistic undefined")

    low_power = n < LOW_POWER_N
    if low_power:
        notes.append(f"n={n} runs: low statistical power, interpret with care")

    return Comparison(
        metric=metric,
        n=n,
        mean_tsarm=statistics.fmean(a),
        mean_baseline=statistics.fmean(b),
        sd_tsarm=statistics.stdev(a) if n > 1 else 0.0,
        sd_baseline=statistics.stdev(b) if n > 1 else 0.0,
        shapiro_p=shapiro_p,
        normal=normal,
        test=test,
        statistic=statistic,
        p_value=p_value,
        significant=(p_value < alpha) if p_value is not None else None,
        effect_size_name=eff_name,
        effect_size=eff,
        low_power=low_power,
        note="; ".join(notes),
    )
