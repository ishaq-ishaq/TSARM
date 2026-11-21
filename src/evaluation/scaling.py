"""Scalability metrics and run bookkeeping (proposal Sec. 3.7.3 i, 3.7.4).

Pure functions so the formulas are unit-testable without Spark:

* Throughput   ``Th = N / T``                 (triples per second)
* Latency      ``L = t_end - t_start``        (end-to-end seconds)
* Speedup      ``S_p = T_1 / T_p``            (strong scaling, fixed data)
* Efficiency   ``E_p = S_p / p``
* Weak scaling ``W_p = Th_p / Th_1``          (data grown with resources)

Note on weak scaling: when the data grows in proportion to ``p``, ideal
throughput also grows ``p``-fold, so ``Th_p / Th_1`` is ~``p`` (not ~1) under
perfect scaling. The proposal's "W_p ~ 1 is ideal" therefore holds for the
*per-worker* ratio. Both are reported: ``throughput_ratio`` = ``Th_p / Th_1``
(the formula as written) and ``weak_scaling_efficiency`` =
``(Th_p / p) / Th_1`` (= ``T_1 / T_p``), which is ~1 when scaling is ideal.

Repeated runs are summarised as mean, standard deviation and a 95% confidence
interval ``x_bar +/- t_{alpha/2, n-1} * s / sqrt(n)`` (Table 3.4).

``p`` here is the number of Spark executor cores/workers of the run: local-mode
cores on a single machine, or worker nodes on a cluster. The hardware
fingerprint is stored with every result file so that numbers from different
machines are never mixed.
"""

from __future__ import annotations

import math
import os
import platform
import statistics
import subprocess
from dataclasses import dataclass
from typing import Dict, List, Sequence


def throughput(n_triples: int, seconds: float) -> float:
    return n_triples / seconds if seconds > 0 else 0.0


def speedup(t_1: float, t_p: float) -> float:
    return t_1 / t_p if t_p > 0 else 0.0


def efficiency(s_p: float, p: int) -> float:
    return s_p / p if p > 0 else 0.0


def weak_scaling_ratio(th_1: float, th_p: float) -> float:
    """``W_p = Th_p / Th_1`` exactly as written in proposal Sec. 3.7.3 i."""
    return th_p / th_1 if th_1 > 0 else 0.0


def weak_scaling_efficiency(th_1: float, th_p: float, p: int) -> float:
    """Per-worker weak-scaling ratio ``(Th_p / p) / Th_1``; ideal = 1."""
    return weak_scaling_ratio(th_1, th_p) / p if p > 0 else 0.0


@dataclass
class Summary:
    n: int
    mean: float
    sd: float
    ci_low: float
    ci_high: float


def summarize(values: Sequence[float], confidence: float = 0.95) -> Summary:
    """Mean, sample SD and t-based confidence interval of repeated runs."""
    vals = [float(v) for v in values]
    n = len(vals)
    if n == 0:
        raise ValueError("summarize() needs at least one value.")
    mean = statistics.fmean(vals)
    if n == 1:
        return Summary(1, mean, 0.0, mean, mean)
    sd = statistics.stdev(vals)
    from scipy import stats  # local import: scipy only needed for CIs

    t_crit = stats.t.ppf(1 - (1 - confidence) / 2, df=n - 1)
    half = t_crit * sd / math.sqrt(n)
    return Summary(n, mean, sd, mean - half, mean + half)


def strong_scaling_table(runtimes: Dict[int, List[float]]) -> List[Dict]:
    """Per-``p`` summary with speedup/efficiency relative to the smallest ``p``.

    Args:
        runtimes: ``{p: [runtime_sec, ...]}`` for a fixed dataset.
    """
    if not runtimes:
        return []
    ps = sorted(runtimes)
    base_p = ps[0]
    t_base = summarize(runtimes[base_p]).mean
    rows = []
    for p in ps:
        s = summarize(runtimes[p])
        # Relative to the smallest p measured (normally p = 1).
        sp = speedup(t_base, s.mean) * base_p
        rows.append(
            {
                "p": p,
                "runs": s.n,
                "runtime_mean": s.mean,
                "runtime_sd": s.sd,
                "runtime_ci_low": s.ci_low,
                "runtime_ci_high": s.ci_high,
                "speedup": sp,
                "efficiency": efficiency(sp, p),
            }
        )
    return rows


def weak_scaling_table(throughputs: Dict[int, List[float]]) -> List[Dict]:
    """Per-``p`` throughput summary with ``Th_p / Th_1`` and its per-worker form.

    ``p`` values must be the resource counts the data was scaled with (e.g.
    data size = base size * p).
    """
    if not throughputs:
        return []
    ps = sorted(throughputs)
    th_base = summarize(throughputs[ps[0]]).mean
    rows = []
    for p in ps:
        s = summarize(throughputs[p])
        rows.append(
            {
                "p": p,
                "runs": s.n,
                "throughput_mean": s.mean,
                "throughput_sd": s.sd,
                "throughput_ci_low": s.ci_low,
                "throughput_ci_high": s.ci_high,
                "throughput_ratio": weak_scaling_ratio(th_base, s.mean),
                "weak_scaling_efficiency": weak_scaling_efficiency(
                    th_base, s.mean, p // ps[0] if ps[0] else p
                ),
            }
        )
    return rows


def _run(cmd: List[str]) -> str:
    try:
        return subprocess.run(
            cmd, capture_output=True, text=True, timeout=10, check=False
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def hardware_fingerprint() -> Dict[str, object]:
    """Describe the machine a result was produced on (macOS or Linux)."""
    info: Dict[str, object] = {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "logical_cpus": os.cpu_count(),
    }
    if platform.system() == "Darwin":
        info["cpu"] = _run(["sysctl", "-n", "machdep.cpu.brand_string"])
        mem = _run(["sysctl", "-n", "hw.memsize"])
        info["memory_gb"] = round(int(mem) / 2**30, 1) if mem.isdigit() else None
        for level in (0, 1):
            cores = _run(["sysctl", "-n", f"hw.perflevel{level}.physicalcpu"])
            if cores.isdigit():
                info[f"perflevel{level}_cores"] = int(cores)
    else:
        try:
            with open("/proc/cpuinfo") as fh:
                for line in fh:
                    if line.startswith("model name"):
                        info["cpu"] = line.split(":", 1)[1].strip()
                        break
            with open("/proc/meminfo") as fh:
                kb = int(fh.readline().split()[1])
                info["memory_gb"] = round(kb / 2**20, 1)
        except OSError:
            pass
    java_home = os.environ.get("JAVA_HOME", "")
    java = os.path.join(java_home, "bin", "java") if java_home else "java"
    try:
        out = subprocess.run(
            [java, "-version"], capture_output=True, text=True, timeout=10
        )
        info["java"] = (out.stderr or out.stdout).splitlines()[0] if (out.stderr or out.stdout) else ""
    except (OSError, subprocess.SubprocessError):
        info["java"] = ""
    try:
        import pyspark

        info["spark"] = pyspark.__version__
    except ImportError:  # pragma: no cover
        info["spark"] = None
    return info
