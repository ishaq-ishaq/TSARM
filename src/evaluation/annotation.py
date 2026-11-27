"""Hand-validated reference set for E3 on real data (proposal Sec. 3.7.3 ii).

On synthetic data the planted patterns are the ground truth. Wikidata and
DBpedia have no ground truth, so the reference set ``R_true`` is built by human
judgement, using the pooling method of information-retrieval evaluation:

1. **Pool.** Rules from every system (TSARM, AMIE+, RDFRules) are converted to
   the shared rule key (:mod:`src.evaluation.quality`) and pooled. Rules found
   by several systems appear once, so no annotator knows which system found a
   rule.
2. **Sample.** If the pool is too large to judge in full, a stratified random
   sample is drawn (strata = system x lifecycle class, so transient and
   emerging rules are judged as well as stable ones). Every stratum keeps at
   least ``min_per_stratum`` rules.
3. **Judge.** Two annotators label each rule independently in a CSV sheet:
   ``1`` = the rule states a valid, non-trivial regularity of the domain,
   ``0`` = invalid or trivial (taxonomic restatement, artefact of extraction),
   ``?`` = cannot decide. The sheet gives shortened names, optional labels and
   browsable links for every item.
4. **Score.** Agreement is Cohen's kappa over rules both annotators decided.
   A rule enters ``R_true`` when both annotators mark it ``1`` (or, after an
   adjudication pass, when the ``final`` column says ``1``). Precision of each
   system is then the share of its judged rules in ``R_true``, with a Wilson
   95 % interval. Recall is *relative recall* against the pooled ``R_true``,
   the usual reading of recall under pooling.

The written reference CSV has the ``antecedent,consequent`` columns that
:func:`src.evaluation.quality.load_reference_rules` reads.
"""

from __future__ import annotations

import csv
import json
import math
import random
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

PREFIXES = (
    ("http://www.wikidata.org/prop/direct/", "wdt:"),
    ("http://www.wikidata.org/entity/", "wd:"),
    ("http://dbpedia.org/ontology/", "dbo:"),
    ("http://dbpedia.org/resource/", "dbr:"),
    ("http://www.w3.org/2002/07/owl#", "owl:"),
    ("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf:"),
    ("http://www.w3.org/2000/01/rdf-schema#", "rdfs:"),
    ("http://schema.org/", "schema:"),
    ("http://xmlns.com/foaf/0.1/", "foaf:"),
)

SHEET_COLUMNS = (
    "id", "rule", "readable", "labels", "links", "systems", "lifecycle",
    "mean_confidence", "confidence_series", "judge_a", "judge_b", "final", "note",
)


# ---------------------------------------------------------------------------
# Rules in, readable text out
# ---------------------------------------------------------------------------


def shorten(iri: str) -> str:
    for ns, prefix in PREFIXES:
        if iri.startswith(ns):
            return prefix + iri[len(ns):]
    return iri


def _iris(item: str) -> List[str]:
    """IRIs inside one item: a class IRI, or both sides of ``predicate=object``."""
    if item.startswith("http") and "=http" in item:
        p, o = item.split("=http", 1)
        return [p, "http" + o]
    return [item]


def readable_item(item: str) -> str:
    return "=".join(shorten(i) for i in _iris(item))


def split_key(key: str) -> Tuple[List[str], List[str]]:
    ante, cons = key.split(" => ", 1)
    return [a for a in ante.split(",") if a], [c for c in cons.split(",") if c]


def readable_rule(key: str) -> str:
    ante, cons = split_key(key)
    return " AND ".join(readable_item(a) for a in ante) + "  =>  " + " AND ".join(readable_item(c) for c in cons)


def browse_url(iri: str) -> str:
    for ns in ("http://www.wikidata.org/entity/", "http://www.wikidata.org/prop/direct/"):
        if iri.startswith(ns):
            local = iri[len(ns):]
            return "https://www.wikidata.org/wiki/" + ("Property:" + local if local.startswith("P") else local)
    return iri


def rule_iris(key: str) -> List[str]:
    ante, cons = split_key(key)
    out: List[str] = []
    for item in ante + cons:
        for iri in _iris(item):
            if iri not in out:
                out.append(iri)
    return out


def load_tsarm_export(path) -> List[Dict]:
    """Rules from a TSARM JSON export (:mod:`src.metrics.export`)."""
    data = json.loads(Path(path).read_text())
    return data["rules"] if isinstance(data, dict) else data


# ---------------------------------------------------------------------------
# Pool and sample
# ---------------------------------------------------------------------------


@dataclass
class PooledRule:
    key: str
    systems: List[str]
    lifecycle: str = ""
    mean_confidence: Optional[float] = None
    confidence_series: str = ""


def pool_rules(
    tsarm_rules: Sequence[Mapping] = (),
    baselines: Optional[Mapping[str, Mapping[str, Optional[float]]]] = None,
) -> List[PooledRule]:
    """One entry per distinct rule key, with the systems that found it.

    ``baselines`` maps a system name to ``{rule_key: confidence}``, the
    ``comparable`` dictionaries produced by ``scripts/evaluate_quality.py``.
    """
    pooled: Dict[str, PooledRule] = {}
    for r in tsarm_rules:
        series = ";".join(f"{c:.3f}" for c in r.get("confidence_series") or [])
        pooled[r["rule_key"]] = PooledRule(
            r["rule_key"], ["tsarm"], r.get("lifecycle", ""), r.get("mean_confidence"), series
        )
    for system, rules in (baselines or {}).items():
        for key, conf in rules.items():
            if key in pooled:
                if system not in pooled[key].systems:
                    pooled[key].systems.append(system)
            else:
                pooled[key] = PooledRule(key, [system], "", conf, "")
    return sorted(pooled.values(), key=lambda p: p.key)


def stratified_sample(
    pool: Sequence[PooledRule], n: int, seed: int = 42, min_per_stratum: int = 5
) -> List[PooledRule]:
    """At most ``n`` rules, proportional per stratum, each stratum >= ``min_per_stratum``.

    The stratum of a rule is ``"+".join(systems) / lifecycle``. If the pool is
    no larger than ``n``, the whole pool is returned.
    """
    if len(pool) <= n:
        return list(pool)
    strata: Dict[str, List[PooledRule]] = defaultdict(list)
    for p in pool:
        strata["+".join(sorted(p.systems)) + "/" + (p.lifecycle or "-")].append(p)
    rng = random.Random(seed)
    quota = {s: max(min(min_per_stratum, len(m)), round(n * len(m) / len(pool))) for s, m in strata.items()}
    picked: List[PooledRule] = []
    for s in sorted(strata):
        members = sorted(strata[s], key=lambda p: p.key)
        picked += rng.sample(members, min(quota[s], len(members)))
    return sorted(picked, key=lambda p: p.key)


def write_sheet(
    rules: Sequence[PooledRule],
    path,
    labels: Optional[Mapping[str, str]] = None,
    seed: int = 42,
) -> Path:
    """Annotation sheet in random order (so neighbours do not cue each other)."""
    labels = labels or {}
    order = list(rules)
    random.Random(seed).shuffle(order)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=SHEET_COLUMNS)
        w.writeheader()
        for i, p in enumerate(order, 1):
            iris = rule_iris(p.key)
            w.writerow({
                "id": f"R{i:04d}",
                "rule": p.key,
                "readable": readable_rule(p.key),
                "labels": "; ".join(f"{shorten(i)} = {labels[i]}" for i in iris if i in labels),
                "links": " ".join(browse_url(i) for i in iris if i.startswith("http")),
                "systems": "+".join(sorted(p.systems)),
                "lifecycle": p.lifecycle,
                "mean_confidence": "" if p.mean_confidence is None else f"{p.mean_confidence:.3f}",
                "confidence_series": p.confidence_series,
                "judge_a": "", "judge_b": "", "final": "", "note": "",
            })
    return path


def fetch_wikidata_labels(iris: Iterable[str], lang: str = "en", timeout: float = 30.0) -> Dict[str, str]:
    """English labels for Wikidata IRIs via the public ``wbgetentities`` API (50 per call)."""
    import urllib.parse
    import urllib.request

    ns = "http://www.wikidata.org/entity/"
    ids = sorted({i[len(ns):] for i in iris if i.startswith(ns)})
    out: Dict[str, str] = {}
    for k in range(0, len(ids), 50):
        query = urllib.parse.urlencode({
            "action": "wbgetentities", "ids": "|".join(ids[k:k + 50]), "props": "labels",
            "languages": lang, "format": "json",
        })
        req = urllib.request.Request(
            "https://www.wikidata.org/w/api.php?" + query, headers={"User-Agent": "TSARM-annotation/0.1"}
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            ents = json.load(resp).get("entities", {})
        for qid, ent in ents.items():
            label = ent.get("labels", {}).get(lang, {}).get("value")
            if label:
                out[ns + qid] = label
    return out


# ---------------------------------------------------------------------------
# Score
# ---------------------------------------------------------------------------


def _judgement(value: str) -> Optional[int]:
    v = (value or "").strip().lower()
    if v in ("1", "y", "yes", "valid"):
        return 1
    if v in ("0", "n", "no", "invalid"):
        return 0
    return None


def cohen_kappa(a: Sequence[int], b: Sequence[int]) -> float:
    """Cohen's kappa for two raters' binary labels (``nan`` when undefined)."""
    if len(a) != len(b) or not a:
        return float("nan")
    n = len(a)
    po = sum(x == y for x, y in zip(a, b)) / n
    ca, cb = Counter(a), Counter(b)
    pe = sum(ca[c] * cb[c] for c in (0, 1)) / (n * n)
    return float("nan") if pe == 1 else (po - pe) / (1 - pe)


def wilson_interval(k: int, n: int, z: float = 1.959964) -> Tuple[float, float]:
    if n == 0:
        return (float("nan"), float("nan"))
    p = k / n
    d = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / d
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, centre - half), min(1.0, centre + half))


RETEST_COLUMNS = ["id", "rule", "readable", "labels", "links", "retest", "note"]


def write_retest_sheet(sheet, path, fraction: float = 0.25, seed: int = 7) -> Path:
    """Blind re-labelling sheet for the single-annotator protocol.

    Draws a random ``fraction`` of the rules that ``judge_a`` decided, in a new
    random order, without the first labels, the systems, the lifecycle class or
    the confidences, so that the second pass is independent of the first.
    """
    with open(sheet, newline="", encoding="utf-8-sig") as fh:
        rows = [r for r in csv.DictReader(fh) if _judgement(r["judge_a"]) is not None]
    if not rows:
        raise ValueError(f"{sheet}: no rule has a decided judge_a label yet")
    k = max(1, round(fraction * len(rows)))
    picked = random.Random(seed).sample(rows, min(k, len(rows)))
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=RETEST_COLUMNS)
        w.writeheader()
        for r in picked:
            w.writerow({c: r.get(c, "") for c in RETEST_COLUMNS[:5]} | {"retest": "", "note": ""})
    return path


@dataclass
class AnnotationScore:
    n_rules: int
    n_both_decided: int
    agreement: float
    kappa: float
    n_disagreements: int
    reference: List[str]
    per_system: Dict[str, Dict[str, float]]
    mode: str = "inter-rater"  # "inter-rater" (judge_a vs judge_b) or "intra-rater" (judge_a vs retest)


def score_sheet(path, retest=None) -> AnnotationScore:
    """Agreement, reference set and per-system precision / relative recall.

    Two annotators: agreement and kappa compare ``judge_a`` with ``judge_b``,
    and a rule's verdict is ``final`` if given, else the agreed label.

    One annotator (``judge_b`` empty throughout): pass the blind re-labelling
    sheet as ``retest``. Agreement and kappa then compare each re-labelled
    rule's ``judge_a`` with its ``retest`` label (intra-rater reliability), and
    a rule's verdict is ``final`` if given, else ``judge_a``.
    """
    with open(path, newline="", encoding="utf-8-sig") as fh:
        rows = list(csv.DictReader(fh))
    single = all(_judgement(r.get("judge_b", "")) is None for r in rows)
    pairs, verdict, disagreements = [], {}, 0
    if single:
        first = {r["rule"]: _judgement(r["judge_a"]) for r in rows}
        if retest is not None:
            with open(retest, newline="", encoding="utf-8-sig") as fh:
                for r in csv.DictReader(fh):
                    a, b = first.get(r["rule"]), _judgement(r["retest"])
                    if a is not None and b is not None:
                        pairs.append((a, b))
                        disagreements += a != b
    for r in rows:
        a, b, final = _judgement(r["judge_a"]), _judgement(r.get("judge_b", "")), _judgement(r.get("final", ""))
        if not single and a is not None and b is not None:
            pairs.append((a, b))
            disagreements += a != b
        if final is not None:
            verdict[r["rule"]] = final
        elif a is not None and (single or a == b):
            verdict[r["rule"]] = a
    reference = sorted(k for k, v in verdict.items() if v == 1)
    ref = set(reference)
    per_system: Dict[str, Dict[str, float]] = {}
    systems = sorted({s for r in rows for s in r["systems"].split("+") if s})
    for s in systems:
        judged = [r["rule"] for r in rows if s in r["systems"].split("+") and r["rule"] in verdict]
        hits = sum(k in ref for k in judged)
        lo, hi = wilson_interval(hits, len(judged))
        found = {r["rule"] for r in rows if s in r["systems"].split("+")}
        per_system[s] = {
            "judged": len(judged), "valid": hits,
            "precision": hits / len(judged) if judged else float("nan"),
            "precision_ci_low": lo, "precision_ci_high": hi,
            "relative_recall": len(found & ref) / len(ref) if ref else float("nan"),
        }
    a = [x for x, _ in pairs]
    b = [y for _, y in pairs]
    return AnnotationScore(
        n_rules=len(rows),
        n_both_decided=len(pairs),
        agreement=sum(x == y for x, y in pairs) / len(pairs) if pairs else float("nan"),
        kappa=cohen_kappa(a, b),
        n_disagreements=disagreements,
        reference=reference,
        per_system=per_system,
        mode="intra-rater" if single else "inter-rater",
    )


def write_reference(keys: Iterable[str], path, note: str = "") -> Path:
    """Reference CSV readable by :func:`src.evaluation.quality.load_reference_rules`."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        if note:
            fh.write(f"# {note}\n")
        w = csv.writer(fh)
        w.writerow(["antecedent", "consequent"])
        for k in keys:
            ante, cons = split_key(k)
            w.writerow([";".join(ante), ";".join(cons)])
    return path
