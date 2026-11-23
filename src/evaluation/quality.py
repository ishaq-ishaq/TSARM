"""Rule quality (E3) and temporal sensitivity (E4): proposal Sec. 3.7.3 ii-iii.

**Rule quality** compares a mined rule set ``R_mined`` with a ground-truth
reference set ``R_true``::

    P   = |R_mined ∩ R_true| / |R_mined|
    Rec = |R_mined ∩ R_true| / |R_true|
    F1  = 2 * P * Rec / (P + Rec)

Rules are compared by a canonical key, ``sorted(antecedent) => sorted(consequent)``
over items of the form ``predicate=object``, identical to TSARM's ``rule_key``.
AMIE's Horn-rule output is converted to the same key when every atom is
``?a <p> <constant>`` on one shared variable, i.e. the subset of AMIE rules that
expresses the same kind of pattern as TSARM. Other AMIE rules (multi-variable
chains) are reported as *not comparable*, not counted as wrong.

**Temporal sensitivity** checks a reference list of dated events, each saying
a rule should *emerge* or *decay* at a given time::

    TS = |E_correct| / |E|

An event is correctly identified when the rule's confidence, compared between
the last window that ends before the event and the first window that starts
at or after it, moves in the expected direction by more than ``epsilon``
(the drift stability band of Table 3.2, default 0.05).
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

ITEM_SEP = "="

# ---------------------------------------------------------------------------
# Rule keys
# ---------------------------------------------------------------------------


def rule_key(antecedent: Iterable[str], consequent: Iterable[str]) -> str:
    """Canonical, order-independent rule key (same as TSARM's ``rule_key``)."""
    return ",".join(sorted(antecedent)) + " => " + ",".join(sorted(consequent))


_ATOM = re.compile(r"(\?\w+)\s+<([^>]*)>\s+(<[^>]*>|\?\w+|\S+)")


def amie_rule_key(rule: str) -> Optional[str]:
    """TSARM-style key for an AMIE rule, or ``None`` if not comparable.

    ``?a <worksAt> <Org0>  => ?a <type> <Class0>`` becomes
    ``worksAt=Org0 => type=Class0`` (full IRIs kept). Comparable only if all
    atoms share one subject variable and have constant objects.
    """
    if "=>" not in rule:
        return None
    body, head = rule.split("=>", 1)
    sides: List[List[str]] = []
    subjects: Set[str] = set()
    for side in (body, head):
        atoms = _ATOM.findall(side)
        if not atoms:
            return None
        items = []
        for subj, pred, obj in atoms:
            if not obj.startswith("<"):
                return None  # variable or literal object: not an item pattern
            subjects.add(subj)
            items.append(f"{pred}{ITEM_SEP}{obj[1:-1]}")
        sides.append(items)
    if len(subjects) != 1:
        return None
    return rule_key(sides[0], sides[1])


def _rdfrules_iri(node) -> Optional[str]:
    """IRI string of an RDFRules export node value, or None if not an IRI."""
    if isinstance(node, dict) and "nameSpace" in node and "localName" in node:
        return f"{node['nameSpace']}{node['localName']}"
    return None


def rdfrules_rule_key(rule: dict) -> Optional[str]:
    """TSARM-style key for one rule from an RDFRules JSON export, or ``None``.

    Comparable only when every atom has the same variable subject and a
    constant IRI object (literal objects are excluded, as in TSARM's items).
    """
    body = rule.get("body") or []
    head = rule.get("head")
    if not body or not isinstance(head, dict):
        return None
    sides: List[List[str]] = []
    subjects: Set[str] = set()
    for atoms in (body, [head]):
        items = []
        for atom in atoms:
            subj, obj, pred = atom.get("subject", {}), atom.get("object", {}), atom.get("predicate")
            if subj.get("type") != "variable" or obj.get("type") != "constant":
                return None
            p_iri, o_iri = _rdfrules_iri(pred), _rdfrules_iri(obj.get("value"))
            if not p_iri or not o_iri:
                return None
            subjects.add(subj.get("value"))
            items.append(f"{p_iri}{ITEM_SEP}{o_iri}")
        sides.append(items)
    if len(subjects) != 1:
        return None
    return rule_key(sides[0], sides[1])


# ---------------------------------------------------------------------------
# E3: precision / recall / F1
# ---------------------------------------------------------------------------


@dataclass
class Quality:
    n_mined: int
    n_true: int
    n_correct: int
    precision: float
    recall: float
    f1: float


def precision_recall_f1(mined: Iterable[str], true: Iterable[str]) -> Quality:
    m, t = set(mined), set(true)
    hit = len(m & t)
    p = hit / len(m) if m else 0.0
    r = hit / len(t) if t else 0.0
    f1 = 2 * p * r / (p + r) if (p + r) else 0.0
    return Quality(len(m), len(t), hit, p, r, f1)


def load_reference_rules(path) -> Set[str]:
    """Reference rules from CSV with columns ``antecedent,consequent``.

    Multiple items in one cell are separated by ``;``. Lines starting with
    ``#`` are comments, so the file can carry validation notes.
    """
    keys = set()
    with open(path, newline="", encoding="utf-8") as fh:
        rows = csv.DictReader(line for line in fh if not line.lstrip().startswith("#"))
        for row in rows:
            ante = [x.strip() for x in row["antecedent"].split(";") if x.strip()]
            cons = [x.strip() for x in row["consequent"].split(";") if x.strip()]
            if ante and cons:
                keys.add(rule_key(ante, cons))
    return keys


# ---------------------------------------------------------------------------
# E4: temporal sensitivity
# ---------------------------------------------------------------------------


@dataclass
class Event:
    rule_key: str
    when: datetime
    kind: str  # "emergence" or "decay"
    label: str = ""


@dataclass
class EventOutcome:
    event: Event
    window_before: Optional[int]
    window_after: Optional[int]
    conf_before: Optional[float]
    conf_after: Optional[float]
    detected: bool
    reason: str


def bracketing_windows(
    windows: Dict[int, Tuple[datetime, datetime]], when: datetime
) -> Tuple[Optional[int], Optional[int]]:
    """Last window ending before ``when`` and first window starting at/after it.

    ``windows`` maps window id -> (start, end) timestamps of its snapshots.
    """
    before = [w for w, (s, e) in windows.items() if e < when]
    after = [w for w, (s, e) in windows.items() if s >= when]
    wb = max(before, key=lambda w: windows[w][1]) if before else None
    wa = min(after, key=lambda w: windows[w][0]) if after else None
    return wb, wa


def evaluate_events(
    events: Sequence[Event],
    trajectories: Dict[str, Dict[int, float]],
    windows: Dict[int, Tuple[datetime, datetime]],
    epsilon: float = 0.05,
) -> Tuple[float, List[EventOutcome]]:
    """Temporal sensitivity ``TS = |E_correct| / |E|`` plus per-event outcomes.

    Args:
        events: reference events.
        trajectories: ``rule_key -> {window_id: confidence}`` (TSARM's dense
            per-window confidence, e.g. from ``compute_metrics``).
        windows: ``window_id -> (first snapshot time, last snapshot time)``.
        epsilon: minimum confidence change that counts as a transition.
    """
    if not events:
        raise ValueError("At least one reference event is required.")
    outcomes = []
    for ev in events:
        if ev.kind not in ("emergence", "decay"):
            raise ValueError(f"Unknown event kind {ev.kind!r}")
        wb, wa = bracketing_windows(windows, ev.when)
        traj = trajectories.get(ev.rule_key)
        if wb is None or wa is None:
            outcomes.append(EventOutcome(ev, wb, wa, None, None, False, "event not bracketed by windows"))
            continue
        if traj is None:
            outcomes.append(EventOutcome(ev, wb, wa, None, None, False, "rule not among mined candidates"))
            continue
        cb, ca = traj.get(wb, 0.0), traj.get(wa, 0.0)
        change = ca - cb
        ok = change > epsilon if ev.kind == "emergence" else change < -epsilon
        reason = f"confidence {cb:.3f} -> {ca:.3f} ({change:+.3f})"
        outcomes.append(EventOutcome(ev, wb, wa, cb, ca, ok, reason))
    ts = sum(o.detected for o in outcomes) / len(outcomes)
    return ts, outcomes
