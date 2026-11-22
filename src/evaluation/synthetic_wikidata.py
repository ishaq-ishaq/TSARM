"""Synthetic Wikidata-like snapshot series, for rehearsing the Wikidata run.

Writes files in exactly the format of ``scripts/extract_wikidata.py``
(``<snapshot>.tsv.gz`` with P31/P279 statements), so the real driver
``scripts/run_wikidata_temporal.py`` runs on them unchanged. The data imitate
the properties of Wikidata that matter for TSARM's cost and behaviour:

* a deep class hierarchy (P279) in which classes have one to three parents;
* a heavy-tailed class distribution: a few giant classes (in Wikidata, human,
  scholarly article, taxon, Wikimedia category) and a long tail of small ones;
* a population that grows from snapshot to snapshot;
* entities with one or two P31 classes.

Two **reclassification events** are planted at a known date, giving a ground
truth for temporal sensitivity (E4):

* ``decay``: before the event, entities of class ``A`` are also typed ``B``;
  afterwards most of them lose ``B``, so ``{A} => {B}`` decays;
* ``emergence``: from the event on, entities of class ``C`` are also typed
  ``D``, so ``{C} => {D}`` emerges.
"""

from __future__ import annotations

import gzip
import random
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

ENTITY = "http://www.wikidata.org/entity/"
P31 = "http://www.wikidata.org/prop/direct/P31"
P279 = "http://www.wikidata.org/prop/direct/P279"
HEADER = "subject\tpredicate\tobject\trank\tbest_rank\tstart_time\tstart_precision\tend_time\tend_precision\tpoint_in_time\tpoint_precision\n"

SNAPSHOTS = ("2017-01", "2018-01", "2019-01", "2020-09", "2021-01", "2022-01", "2026-11")
# Share of the final population present in each snapshot (Wikidata grew ~4x 2017->2026).
GROWTH = (0.25, 0.35, 0.50, 0.65, 0.72, 0.80, 1.00)
EVENT_AT = 3  # index of the first snapshot after the planted events (2020-09)

# Planted classes: Q-ids outside the generated hierarchy's range.
A, B, C, D = "Q9000001", "Q9000002", "Q9000003", "Q9000004"


@dataclass
class PlantedEvent:
    antecedent: str
    consequent: str
    kind: str  # "decay" or "emergence"
    when: datetime


def _hierarchy(n_classes: int, max_depth: int, rng: random.Random) -> Dict[int, List[int]]:
    """class -> parents; class 0 is the root. Depth grows with class id."""
    parents: Dict[int, List[int]] = {0: []}
    by_depth: Dict[int, List[int]] = {0: [0]}
    for c in range(1, n_classes):
        depth = min(max_depth, 1 + int(max_depth * (c / n_classes) ** 0.5))
        pool = by_depth.get(depth - 1) or by_depth[max(k for k in by_depth if k < depth)]
        k = rng.choices((1, 2, 3), weights=(0.55, 0.30, 0.15))[0]
        # Preferential attachment: early classes in a level attract more children.
        ps = {pool[min(int(rng.paretovariate(1.2)) - 1, len(pool) - 1)] for _ in range(k)}
        parents[c] = sorted(ps)
        by_depth.setdefault(depth, []).append(c)
    return parents


def generate(
    out_dir: Path,
    n_entities: int = 200_000,
    n_classes: int = 5_000,
    max_depth: int = 12,
    planted_share: float = 0.03,
    seed: int = 7,
) -> Tuple[List[Path], List[PlantedEvent], int]:
    """Write the seven snapshots. Returns (files, planted events, total rows)."""
    rng = random.Random(seed)
    out_dir.mkdir(parents=True, exist_ok=True)
    parents = _hierarchy(n_classes, max_depth, rng)
    leaves = list(range(n_classes // 4, n_classes))
    giants = leaves[:4]  # human, scholarly article, taxon, category
    giant_weights = (0.10, 0.30, 0.04, 0.05)

    # Entity profile, fixed across snapshots (except the planted changes).
    entity_classes: List[Tuple[str, ...]] = []
    for _ in range(n_entities):
        r = rng.random()
        acc, cls = 0.0, None
        for g, w in zip(giants, giant_weights):
            acc += w
            if r < acc:
                cls = g
                break
        if cls is None:
            cls = leaves[4 + int((rng.paretovariate(0.8) - 1) * 50) % (len(leaves) - 4)]
        types = [f"Q{cls + 1}"]
        if rng.random() < 0.15:
            types.append(f"Q{rng.choice(leaves) + 1}")
        entity_classes.append(tuple(types))

    n_planted = int(n_entities * planted_share)
    planted_a = range(0, n_planted)              # A, with B before the event
    planted_c = range(n_planted, 2 * n_planted)  # C, with D from the event on
    order = list(range(n_entities))
    rng.shuffle(order)
    first_seen = {e: i / n_entities for i, e in enumerate(order)}  # join order -> growth

    files, total = [], 0
    for s, (snap, share) in enumerate(zip(SNAPSHOTS, GROWTH)):
        path = out_dir / f"{snap}.tsv.gz"
        srng = random.Random(seed * 100 + s)
        rows = 0
        with gzip.open(path, "wt", compresslevel=1) as fh:
            fh.write(HEADER)
            for c, ps in parents.items():
                for p in ps:
                    fh.write(f"{ENTITY}Q{c + 1}\t{P279}\t{ENTITY}Q{p + 1}\tnormal\ttrue\t\t\t\t\t\t\n")
                    rows += 1
            for x, (y) in ((A, "Q2"), (B, "Q2"), (C, "Q3"), (D, "Q3")):
                fh.write(f"{ENTITY}{x}\t{P279}\t{ENTITY}{y}\tnormal\ttrue\t\t\t\t\t\t\n")
                rows += 1
            for e in range(n_entities):
                in_planted = e < 2 * n_planted
                if not in_planted and first_seen[e] >= share:
                    continue  # not created yet (planted entities exist from the start)
                types = list(entity_classes[e])
                if e in planted_a:
                    types = [A]
                    if s < EVENT_AT or srng.random() < 0.15:
                        types.append(B)
                elif e in planted_c:
                    types = [C]
                    if s >= EVENT_AT and srng.random() < 0.90:
                        types.append(D)
                    elif s < EVENT_AT and srng.random() < 0.10:
                        types.append(D)
                subj = f"{ENTITY}Q{20_000_000 + e}"
                for t in types:
                    fh.write(f"{subj}\t{P31}\t{ENTITY}{t}\tnormal\ttrue\t\t\t\t\t\t\n")
                    rows += 1
        files.append(path)
        total += rows
    when = datetime.strptime(SNAPSHOTS[EVENT_AT], "%Y-%m")
    events = [
        PlantedEvent(f"{ENTITY}{A}", f"{ENTITY}{B}", "decay", when),
        PlantedEvent(f"{ENTITY}{C}", f"{ENTITY}{D}", "emergence", when),
    ]
    return files, events, total
