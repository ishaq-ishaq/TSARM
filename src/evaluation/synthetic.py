"""Synthetic temporal-RDF generator for scalability and sensitivity testing.

Generating data locally (rather than only relying on downloaded dumps) gives a
reproducible way to:

* sweep dataset size to measure **computational scalability** (runtime vs. number
  of triples), and
* inject *known* temporal patterns so **temporal sensitivity** can be validated:
  some association rules are made **persistent** (hold in every snapshot) and
  others **transient** (hold only early), so the metrics stage should report high
  vs. low persistence accordingly.

Schema produced (all N-Triples, so the distributed ingestion path is exercised):

* ``<entity_i> rdf:type <Class_c>``                 -- entity class membership
* ``<entity_i> :worksAt <Org_o>``                   -- a categorical relation

Persistent pattern: every entity of ``Class_0`` also has ``worksAt Org_0`` in
*all* snapshots -> rule ``{type=Class_0} => {worksAt=Org_0}`` persists.

Transient pattern: entities of ``Class_1`` have ``worksAt Org_1`` only in the
first ``transient_until`` snapshots, then switch to a random org -> the rule
``{type=Class_1} => {worksAt=Org_1}`` decays (low persistence, negative drift).
"""

from __future__ import annotations

import random
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

from .adapters import Dataset, Snapshot

EX = "http://tsarm.example.org/"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
WORKS_AT = f"{EX}worksAt"


def _triple(s: str, p: str, o: str) -> str:
    return f"<{s}> <{p}> <{o}> .\n"


def generate_snapshot_file(
    path: Path,
    n_entities: int,
    n_classes: int,
    n_orgs: int,
    snapshot_index: int,
    transient_until: int,
    seed: int,
) -> int:
    """Write one snapshot's N-Triples file. Returns the number of triples."""
    rng = random.Random(seed + snapshot_index)
    count = 0
    with open(path, "w", encoding="utf-8") as out:
        for i in range(n_entities):
            cls = i % n_classes
            entity = f"{EX}entity{i}"
            class_iri = f"{EX}Class{cls}"
            out.write(_triple(entity, RDF_TYPE, class_iri))
            count += 1

            if cls == 0:
                # Persistent: always Org0.
                org = 0
            elif cls == 1:
                # Transient: Org1 early, then drifts to a random other org.
                if snapshot_index < transient_until:
                    org = 1
                else:
                    org = rng.randrange(n_orgs)
            else:
                # Background noise: random org each snapshot.
                org = rng.randrange(n_orgs)

            out.write(_triple(entity, WORKS_AT, f"{EX}Org{org}"))
            count += 1
    return count


def generate_dataset(
    out_dir: Path,
    name: str = "synthetic",
    n_entities: int = 1000,
    n_snapshots: int = 4,
    n_classes: int = 5,
    n_orgs: int = 5,
    transient_until: int = 2,
    start_year: int = 2020,
    seed: int = 42,
) -> Tuple[Dataset, int]:
    """Generate a multi-snapshot synthetic dataset on disk.

    Args:
        out_dir: Directory to write snapshot ``.nt`` files into (created if
            needed).
        name: Dataset name used in benchmark results.
        n_entities: Entities per snapshot (drives the triple count: ~2x).
        n_snapshots: Number of temporal snapshots.
        n_classes / n_orgs: Vocabulary sizes for the type / worksAt relations.
        transient_until: Snapshot index before which the transient rule holds.
        start_year: First snapshot's year (snapshots are spaced one year apart).
        seed: RNG seed for reproducibility.

    Returns:
        ``(dataset, total_triples)`` where ``dataset`` is ready to hand to the
        benchmark harness.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    snapshots: List[Snapshot] = []
    total = 0
    for s in range(n_snapshots):
        year = start_year + s
        snap_id = str(year)
        path = out_dir / f"{name}_{snap_id}.nt"
        total += generate_snapshot_file(
            path=path,
            n_entities=n_entities,
            n_classes=n_classes,
            n_orgs=n_orgs,
            snapshot_index=s,
            transient_until=transient_until,
            seed=seed,
        )
        snapshots.append(Snapshot(snap_id, path, datetime(year, 1, 1)))

    return Dataset(name=name, snapshots=snapshots), total


# Item tokens for the injected ground-truth rules, so tests/experiments can
# locate them in the metrics output.
PERSISTENT_RULE: Dict[str, str] = {
    "antecedent": f"{RDF_TYPE}={EX}Class0",
    "consequent": f"{WORKS_AT}={EX}Org0",
}
TRANSIENT_RULE: Dict[str, str] = {
    "antecedent": f"{RDF_TYPE}={EX}Class1",
    "consequent": f"{WORKS_AT}={EX}Org1",
}
