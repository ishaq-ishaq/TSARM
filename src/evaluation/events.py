"""Candidate reference events for E4 on real data (proposal Sec. 3.7.3 iii).

E4 needs a list of dated events, each saying that a rule should *emerge* or
*decay* at a known time. On real snapshots there is no planted truth, so the
list is curated by hand. This module proposes the candidates.

The candidates come from **retyping of the same entities**, not from TSARM's
output. That keeps the evaluation from being circular: TSARM is judged on
whether its window-level confidence trajectories pick up changes found by a
different method. For each pair of consecutive snapshots ``(s, s+1)`` and
each entity present in both:

* **gain** ``X -> +Y``: the entity is typed ``X`` in both snapshots and gains
  type ``Y`` in ``s+1``. Many such entities mean that ``{X} => {Y}`` should
  *emerge* at the date of ``s+1``.
* **loss** ``X -> -Y``: the entity is typed ``X`` in both snapshots and loses
  ``Y``. Many such entities mean that ``{X} => {Y}`` should *decay*.

A change becomes a candidate when at least ``min_entities`` entities make it
and they are at least ``min_share`` of the entities of ``X`` that persist
between the two snapshots. Growth (new entities) is ignored on purpose, so
these are edits to existing data: bot runs, ontology refactorings, imports
that retype old items. Each candidate still has to be confirmed by a person,
for example against the Wikidata bot-request archive or the class's edit
history, before it enters the reference list (``confirmed`` column).

The CSV written by :func:`write_candidates` is read back by
:func:`load_events`, which keeps only confirmed rows and builds the
:class:`src.evaluation.quality.Event` objects that
:func:`src.evaluation.quality.evaluate_events` scores. Hand-written events can
be added as extra rows.
"""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import List, Sequence

from src.evaluation.quality import Event, rule_key

CANDIDATE_COLUMNS = (
    "kind", "antecedent", "consequent", "when", "from_snapshot", "to_snapshot",
    "n_entities", "n_antecedent", "share", "links", "confirmed", "source", "note",
)


def retyping_candidates(
    statements,
    min_entities: int = 100,
    min_share: float = 0.2,
    type_predicate: str = "http://www.wikidata.org/prop/direct/P31",
):
    """Candidate emergence/decay events from entity retyping (Spark DataFrame).

    Args:
        statements: TSARM canonical statements with ``subject, predicate,
            object, snapshot, snapshot_ts`` (e.g. from
            :func:`src.ingestion.wikidata.load_snapshots`).

    Returns:
        DataFrame ``kind, antecedent, consequent, from_snapshot, to_snapshot,
        when, n_entities, n_antecedent, share``, ordered by ``when`` and size.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    types = (
        statements.where(F.col("predicate") == type_predicate)
        .groupBy("snapshot", "snapshot_ts", "subject")
        .agg(F.array_sort(F.collect_set("object")).alias("types"))
    )
    order = types.select("snapshot", "snapshot_ts").distinct()
    order = order.withColumn("_next", F.lead("snapshot").over(Window.orderBy("snapshot_ts")))
    nxt = order.where(F.col("_next").isNotNull()).select(
        F.col("snapshot").alias("from_snapshot"), F.col("_next").alias("to_snapshot")
    )
    before = types.select(F.col("snapshot").alias("from_snapshot"), "subject", F.col("types").alias("t0"))
    after = types.select(
        F.col("snapshot").alias("to_snapshot"), F.col("snapshot_ts").alias("when"), "subject", F.col("types").alias("t1")
    )
    pairs = (
        before.join(nxt, "from_snapshot")
        .join(after, ["to_snapshot", "subject"])
        .withColumn("kept", F.array_intersect("t0", "t1"))
        .where(F.size("kept") > 0)
        .localCheckpoint()
    )
    # Denominator: entities of X present in both snapshots and still typed X.
    n_x = (
        pairs.select("from_snapshot", "to_snapshot", F.explode("kept").alias("antecedent"))
        .groupBy("from_snapshot", "to_snapshot", "antecedent").agg(F.count("*").alias("n_antecedent"))
    )

    def changes(kind, delta):
        return (
            pairs.withColumn("_d", delta).where(F.size("_d") > 0)
            .select("from_snapshot", "to_snapshot", "when", F.explode("kept").alias("antecedent"), "_d")
            .select("from_snapshot", "to_snapshot", "when", "antecedent", F.explode("_d").alias("consequent"))
            .groupBy("from_snapshot", "to_snapshot", "when", "antecedent", "consequent")
            .agg(F.count("*").alias("n_entities"))
            .withColumn("kind", F.lit(kind))
        )

    both = changes("emergence", F.array_except("t1", "t0")).unionByName(
        changes("decay", F.array_except("t0", "t1"))
    )
    return (
        both.join(n_x, ["from_snapshot", "to_snapshot", "antecedent"])
        .withColumn("share", F.col("n_entities") / F.col("n_antecedent"))
        .where((F.col("n_entities") >= F.lit(min_entities)) & (F.col("share") >= F.lit(min_share)))
        .select("kind", "antecedent", "consequent", "from_snapshot", "to_snapshot", "when",
                "n_entities", "n_antecedent", "share")
        .orderBy("when", F.desc("n_entities"))
    )


def _wiki_link(iri: str) -> str:
    ns = "http://www.wikidata.org/entity/"
    return "https://www.wikidata.org/wiki/" + iri[len(ns):] if iri.startswith(ns) else iri


def write_candidates(rows: Sequence, path) -> Path:
    """Write candidate rows (Spark ``Row`` objects or dicts) as a review sheet."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=CANDIDATE_COLUMNS)
        w.writeheader()
        for r in rows:
            d = r.asDict() if hasattr(r, "asDict") else dict(r)
            when = d["when"]
            w.writerow({
                **{k: d[k] for k in ("kind", "antecedent", "consequent", "from_snapshot", "to_snapshot",
                                     "n_entities", "n_antecedent")},
                "when": when.strftime("%Y-%m-%d") if hasattr(when, "strftime") else str(when),
                "share": f"{d['share']:.3f}",
                "links": f"{_wiki_link(d['antecedent'])} {_wiki_link(d['consequent'])}",
                "confirmed": "", "source": "", "note": "",
            })
    return path


def load_events(path, confirmed_only: bool = True) -> List[Event]:
    """Reference events from a reviewed candidate sheet.

    A row counts when ``confirmed`` is ``1``/``yes`` (or always, with
    ``confirmed_only=False``). ``antecedent`` and ``consequent`` may hold
    several items separated by ``;``; ``when`` is ``YYYY-MM[-DD]``.
    """
    events = []
    with open(path, newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(line for line in fh if not line.lstrip().startswith("#")):
            if confirmed_only and (r.get("confirmed") or "").strip().lower() not in ("1", "y", "yes"):
                continue
            ante = [x.strip() for x in r["antecedent"].split(";") if x.strip()]
            cons = [x.strip() for x in r["consequent"].split(";") if x.strip()]
            raw = r["when"].strip()
            when = datetime.strptime(raw, "%Y-%m-%d" if raw.count("-") == 2 else "%Y-%m")
            label = (r.get("note") or r.get("source") or "").strip()
            events.append(Event(rule_key(ante, cons), when, r["kind"].strip(), label))
    return events


def trajectories_from_report(report) -> dict:
    """``rule_key -> {window_id: confidence}`` from a TSARM JSON export."""
    return {
        rule_key(r["antecedent"], r["consequent"]): dict(zip(r["window_series"], r["confidence_series"]))
        for r in report["rules"]
    }


def window_times_from_report(report) -> dict:
    """``window_id -> (first, last snapshot date)`` from a Wikidata driver export."""
    from src.ingestion.wikidata import snapshot_ts_from_name

    times = {}
    for i, snaps in enumerate(report["params"]["windows"]):
        ds = [snapshot_ts_from_name(s) for s in snaps]
        times[i] = (min(ds), max(ds))
    return times
