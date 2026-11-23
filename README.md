# TSARM — Temporal Semantic Association Rule Mining

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.24470493.svg)](https://doi.org/10.5281/zenodo.24470493)

A scalable framework for **Temporal Semantic Association Rule Mining in Evolving
Knowledge Graphs**, built on Apache Spark (PySpark) and RDFLib.



## What it does

TSARM mines association rules from **time-partitioned RDF** knowledge graphs and
scores them with temporal significance metrics (temporal support, temporal
confidence drift, rule persistence). It is designed to scale from a local
laptop to Google Cloud Dataproc / AWS EMR, and to be benchmarked against
[SANSA](https://sansa-stack.net/) and
[RDFRules](https://github.com/propi/rdfrules).

## Pipeline stages

| Stage | Module | Status |
|-------|--------|--------|
| Ingestion — RDF → time-partitioned Parquet | [`src/ingestion/`](src/ingestion/) | ✅ implemented |
| Windowing — sliding temporal windows | [`src/windowing/`](src/windowing/) | ✅ implemented |
| Mining — distributed semantic ARM | [`src/mining/`](src/mining/) | ✅ implemented |
| Metrics — temporal significance | [`src/metrics/`](src/metrics/) | ✅ implemented |
| Evaluation — baseline comparison harness | [`src/evaluation/`](src/evaluation/) | ✅ implemented |

## Setup

Requires **Java 17** (PySpark 4.x needs JDK 17+) and **Python 3.9+**.

```bash
brew install openjdk@17
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"   # so Spark finds the right JVM
python3 -m pip install -r requirements.txt
```

## Quick start

Run the full pipeline (ingestion → windowing → mining → temporal metrics) on the
committed sample snapshots:

```bash
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
python3 scripts/run_pipeline.py
```

Or just ingest the samples into snapshot-partitioned Parquet:

```bash
python3 scripts/ingest_samples.py
```

## Temporal metrics

Computed per candidate rule over **all** windows `W` (proposal §3.5.5; see
[`src/metrics/temporal.py`](src/metrics/temporal.py)). A rule is a candidate if
it is frequent in at least one window; its support/confidence/lift are then
recounted exactly in every window ([`recount_candidates`](src/mining/arm.py)),
so a window where it was merely below threshold contributes its true value
rather than zero.

- **Temporal support** (Eq. 3.5) — `TempSupp(r) = (1/|W|) Σ_w supp_w(r)`
- **Temporal confidence drift** (Eq. 3.6) — mean first difference
  `Drift(r) = Σ_i [conf_{w_{i+1}}(r) − conf_{w_i}(r)] / (|W| − 1)`
- **Rule Persistence Score** (Eq. 3.7) — `RPS(r) = (1/|W|)·|{w : conf_w(r) ≥ τ}|`, τ = 0.70

Each rule is then given exactly one **lifecycle class** (Table 3.2 as amended,
ε = 0.05, evaluated in order): transient (RPS < 0.2), emerging (Drift > ε),
decaying (Drift < −ε), stable (|Drift| ≤ ε, RPS ≥ 0.5) and intermittent
(|Drift| ≤ ε, 0.2 ≤ RPS < 0.5). The ranked rule set
(class, metrics, full per-window trajectories) is written as JSON + CSV by
[`src/metrics/export.py`](src/metrics/export.py).

## Baseline comparison (objective iv)

Compare TSARM against SANSA and RDFRules on the same datasets:

```bash
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
python3 scripts/run_benchmark.py     # writes results/benchmark_sample.csv
```

TSARM runs in-process. SANSA / RDFRules are external JVM systems wired in via
command templates — set `$SANSA_CMD` / `$RDFRULES_CMD` (placeholders:
`{input} {output} {min_support} {min_confidence}`; see
[`src/evaluation/adapters.py`](src/evaluation/adapters.py)). Until configured
they are reported as *skipped* rather than blocking the run. The harness reports
**scalability** (runtime, input size), **rule quality** (rule count, mean
support/confidence) and **temporal sensitivity** (persistence / drift, which the
snapshot-based baselines cannot produce).

#### Head-to-head with AMIE+ and RDFRules

Both baselines are wired in: AMIE 3.5.1 via [`scripts/amie_mine.py`](scripts/amie_mine.py)
(`$AMIE_CMD`) and RDFRules 1.9.0 via [`scripts/rdfrules_mine.py`](scripts/rdfrules_mine.py)
(`$RDFRULES_CMD`). Download commands and the exact environment are in
[`docs/experiments.md`](docs/experiments.md) (E3–E5).

```bash
caffeinate -i python3 scripts/run_benchmark.py --synthetic 20000 --repeats 3
python3 scripts/evaluate_quality.py --entities 20000
```

Runtime on synthetic data (Apple M5 Pro; one discarded warm-up round, then
mean ± SD of 3 paired rounds; 1-hour wall-clock limit per system; AMIE 3.5.1
and RDFRules 1.9.0 run on the pooled graph):

| Input triples | TSARM | AMIE+ | RDFRules |
| --- | --- | --- | --- |
| 16 k | 2.3 ± 0.3 s | **0.6 ± 0.1 s** | 3.7 ± 0.1 s |
| 160 k | **3.1 ± 0.3 s** | 26.2 ± 0.9 s | 236.4 ± 8.9 s |
| 1.6 M | **9.0 ± 0.4 s** | timeout (> 1 h) | timeout (> 1 h) |

AMIE+ is fastest on the smallest input, where Spark's fixed start-up cost
dominates TSARM's runtime. From 160 k triples on, TSARM is faster than both
(paired t-test, p < 0.001 at 160 k; n = 3, so statistical power is low).
At 1.6 M triples neither baseline finished within an hour.

**Rule quality and temporal sensitivity** (E3/E4, 160 k triples,
[`scripts/evaluate_quality.py`](scripts/evaluate_quality.py)), scored against
the two rules the generator plants:

| System | Rules mined | Comparable | Precision | Recall | Planted transient rule |
| --- | --- | --- | --- | --- | --- |
| **TSARM** | 4 | 4 | 0.50 | 1.00 | *decaying*: confidence 1.00 → 0.20, detected (TS = 1.0) |
| AMIE+ | 40 | 40 | 0.05 | 1.00 | pooled confidence 1.0, no change visible |
| RDFRules | 226 887 | 40 | 0.05 | 1.00 | pooled confidence 1.0, no change visible |

All three systems find both planted rules. TSARM's two extra rules are their
converses (Org ⇒ Class), which hold in the data but were not planted; scoring
is strict, so they count as false positives. TSARM classifies both as
*transient*, because their confidence stays below τ = 0.70 in every window. "Comparable" means a rule can be
written as TSARM items (one variable, constant objects); the other RDFRules
rules use variable chains and are not scored. The baselines mine the pooled
graph, so they rate the planted transient rule as fully confident and cannot
show that it collapsed.

> **SANSA:** its ARM module (AmieSpark) exists only in the legacy SANSA v0.7.1
> (Scala 2.11 / Spark 2.x) and was removed from current SANSA — incompatible
> with this project's Spark 4.0 stack, so a SANSA run is not pursued. See
> [`docs/baselines.md`](docs/baselines.md) for the feasibility analysis. The
> harness keeps a ready `$SANSA_CMD` adapter slot.

### Scalability sweep

A synthetic temporal-RDF generator ([`src/evaluation/synthetic.py`](src/evaluation/synthetic.py))
produces datasets at controllable size with *known* persistent and transient
rule patterns, so scalability and temporal sensitivity can be measured
reproducibly offline:

```bash
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
python3 scripts/scale_test.py 2000 10000 40000   # entity counts -> results/scale_test.csv
```

#### Core scaling on one machine (experiment E1)

[`scripts/core_scaling.py`](scripts/core_scaling.py) runs the full pipeline
(ingest → window → mine + recount → temporal metrics) with Spark `local[p]`.
Each core count gets one discarded warm-up run, then 3 measured runs. Shuffle
partitions are fixed, so every run does identical work. Results are saved with
a hardware fingerprint.

```bash
python3 scripts/core_scaling.py --mode strong --cores 1,2,4,8,15 --entities 1500000
python3 scripts/core_scaling.py --mode weak   --cores 1,2,4,8,15 --entities 150000
```

**Strong scaling**: 12M triples, Apple M5 Pro (15 cores, 24 GB), mean ± SD of
3 runs:

| Cores | Runtime | Speedup | Efficiency | Serial fraction (Karp–Flatt) |
| --- | --- | --- | --- | --- |
| 1 | 122.9 ± 4.7 s | 1.00 | 1.00 | n/a |
| 2 | 52.5 ± 4.3 s | 2.34 | 1.17 | −0.15 |
| 4 | 38.3 ± 4.4 s | 3.21 | 0.80 | 0.08 |
| 8 | 32.7 ± 9.6 s | 3.76 | 0.47 | 0.16 |
| 15 | 25.6 ± 5.1 s | **4.81** | 0.32 | 0.15 |

On a 10× smaller input (1.2M triples) the serial fraction is 0.31–0.50 and
speedup peaks at 2.25×. Fixed coordination costs dominate small inputs, and
larger inputs scale better.

**Weak scaling**: 150k entities per core (1.2M → 18M triples). Per-worker
efficiency is about 1.0 up to 4 cores (1.06, 0.98) and 0.75 at 8 cores. At 15
cores it drops to about 0.3 (re-measured with 5 runs). Giving Spark a larger
heap made it *worse*, which points to the 24 GB of physical memory, not CPU,
as the single-node limit at high core counts. Pooling memory across nodes
(experiment E2, [`docs/oracle_cluster.md`](docs/oracle_cluster.md)) is the
remedy.

The injected persistent rule scores persistence 1.0 (*stable*) and the
transient rule is classified *decaying*, confirming temporal sensitivity at
scale.

### Real-world data (DBpedia)

[`scripts/run_dbpedia.py`](scripts/run_dbpedia.py) runs the pipeline on two real
DBpedia `instance_types_en` release dumps (2015-10 and 2016-10) as temporal
snapshots. Spark reads the `.bz2` dumps directly. Download them into
`data/raw/` first (≈42 MB each):

```bash
curl -L -o data/raw/dbpedia_2015-10_instance_types_en.ttl.bz2 \
  https://downloads.dbpedia.org/2015-10/core-i18n/en/instance_types_en.ttl.bz2
curl -L -o data/raw/dbpedia_2016-10_instance_types_en.ttl.bz2 \
  https://downloads.dbpedia.org/2016-10/core-i18n/en/instance_types_en.ttl.bz2
```

Result on the Apple M5 Pro: **10.2 M real triples** ingested in 12.3 s
(~831 k triples/s; 26 s end to end); 5.58 M distinct entities; 236 k entities whose type was *refined*
between releases. Mining surfaces the type-evolution rule
`type=owl:Thing ⇒ type=dbo:Person` (confidence 0.52, lift 4.57) — generic
entities reclassified to a specific class across releases, the evolving-KG
pattern TSARM targets. (`instance_types` carries one type per entity per
release, so richer rules need a multi-predicate dump such as
`mappingbased_objects`.)

#### Multi-window temporal trajectories

[`scripts/run_dbpedia_temporal.py`](scripts/run_dbpedia_temporal.py) adds the
2016-04 release for **three** snapshots, giving two overlapping windows
(`{2015-10, 2016-04}`, `{2016-04, 2016-10}`) and so a real confidence trajectory
per rule (15.4 M triples, 43 s end to end on an Apple M5 Pro). Each candidate
rule's confidence is recounted in *every* window, including windows where it was
below the mining threshold, so its trajectory is exact:

| Rule | Frequent in | Confidence (w0 → w1) | Drift | Persistence | Lifecycle |
| --- | --- | --- | --- | --- | --- |
| `owl:Thing ⇒ Person` | w0, w1 | 0.299 → 0.344 | +0.045 | 1.0 | stable |
| `Person ⇒ owl:Thing` | w1 | 0.093 → 0.213 | +0.120 | 0.5 | emerging |

The first rule holds in both windows with drift inside the ±0.05 stability band.
The second is not absent from the earlier window: it is already present at
confidence 0.093, below the mining threshold, and more than doubles. TSARM
therefore classifies it as an *emerging* rule, the temporal significance signal
(objectives ii & iv) the metrics are designed to capture, here on real
evolving-KG data. Zero-padding absent windows would misreport this rule as
transient.

#### Class items with the DBpedia ontology

With `--item-mode class`, each entity's items are its classes plus their
ancestors in the DBpedia ontology's own hierarchy (710 classes, depth 6;
schema.org/DOLCE mappings excluded):

```bash
bash scripts/download_dbpedia.sh        # releases + ontology (Databus, Wayback fallback)
python3 scripts/run_dbpedia_temporal.py --item-mode class --tag class_dbo_full
```

Class items are mined by a level-wise miner ([`src/mining/generalized.py`](src/mining/generalized.py))
that caps itemsets at three items and never builds an itemset holding a class
together with one of its ancestors (Srikant & Agrawal, 1995). Such itemsets are
redundant, and Spark's FP-Growth can neither skip them nor cap their length.
Without a hierarchy the miner returns exactly FP-Growth's rules (tested). On the
three releases it finds generalised rules such as `owl:Thing ⇒ Agent`, whose
confidence rises from 0.302 to 0.353.

#### Valid-time windows

[`scripts/run_wikidata_temporal.py`](scripts/run_wikidata_temporal.py) can also
window one dump by the *valid time* of its statements (Wikidata P580/P582/P585),
with calendar windows `[t, t+δ)`:
`--snapshots 2026-11 --valid-time 1975-01,2025-01,60,60`.

## Install / reproducibility

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e .            # or: pip install -e ".[test,notebooks]"
```

The project is packaged (`setup.cfg` + `pyproject.toml`), MIT-licensed, and
ships a [`CITATION.cff`](CITATION.cff). Every release is archived on Zenodo (objective iii):
cite **all versions** with the concept DOI [10.5281/zenodo.24470493](https://doi.org/10.5281/zenodo.24470493),
or a specific version, e.g. v0.9.0: [10.5281/zenodo.24470494](https://doi.org/10.5281/zenodo.24470494). Dependencies are pinned in [`requirements.txt`](requirements.txt).

## Ingestion design

Each input RDF file is treated as a **snapshot** valid at a known timestamp.
Triples are normalised to a canonical schema and tagged with their snapshot id +
validity time, then written as Parquet partitioned by snapshot so later stages
read only the snapshots a sliding window touches.

Canonical schema: `subject, predicate, object, object_kind`
(`iri`/`blank`/`literal`), `datatype`, `language`, `snapshot`, `snapshot_ts`.

- **N-Triples (`.nt`)** are read distributed via `spark.read.text` + Spark SQL
  regex parsing — no triple data passes through the driver.
- **Turtle / RDF-XML / JSON-LD** small samples use the in-memory RDFLib parser
  (pass `fmt="turtle"` etc.).

## Tests

```bash
python3 -m pytest
```

`tests/test_rdf_parser.py` runs without Spark; `tests/test_ingest.py` and
`tests/test_windowing.py` spin up a local SparkSession. `tests/conftest.py`
auto-points `JAVA_HOME` at a Homebrew JDK 17+ if it is not already set.

## Project layout

```
data/raw/         Raw RDF dumps (+ committed sample_*.nt)
data/processed/   Generated Parquet (gitignored)
src/ingestion/    RDF parsing + Spark ingestion  ← current focus
src/windowing/    Sliding-window module (planned)
src/mining/       Distributed ARM (planned)
src/metrics/      Temporal significance metrics (planned)
scripts/          Runnable example drivers
tests/            Unit + integration tests
notebooks/        Exploratory Jupyter notebooks
```
