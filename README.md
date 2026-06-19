# TSARM — Temporal Semantic Association Rule Mining

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

Computed per rule across the window sequence (see
[`src/metrics/temporal.py`](src/metrics/temporal.py) for formal definitions):

- **Temporal support** — time-averaged support `TS(r) = (1/n) Σ_w supp_w(r)`
- **Temporal confidence drift** — net change `conf_last − conf_first`, its
  per-step rate, and **confidence volatility** (mean |consecutive change|)
- **Rule Persistence Score** — `RPS(r) = (1/n)·|{w : conf_w(r) ≥ τ}|`

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

### Scalability sweep

A synthetic temporal-RDF generator ([`src/evaluation/synthetic.py`](src/evaluation/synthetic.py))
produces datasets at controllable size with *known* persistent and transient
rule patterns, so scalability and temporal sensitivity can be measured
reproducibly offline:

```bash
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
python3 scripts/scale_test.py 2000 10000 40000   # entity counts -> results/scale_test.csv
```

On the M1 dev machine, pipeline runtime stays roughly flat (~10–16 s) as the
triple count grows 20× (16k → 320k), i.e. throughput rises from ~1k to ~19k
triples/s as fixed Spark startup overhead amortises — the expected distributed
scaling profile. The injected persistent rule scores persistence 1.0 while the
transient rule decays, confirming temporal sensitivity at scale.

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

Result on the M1 dev machine: **10.2 M real triples** ingested at ~151 k
triples/s; 5.58 M distinct entities; 236 k entities whose type was *refined*
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
per rule (15.4 M triples). TSARM's temporal metrics then distinguish:

| Rule | Windows | Confidence | Drift | Persistence |
| --- | --- | --- | --- | --- |
| `owl:Thing ⇒ Person` | w0, w1 | 0.299 → 0.344 | +0.045 | 1.0 (persistent) |
| `Person ⇒ owl:Thing` | w1 only | 0.213 | 0.0 | 0.5 (transient) |

The persistent rule's confidence *rises* across the 2015→2016 releases (positive
drift), while the second rule appears only in the later window — exactly the
temporal significance signal (objectives ii & iv) the metrics are designed to
capture, here on real evolving-KG data.

## Install / reproducibility

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e .            # or: pip install -e ".[test,notebooks]"
```

The project is packaged (`setup.cfg` + `pyproject.toml`), MIT-licensed, and
ships a [`CITATION.cff`](CITATION.cff); a Zenodo DOI is to be minted on release
(objective iii). Dependencies are pinned in [`requirements.txt`](requirements.txt).

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
