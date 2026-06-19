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
