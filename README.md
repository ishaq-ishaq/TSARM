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
| Mining — distributed semantic ARM | [`src/mining/`](src/mining/) | ⬜ planned |
| Metrics — temporal significance | [`src/metrics/`](src/metrics/) | ⬜ planned |

## Setup

Requires **Java 17** (PySpark 4.x needs JDK 17+) and **Python 3.9+**.

```bash
brew install openjdk@17
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"   # so Spark finds the right JVM
python3 -m pip install -r requirements.txt
```

## Quick start

Ingest the committed sample snapshots into snapshot-partitioned Parquet:

```bash
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
python3 scripts/ingest_samples.py
```

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
