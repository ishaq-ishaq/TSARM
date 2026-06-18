# TSARM — Temporal Semantic Association Rule Mining

A scalable framework for **Temporal Semantic Association Rule Mining in Evolving
Knowledge Graphs**, built on Apache Spark (PySpark) and RDFLib.

> MSc research project — Isya Isyaku (24/508CSCE/032), University of Abuja,
> Department of Computer Science. Supervisor: Dr. Fatima Binta Abdullahi.

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
| Windowing — sliding temporal windows | [`src/windowing/`](src/windowing/) | ⬜ planned |
| Mining — distributed semantic ARM | [`src/mining/`](src/mining/) | ⬜ planned |
| Metrics — temporal significance | [`src/metrics/`](src/metrics/) | ⬜ planned |

## Setup

Requires **Java 11** (`brew install openjdk@11`) and **Python 3.9+**.

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

## Quick start

Ingest the committed sample snapshots into snapshot-partitioned Parquet:

```bash
.venv/bin/python scripts/ingest_samples.py
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
.venv/bin/python -m pytest
```

`tests/test_rdf_parser.py` runs without Spark; `tests/test_ingest.py` spins up a
local SparkSession.

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
