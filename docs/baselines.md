# Baseline comparison: feasibility notes (objective iv)

TSARM is evaluated against two systems named in the research proposal: **SANSA**
(Lehmann et al., 2017) and **RDFRules** (Zeman, Kliegr & Svatek, 2021). The
comparison harness ([`src/evaluation/`](../src/evaluation/)) provides a common
adapter interface; this note records the integration status and feasibility of
each baseline, which differs substantially.

## RDFRules — integrated and run ✅

RDFRules is actively maintained, ships a runnable release (a single 60 MB zip
with `bin/main`, `lib/`, `webapp/`), supports **batch processing** of JSON task
pipelines, and runs on Java 17.

Integration: [`scripts/rdfrules_mine.py`](../scripts/rdfrules_mine.py) builds the
pipeline `LoadGraph → Index → Mine → ComputeConfidence → ExportRules`, runs
`sh bin/main task.json result.json`, and writes a `rules.csv` for the harness.

Practical gotchas handled:

- **Workspace-relative paths.** RDFRules resolves every `path` against its
  workspace directory. The wrapper sets `RDFRULES_WORKSPACE` to a temp dir,
  symlinks the inputs in, and uses relative names.
- **Measure names.** Exported rules carry a `measures` *list*; confidence is
  `CwaConfidence` and the relative support proxy is `HeadCoverage` (RDFRules'
  `Support` is an absolute count).

Result (16 k-triple synthetic dataset, RDFRules 1.9.0, Java 17):

| System | Runtime | #Rules | Mean conf | Temporal metrics |
| --- | --- | --- | --- | --- |
| TSARM | 57.9 s | 4 | 0.81 | yes (persistence, drift) |
| RDFRules (AMIE+) | 82.3 s | 22 777 | 0.50 | no (snapshot-only) |

AMIE+ exhaustively enumerates logical Horn rules (many, lower mean confidence);
TSARM mines few focused transactional rules with temporal significance. Rule
**counts** are therefore not directly comparable, but **runtime** and **temporal
capability** are meaningful comparison axes.

## SANSA — adapter ready, baseline impractical ⚠️

SANSA's association-rule-mining capability is **AmieSpark**
(`net.sansa_stack.ml.spark.mining.amieSpark.MineRules`), a distributed AMIE
implementation. Investigation findings:

- AmieSpark exists as **source only in SANSA v0.7.1**
  (`sansa-ml-spark/.../mining/amieSpark/MineRules.scala`), the v0.7.x line
  contemporaneous with Lehmann et al. (2017). It targets **Scala 2.11 /
  Spark 2.x**.
- It has been **removed from current SANSA** (0.8.x–0.9.x). In the present
  source tree only the *generated scaladoc HTML* under
  `docs/scaladocs/0.7.1_ICSC_paper/` remains; there is no `amieSpark` source and
  no mining `.scala` files.
- Running it therefore requires the **legacy v0.7.1 toolchain**: Scala 2.11,
  Spark 2.4, plus an SBT/Maven build of a custom job — a separate runtime that
  is **incompatible with TSARM's Spark 4.0 / Scala 2.13 environment** and cannot
  share it.

**Decision:** a SANSA AmieSpark run is not pursued. The cost (assembling an
unmaintained Spark 2.x / Scala 2.11 stack and a Scala build) is disproportionate
to the value, since RDFRules already provides a working, modern AMIE-family
baseline. The harness keeps a ready SANSA adapter slot (`$SANSA_CMD`); if a
legacy SANSA 0.7.1 job is assembled later, wiring it in is a one-line
configuration.

For the thesis, the SANSA comparison is best framed **architecturally** (SANSA
as a distributed RDF-processing stack on Spark, whose ARM module is legacy and
unmaintained) rather than as a like-for-like quantitative rule-mining benchmark,
with **RDFRules as the practical quantitative AMIE baseline**.
