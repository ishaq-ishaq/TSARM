# TSARM experiments: how to reproduce every result

Every experiment is one script with fixed parameters and a fixed random seed.
Each writes its raw per-run measurements, a summary, and the hardware it ran
on to `results/`, so results from different machines are never mixed.

Common setup (macOS shown; on Linux use the system Java 17 path):

```bash
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"   # Spark 4 needs Java 17+
python3 -m pip install -r requirements.txt
python3 -m pytest                                 # sanity check (all tests pass)
```

Baseline binaries (not versioned, downloaded into `tools/`):

```bash
# AMIE 3.5.1 (AMIE+ lineage)
mkdir -p tools/amie && curl -L -o tools/amie/amie3.5.1.jar \
  https://github.com/dig-team/AMIE/releases/download/v3.5.1/amie3.5.1.jar
# RDFRules 1.9.0
mkdir -p tools/rdfrules && curl -L -o tools/rdfrules/rdfrules-1.9.0.zip \
  https://github.com/propi/rdfrules/releases/download/1.9.0/rdfrules-1.9.0.zip \
  && (cd tools/rdfrules && unzip -q rdfrules-1.9.0.zip)

export RDFRULES_HOME=$PWD/tools/rdfrules AMIE_JAR=$PWD/tools/amie/amie3.5.1.jar
export RDFRULES_CMD="python3 $PWD/scripts/rdfrules_mine.py --input {input} --output {output} --min-support {min_support} --min-confidence {min_confidence}"
export AMIE_CMD="python3 $PWD/scripts/amie_mine.py --input {input} --output {output} --min-support {min_support} --min-confidence {min_confidence}"
```

On a laptop, run long experiments under `caffeinate -i` (macOS) so the
machine does not sleep mid-measurement. The baseline harness also enforces
its time limit on the wall clock.

---

## E1: Scalability across CPU cores (one machine)

*Proposal §3.7.3 i (strong and weak scaling).*

```bash
python3 scripts/core_scaling.py --mode strong --cores 1,2,4,8,15 --entities 1500000   # 12M triples
python3 scripts/core_scaling.py --mode weak   --cores 1,2,4,8,15 --entities 150000    # 150k entities per core
```

- **Protocol:** Spark `local[p]`; one discarded warm-up run per `p`, then 3
  measured runs; shuffle partitions fixed at 32, so every run does the same work.
- **Outputs:** `results/core_scaling_{strong,weak}_{runs,summary}.csv` and `.json`
  (summary includes mean, SD, 95% CI, speedup, efficiency and Karp–Flatt
  serial fraction).
- **Status:** done on the Apple M5 Pro (see README).

## E2: Scalability across cluster nodes

*Proposal §3.7.3 i, multi-node. Oracle Cloud Always Free, 4 × (1 OCPU, 6 GB).*

Follow `docs/oracle_cluster.md` Phase B. Command on the master:

```bash
TSARM_DRIVER_MEMORY=1g python3 scripts/core_scaling.py --master-url spark://<master>:7077 \
    --data-dir ~/tsarm-data --mode strong --cores 1,2,3,4 --entities 300000 --executor-memory 3g
```

- **Outputs:** `results/cluster_scaling_strong_*`; runs record `executors_granted`.
- **Status:** code validated on a local standalone cluster; waiting for the
  Oracle account.

## E3: Rule quality against a reference set

*Proposal §3.7.3 ii: precision, recall and F1.*

```bash
python3 scripts/evaluate_quality.py --entities 20000        # synthetic, planted ground truth
```

- **Protocol:** TSARM, AMIE+ and RDFRules on the same data, with the baselines
  on the pooled graph. Baseline rules are compared only when expressible as
  TSARM items (single variable, constant objects); the rest are reported as
  not comparable.
- **For Wikidata:** supply a hand-validated reference CSV
  (`antecedent,consequent`, items separated by `;`) to
  `src.evaluation.quality.load_reference_rules`.
- **Outputs:** `results/quality_<dataset>.json`.
- **Status:** done on synthetic data (160k triples): TSARM P = 0.50, R = 1.00;
  AMIE+ and RDFRules P = 0.05, R = 1.00 on their comparable rules (see README).

## E4: Temporal sensitivity

*Proposal §3.7.3 iii: TS = |E_correct| / |E|.*

Same script as E3. On synthetic data the reference event is the planted
transient rule's collapse. For Wikidata, build a list of dated structural
changes (`src.evaluation.quality.Event`).

- **Detection rule:** confidence moves in the expected direction by more than
  ε = 0.05 between the windows bracketing the event date.
- **Status:** synthetic TS = 1.0 (confidence 1.00 → 0.20 across the event).
  The Wikidata event list still needs manual curation.

## E5: Baseline runtime comparison with statistics

*Proposal §3.7.2 and Table 3.4.*

```bash
caffeinate -i python3 scripts/run_benchmark.py --synthetic 2000   --repeats 3   # 16k triples
caffeinate -i python3 scripts/run_benchmark.py --synthetic 20000  --repeats 3   # 160k
caffeinate -i python3 scripts/run_benchmark.py --synthetic 200000 --repeats 3 --timeout 3600   # 1.6M
```

- **Protocol:** one discarded warm-up round, then 3 paired rounds. Per-system
  timeout on the wall clock; a system that times out is recorded as `timeout`
  and not re-run. TSARM is compared with each baseline by Shapiro–Wilk on the
  paired differences, then a paired t-test with Cohen's d or a Wilcoxon
  signed-rank test with rank-biserial r.
- **Outputs:** `results/benchmark_synthetic[_n<N>]{,_runs,_stats}.csv`.
- **Status:** done at all three sizes on the M5. At 1.6M triples both
  baselines time out (> 1 h), so no `_stats.csv` is written (no paired data).

## Real-data runs (DBpedia)

```bash
python3 scripts/run_dbpedia.py            # 2 releases, 10.2M triples
python3 scripts/run_dbpedia_temporal.py   # 3 releases, 2 windows, 15.4M triples
```

The dumps go in `data/raw/` (download commands in the README).

## Wikidata (primary dataset)

```bash
bash scripts/download_wikidata.sh 2026-11   # expiring dump first
bash scripts/download_wikidata.sh           # remaining six snapshots
```

See `docs/wikidata_data_plan.md`. Output:
`data/processed/wikidata/<snapshot>.tsv.gz`. Then mine it, starting with a
1% entity sample to check memory and runtime:

```bash
python3 scripts/run_wikidata_temporal.py --sample 0.01 --tag s001
python3 scripts/run_wikidata_temporal.py --sample 0.1  --tag s010
python3 scripts/run_wikidata_temporal.py               --tag full
# with only some snapshots downloaded so far:
python3 scripts/run_wikidata_temporal.py --snapshots 2022-01,2026-11 --width 1 --sample 0.01
```

- **Protocol:** best-rank ("truthy") P31/P279 statements; class items with
  P279 ancestors up to `--superclass-depth` hops (default 1; Wikidata's
  hierarchy is too deep for the full closure, see below); windows of 2
  consecutive snapshots, step 1; σ = 0.01, minimum confidence 0.50, τ = 0.70.
  The sample is a hash of the entity IRI, so the same entities are kept in
  every snapshot; P279 edges are never sampled away.
- **Why limit the depth:** an ancestor always co-occurs with its subclass, so
  with *k* ancestors every one of the 2^k subsets is a frequent itemset.
  Taxonomic rules are pruned after mining, so this cost cannot be avoided
  later. Try `--superclass-depth 2` on the 1% sample before using it at full size.
- **Options:** `--valid-at-snapshot` drops statements whose P580/P582 dates
  exclude the snapshot date; `--all-ranks` keeps normal-rank statements that
  are not best-rank.
- **Outputs:** `results/wikidata_temporal_rules_<tag>.{json,csv}` with
  parameters, statement counts, stage timings and the hardware fingerprint.

## Fixed parameters

| Parameter | Value | Source |
|---|---|---|
| τ (RPS confidence threshold) | 0.70 | proposal §3.5.5 |
| ε (drift stability band) | 0.05 | Table 3.2 |
| RPS boundaries | 0.20 / 0.50 | Table 3.2 (as amended) |
| Repetitions | 3 (+1 warm-up) | §3.7.4 |
| Synthetic seed | 42 | `generate_dataset` |
| Synthetic snapshots / transient cut | 4 / 2 | E1–E5 |
| Wikidata/DBpedia mining thresholds | σ = 0.01, min confidence 0.50 | §3.7.4 |
