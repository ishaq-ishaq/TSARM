# Experiment records

Where the evidence for every reported result is kept. Result data live under
`results/` (git-ignored), one folder per experiment or dataset; this index is the
map. `results/REORGANISATION_LOG.txt` lists where each file was before the
folders were introduced (27 Nov 2026). Scripts still write new output to
`results/` itself. Every result JSON also embeds
its parameters, stage timings and a hardware fingerprint (CPU, memory, Python,
Java, Spark versions).

## E1: core scalability (development machine)

`results/E1_core_scaling/`: per-run CSV, summary CSV and JSON (with hardware) of each sweep, logs next to them.

## E2: multi-node scalability (8 cloud instances, eu-north-1)

`results/E2_cluster_scaling/`: `tsarm-aws-results.tgz` (as fetched from the
master node) and the unpacked `cluster_scaling_{strong,weak}_aws{.json,_runs.csv,_summary.csv}`;
node log `cluster_scaling_aws_e2-node.log`; the local standalone-cluster checks in
`local_cluster_checks/`. Every run records the number
of executors granted (0 mismatches in 30 runs). Gap: no instance-metadata
document was saved from the cloud provider; the node log and the hardware
fingerprint identify the instances.

## E3–E5 on synthetic data, DBpedia

Synthetic data and the bundled samples: `results/synthetic_and_samples/`
(`benchmark_*`, `quality_*`, `scale_test.csv`, `sample_temporal_rules.*`).
DBpedia: `results/dbpedia/` (`dbpedia_*` rules and logs, trajectory clusters
`clusters_dbpedia_*`, `dbpedia_sample5/` with three repeats and `timings.txt`,
`dbpedia_amie/OUTCOME.txt`: AMIE on the full releases, heap exhausted).

## Wikidata pilot on generated data

`results/wikidata_rehearsal/` (`wikidata_rehearsal.*`, trajectory clusters `clusters_rehearsal_*`).

## Wikidata (virtual machine, 4 vCPU / 28 GB, South Africa North)

All in `results/wikidata_azure/`, fetched from the VM and checked against the
VM's own checksums:

| File | Content | MD5 |
|---|---|---|
| `wikidata-results-run2.tgz` | final results: rules (1 %, 10 %, 10 % h=2, full), event candidates, sample exports, all step logs (incl. failed first attempts) | c846a7801b626ca60c5012be8e75ba54 |
| `wikidata-results-run1.tgz` | results as packed after the first chain run | e5ae4529e6e8514ad83624366dd9cd19 |
| `provenance-run2.tgz` | final provenance (below) | 08a76961737b4d378029a4040b024d86 |
| `provenance-run1.tgz` | provenance after the first chain run | 3874e360c2e56fc6c6ce6b10007e9e32 |
| `validation-records.tgz` | re-runs of the 1 % and 10 % samples with the anchored recount, comparison script and output (IDENTICAL) | 5364c78f1fdb4dc28582d792c315de59 |
| `vm-logs-final.tgz` | experiment, resource, download and extraction logs | e752309968f02bc6f35caf391e0d67b2 |
| `snapshots/*.tsv.gz` | the seven extracted snapshots (2.0 GB) | SHA-256 in `provenance/extracted.sha256` (all match) |

The provenance archive holds: the cloud provider's instance metadata (VM id,
size, region, image), `system.txt` (hardware, OS, Python, Java), `expected.tsv`
(the archive's published size and MD5 of every dump), `raw.md5` (MD5 of the
2021-01 and 2022-01 dumps on disk: MATCH), `extracted.sha256`, the git commit
and uncommitted diff of both checkouts, the crontab, copies of every log and
operational script, both code patches applied on the VM (`recount.patch`,
`dates.patch`) and `code-changes.log`, which states which run used which code.

Recorded limitations, stated rather than omitted:

* The raw dumps for 2017-01, 2018-01, 2019-01, 2020-09 and 2026-11 were deleted
  after extraction before checksums were recorded; for them only the size
  check of the download and the extraction logs exist.
* `pip-freeze.txt` lists the system Python, not the virtual environment the runs
  used; the Spark and Java versions of each run are in the result JSONs.
* The 1 % and 10 % (h = 1) sample runs used the first recount formulation;
  the later runs used the anchored recount. Both give identical output
  (`validation-records.tgz`).
* First chain run: three steps lost their JVM to the kernel's out-of-memory
  killer because a validation job ran alongside, and the full-data step failed
  on an impossible date (1973-04-31). All four were re-run (run 2); the failed
  logs are kept as `*.run1-failed.log`.

The VM was deallocated after the records were fetched; its disk is kept.

## Wikidata analysis (development machine)

`results/wikidata_analysis/`: trajectory clustering of the full-data rules
(`clusters_wikidata_full{,_shape}.{json,log}`) and the labels of the example
rules (`wikidata_full_example_labels.json`).

## Wikidata baseline comparison (development machine)

`results/wikidata_baselines/`: `run_e5_wikidata.sh` (exact procedure),
`e5_environment.txt`, `e5_progress.log`, `e5_s0001.log`, `e5_s001.log`,
`benchmark_wikidata_s0001{,_runs,_stats}.csv`, `benchmark_wikidata_s001{,_runs}.csv`,
inputs `in_s0001/`, `in_s001/` (with `sample.json`), the baselines' kept rule
files `amie_s0001/`, `rdfrules_s0001/` and TSARM's `tsarm_s0001_check.*`,
`tsarm_s001_check.*`.

## Rule judgements (E3) and reference events (E4) on Wikidata

Rule judgements, `results/annotation/`:

* `wikidata_full_sheet.csv` (299 of the 424 full-data rules) and
  `wikidata_s0001_sheet.csv` (the 3 comparable rules of the baseline
  comparison), each with `.meta.json`; filled by the researcher on 27 Nov 2026
  and kept exactly as saved. `RECORDS_NOTE.txt` records provenance notes on
  the filled sheets and a known gap in the sheet's labels column.
* Scores: `wikidata_full_sheet.score.json` (precision 0.438, Wilson 95 %
  0.383–0.495) and `wikidata_s0001_sheet.score.json`.
* `wikidata_full_retest_sheet.csv`: the blind re-test (75 rules, seed 2026),
  to be filled at least seven days after the first pass; then
  `annotation_kit.py score wikidata_full_sheet.csv --retest wikidata_full_retest_sheet.csv`.

Reference events, `results/events/`:

* `wikidata_candidates_{full,s010}.csv` (as proposed) and
  `..._review.csv` (as confirmed: `confirmed`, `source` = Wikidata revision,
  `note`); 93 of 139 and 14 of 19 confirmed.
* `event_rules_measured_full_w1.json`: `scripts/measure_event_rules.py`
  on the full data, one window per snapshot (per-window counts and confidence
  of every event rule, TS per sheet, timings, hardware); log and environment in
  `logs/measure_full_w1.log`, `logs/measure_full_env.txt` (83 min).
* `event_rules_measured_check_s001.json` + `logs/measure_check_s001.log`: the
  same procedure on the 1 % sample with two-snapshot windows reproduces the
  confidences of the mining run for 40 rules exactly (largest difference 0).
* `event_ts_summary.json`: TS 0.720 (67/93) full, 0.714 (10/14) 10 %; for each
  missed event, whether the consequent was a direct superclass of the
  antecedent in both snapshots (18 of 26), from
  `missed_event_classes_p279_edges.tsv` (best-rank P279 statements of the
  missed classes in every snapshot).
* `wikidata_full_ts.json`: scoring on the mining run itself (`propose_events.py score`) gives 0/93: 58
  events are at the first or last transition, and the others' rules are below
  the mining support threshold.
