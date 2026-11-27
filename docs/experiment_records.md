# Experiment records

Where the evidence for every reported result is kept. Result data live under
`results/` (git-ignored); this index is the map. Every result JSON also embeds
its parameters, stage timings and a hardware fingerprint (CPU, memory, Python,
Java, Spark versions).

## E1: core scalability (development machine)

`results/core_scaling_*` (per-run CSV, summary CSV, JSON with hardware), logs next to them.

## E2: multi-node scalability (8 cloud instances, eu-north-1)

`results/tsarm-aws-results.tgz` (as fetched from the master node) and the
unpacked `results/cluster_scaling_{strong,weak}_aws{.json,_runs.csv,_summary.csv}`;
node log `results/cluster_scaling_aws_e2-node.log`. Every run records the number
of executors granted (0 mismatches in 30 runs). Gap: no instance-metadata
document was saved from the cloud provider; the node log and the hardware
fingerprint identify the instances.

## E3–E5 on synthetic data, DBpedia

`results/benchmark_*`, `results/quality_*`, `results/dbpedia_*`,
`results/dbpedia_sample5/` (three repeats, `timings.txt`),
`results/dbpedia_amie/OUTCOME.txt` (AMIE on the full releases: heap exhausted).

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

## Wikidata baseline comparison (development machine)

`results/wikidata_baselines/`: `run_e5_wikidata.sh` (exact procedure),
`e5_environment.txt`, `e5_progress.log`, `e5_s0001.log`, `e5_s001.log`,
`benchmark_wikidata_s0001{,_runs,_stats}.csv`, `benchmark_wikidata_s001{,_runs}.csv`,
inputs `in_s0001/`, `in_s001/` (with `sample.json`), the baselines' kept rule
files `amie_s0001/`, `rdfrules_s0001/` and TSARM's `tsarm_s0001_check.*`,
`tsarm_s001_check.*`.

## Annotation and events (pending manual work)

`results/annotation/wikidata_full_sheet.csv` (299 rules) and
`wikidata_s0001_sheet.csv` (3 rules), each with `.meta.json`;
`results/events/wikidata_candidates_{full,s010}{,_review}.csv`.
