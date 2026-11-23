# Wikidata temporal snapshots: availability audit and data plan

*Audit run 2026-11-21 against the Internet Archive (archive.org advanced-search
and metadata APIs) and dumps.wikimedia.org.*

## Why this matters

The proposal (§1.6, §3.7.1) plans **seven annual Wikidata snapshots, 2017–2023**.
Wikimedia keeps only the latest few weeks of dumps on dumps.wikimedia.org, so
older snapshots must come from the Internet Archive mirror. This audit checks
what survives there.

## What exists

| Snapshot | Internet Archive item | Usable file | Compressed size |
|---|---|---|---|
| 2017-01-01 | `wikidatawiki-20170101` | `pages-articles.xml.bz2` | 11.7 GB |
| 2018-01-01 | `wikidatawiki-20180101` | `pages-articles.xml.bz2` | 34.9 GB |
| 2019-01-01 | `wikidatawiki-20190101` | `pages-articles.xml.bz2` | 61.6 GB |
| 2019-08-26 | `wikidata-20190826-all.json.gz` | entity JSON dump | 59.4 GB |
| 2020-09-20 | `wikidatawiki-20200920` | `pages-articles.xml.bz2` | 106.2 GB |
| 2021-01-20 | `wikidatawiki-20210120` | `pages-articles.xml.bz2` | 109.8 GB |
| 2022-01-20 | `wikidatawiki-20220120` | `pages-articles.xml.bz2` | 128.9 GB |
| 2022-06-30 | `wikidata-20220630-all.json.gz` | entity JSON dump | 115.6 GB |
| **2023** | *none in a standard format* | only a 370 GB Blazegraph query-service journal (`wikidata-query-blazegraph-data-jnl-2023-01-23…`) | n/a |
| 2026-11 (current) | dumps.wikimedia.org | `latest-all.json.gz` | 157.2 GB |

Notes:

- `pages-articles.xml.bz2` stores **every entity's full JSON as page text**, so it
  is a complete snapshot of item content at that date. It needs an XML wrapper
  parse, but carries the same claims and qualifiers as the JSON dumps.
- There are no early-2020 dumps. The nearest are 2019-08-26 (JSON) and
  2020-09-20 (XML).
- **No 2023 snapshot exists in a usable form.** Extracting the Blazegraph
  journal would mean running a Blazegraph server over 370 GB, which is not
  practical on the available hardware.

## Proposed snapshot series (7 snapshots)

**2017-01 · 2018-01 · 2019-01 · 2020-09 · 2021-01 · 2022-01 · 2026-11**

- About **610 GB** of compressed downloads in total.
- Six near-annual snapshots plus the current dump. The gap from 2022 to 2026
  has to be stated as a limitation; the alternative is dropping to six
  snapshots (2017–2022).
- This replaces "annual 2017–2023" in §1.6/§3.7.1 of the research proposal.

## Processing approach (no need to store the raw dumps)

Each dump is **streamed**: download → decompress → parse → filter, in one pass.
Only the filtered output is written to disk.

- For each item, keep:
  - **P31** (instance of) and **P279** (subclass of) statements, which give the
    class items for Algorithm 3.1;
  - the statement **rank**;
  - any **P580 / P582 / P585** (start / end / point in time) qualifiers.
- Output: partitioned Parquet per snapshot, an expected few GB each instead
  of tens to hundreds.
- Where to run:
  - **Oracle Always Free VM (preferred):** fast cloud-side download; 4 ARM
    cores allow parallel bzip2 decompression. Only the small filtered output
    is copied to the laptop.
  - **M5 laptop (fallback):** fine for compute, but 610 GB over a home
    connection is the bottleneck.

## Valid time vs. snapshot time

- **Snapshot (transaction) time** comes from the dump dates above. It suits the
  **class-based items** (P31/P279), which rarely carry date qualifiers.
- **Valid time** comes from the P580/P582/P585 qualifiers. These are dense on
  relational statements (P39 position held, P54 member of sports team, P108
  employer, P463 member of…) but sparse on P31.

The extractor keeps both, so TSARM can window by either. This also covers the
§1.7 limitation on temporal-annotation coverage.

## Decision (2026-11-22)

The 7-snapshot series was adopted, and the research proposal was updated
accordingly (§1.6, §1.7, §3.7.1, Table 3.3 and §3.7.4).

## Download links

| Snapshot | File | Size |
|---|---|---|
| 2017-01 | <https://archive.org/download/wikidatawiki-20170101/wikidatawiki-20170101-pages-articles.xml.bz2> | 11.7 GB |
| 2018-01 | <https://archive.org/download/wikidatawiki-20180101/wikidatawiki-20180101-pages-articles.xml.bz2> | 34.9 GB |
| 2019-01 | <https://archive.org/download/wikidatawiki-20190101/wikidatawiki-20190101-pages-articles.xml.bz2> | 61.6 GB |
| 2020-09 | <https://archive.org/download/wikidatawiki-20200920/wikidatawiki-20200920-pages-articles.xml.bz2> | 106.2 GB |
| 2021-01 | <https://archive.org/download/wikidatawiki-20210120/wikidatawiki-20210120-pages-articles.xml.bz2> | 109.8 GB |
| 2022-01 | <https://archive.org/download/wikidatawiki-20220120/wikidatawiki-20220120-pages-articles.xml.bz2> | 128.9 GB |
| 2026-11 | <https://dumps.wikimedia.org/wikidatawiki/entities/20261116/wikidata-20261116-all.json.gz> | 157.2 GB |

> **Download the 2026-11 dump first.** Wikimedia keeps only the last few weeks
> of dumps, so this file will disappear. The Internet Archive files are
> permanent.

Every Internet Archive item also offers a torrent, which is often faster and
resumes automatically:
`https://archive.org/download/<item>/<item>_archive.torrent`, for example
`https://archive.org/download/wikidatawiki-20170101/wikidatawiki-20170101_archive.torrent`. The torrent covers
*every* file in the item, so in the torrent client select **only** the
`...-pages-articles.xml.bz2` file.

## How to run

`scripts/download_wikidata.sh` does everything for each snapshot. It
downloads with resume (re-run it after a dropped connection), verifies the size,
extracts the class statements into `data/processed/wikidata/<snapshot>.tsv.gz`,
and deletes the raw dump (set `KEEP_RAW=1` to keep it).

**Faster option:** download on a Google Cloud VM instead of the Mac, at
data-centre speed, and mine Wikidata there; see `docs/azure.md` or `docs/gcp.md`, Phase A.

On the Mac, install the parallel decompressors first. Without `lbzip2`
the `.xml.bz2` snapshots are decompressed on a single core, which is many
times slower:

```bash
brew install lbzip2 pigz
```

Then, with the Mac plugged in (`caffeinate -i` stops it sleeping mid-download):

```bash
cd ~/Documents/Projects/TSARM                          # the project folder
caffeinate -i bash scripts/download_wikidata.sh 2026-11  # the expiring one first
caffeinate -i bash scripts/download_wikidata.sh          # then all the rest
```

Raw dumps go to `~/wikidata-raw` (`RAW_DIR=...` to change). Only one raw dump
is on disk at a time, so the peak disk use is about 160 GB. On the Oracle VM,
first install the multi-core decompressors: `sudo apt install lbzip2 pigz`.

## After the download

Mine the extracted snapshots with `scripts/run_wikidata_temporal.py`; start
with a 1% sample (`--sample 0.01`). See `docs/experiments.md` for the protocol
and options.
