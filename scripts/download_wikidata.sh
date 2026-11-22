#!/usr/bin/env bash
# Download the seven Wikidata snapshots (docs/wikidata_data_plan.md) and extract
# the class statements TSARM needs (P31/P279 + rank + temporal qualifiers).
#
# Run it yourself, on the Oracle VM (preferred: fast cloud-side download) or on
# the Mac:
#
#     bash scripts/download_wikidata.sh                 # all seven, oldest first
#     bash scripts/download_wikidata.sh 2017-01 2026-11 # just these
#
# For each snapshot it:
#   1. downloads the raw dump into $RAW_DIR with resume (re-run after a dropped
#      connection and it continues where it stopped),
#   2. checks the file size against the published size,
#   3. streams it through scripts/extract_wikidata.py into
#      $OUT_DIR/<snapshot>.tsv.gz (a few GB instead of tens to hundreds),
#   4. deletes the raw dump unless KEEP_RAW=1.
#
# Total raw download is about 610 GB, but with the default deletion only one raw
# dump is on disk at a time (largest: 157 GB).
# Optional speed-ups: `sudo apt install lbzip2 pigz` (Linux) or
# `brew install lbzip2 pigz` (Mac) for multi-core decompression.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
RAW_DIR="${RAW_DIR:-$HOME/wikidata-raw}"
OUT_DIR="${OUT_DIR:-$REPO/data/processed/wikidata}"
WORKERS="${WORKERS:-$( (nproc 2>/dev/null || sysctl -n hw.ncpu) )}"
KEEP_RAW="${KEEP_RAW:-0}"
PYTHON="${PYTHON:-python3}"

IA="https://archive.org/download"
# snapshot | URL | size in bytes (from archive.org / dumps.wikimedia.org metadata)
SNAPSHOTS=(
  "2017-01|$IA/wikidatawiki-20170101/wikidatawiki-20170101-pages-articles.xml.bz2|"
  "2018-01|$IA/wikidatawiki-20180101/wikidatawiki-20180101-pages-articles.xml.bz2|"
  "2019-01|$IA/wikidatawiki-20190101/wikidatawiki-20190101-pages-articles.xml.bz2|"
  "2020-09|$IA/wikidatawiki-20200920/wikidatawiki-20200920-pages-articles.xml.bz2|"
  "2021-01|$IA/wikidatawiki-20210120/wikidatawiki-20210120-pages-articles.xml.bz2|"
  "2022-01|$IA/wikidatawiki-20220120/wikidatawiki-20220120-pages-articles.xml.bz2|"
  # Wikimedia deletes this after a few weeks: download it first if time is short.
  "2026-11|https://dumps.wikimedia.org/wikidatawiki/entities/20261116/wikidata-20261116-all.json.gz|157184992349"
)

mkdir -p "$RAW_DIR" "$OUT_DIR"
WANTED=" $* "   # plain string: works with macOS bash 3.2 under set -u

remote_size() {  # follow redirects, print Content-Length of the final response
  curl -sIL "$1" | tr -d '\r' | awk 'tolower($1)=="content-length:"{n=$2} END{print n}'
}

for entry in "${SNAPSHOTS[@]}"; do
  IFS="|" read -r snap url size <<<"$entry"
  if [ -n "${*:-}" ] && [[ "$WANTED" != *" $snap "* ]]; then continue; fi
  out="$OUT_DIR/$snap.tsv.gz"
  if [ -s "$out" ]; then echo "== $snap: already extracted ($out), skipping"; continue; fi

  file="$RAW_DIR/$(basename "$url")"
  [ -n "$size" ] || size="$(remote_size "$url")"
  echo "== $snap: downloading $(basename "$url") ($((size / 1000000000)) GB) -> $file"
  until [ -f "$file" ] && [ "$(wc -c <"$file" | tr -d ' ')" = "$size" ]; do
    curl -L -C - --retry 20 --retry-delay 15 --retry-all-errors -o "$file" "$url" || {
      echo "   connection dropped; resuming in 30 s ..."; sleep 30; }
  done
  echo "   download complete and size verified"

  echo "== $snap: extracting class statements -> $out"
  case "$file" in
    *.xml.bz2)
      if command -v lbzip2 >/dev/null; then
        lbzip2 -dc "$file" | "$PYTHON" "$REPO/scripts/extract_wikidata.py" --input - --format xml --out "$out.part" --workers "$WORKERS"
      else
        echo "   WARNING: lbzip2 not found, decompressing on one core (slow). Install it: brew install lbzip2 (Mac) / sudo apt install lbzip2"
        "$PYTHON" "$REPO/scripts/extract_wikidata.py" --input "$file" --format xml --out "$out.part" --workers "$WORKERS"
      fi ;;
    *.json.gz)
      if command -v pigz >/dev/null; then
        pigz -dc "$file" | "$PYTHON" "$REPO/scripts/extract_wikidata.py" --input - --format json --out "$out.part" --workers "$WORKERS"
      else
        "$PYTHON" "$REPO/scripts/extract_wikidata.py" --input "$file" --format json --out "$out.part" --workers "$WORKERS"
      fi ;;
  esac
  mv "$out.part" "$out"   # only a completed extraction gets the final name

  if [ "$KEEP_RAW" != "1" ]; then rm -f "$file"; echo "   raw dump deleted (KEEP_RAW=1 to keep)"; fi
done
echo "Done. Extracted snapshots in $OUT_DIR"
