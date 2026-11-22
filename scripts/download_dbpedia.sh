#!/usr/bin/env bash
# Download the DBpedia annual release series (English instance types, direct
# types only) plus the DBpedia ontology, for the DBpedia temporal evaluation.
#
#     bash scripts/download_dbpedia.sh             # all releases
#     bash scripts/download_dbpedia.sh 2019-09     # just these
#
# Releases come from the DBpedia Databus (https://databus.dbpedia.org,
# artifact dbpedia/mappings/instance-types). The Databus redirects every file to
# downloads.dbpedia.org; when that server is unreachable, the script falls back
# to the Internet Archive's Wayback Machine copy where one exists (2016-10 to
# 2019-09 and the ontology; slow, about 10-50 KB/s). The 2020-2022 releases
# exist only on downloads.dbpedia.org.
#
# There is no 2017 release on the Databus. Together with the 2015-10 and 2016-04
# releases (old download area, see README) the series is
#   2015-10, 2016-04, 2016-10, 2018-12, 2019-09, 2020-10, 2021-12, 2022-12.
#
# Files go to data/raw/dbpedia_<release>_instance_types_en.ttl.bz2, the names
# scripts/run_dbpedia_temporal.py looks for. Downloads resume after a dropped
# connection: just run the script again.
set -uo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
RAW="$REPO/data/raw"
DBUS="https://databus.dbpedia.org/dbpedia/mappings/instance-types"
WB="http://web.archive.org/web"
LTS="https://downloads.dbpedia.org/repo/lts/mappings/instance-types"

# release | Databus file | Wayback timestamp (empty: no archived copy) | size in bytes
RELEASES=(
  "2016-10|2016.10.01/instance-types_lang=en.ttl.bz2|20250120210952|32186217"
  "2018-12|2018.12.01/instance-types_lang=en.ttl.bz2|20250120212854|36464076"
  "2019-09|2019.09.01/instance-types_lang=en.ttl.bz2|20250120212016|36978303"
  "2020-10|2020.10.01/instance-types_inference=specific_lang=en.ttl.bz2||41255530"
  "2021-12|2021.12.01/instance-types_inference=specific_lang=en.ttl.bz2||44905249"
  "2022-12|2022.12.01/instance-types_inference=specific_lang=en.ttl.bz2||46387756"
)
ONTOLOGY_URL="https://downloads.dbpedia.org/2016-10/dbpedia_2016-10.owl"
ONTOLOGY_WB="$WB/20250906231637id_/$ONTOLOGY_URL"

mkdir -p "$RAW"
WANTED=" $* "   # bash 3.2 (macOS) safe under set -u

size_of() { wc -c <"$1" | tr -d ' '; }

fetch() {  # fetch <out> <size or empty> <url>...
  local out="$1" size="$2"; shift 2
  for url in "$@"; do
    [ -n "$url" ] || continue
    echo "   trying $url"
    curl -fL -# -C - --retry 5 --retry-delay 10 --connect-timeout 20 -o "$out.part" "$url" && {
      if [ -z "$size" ] || [ "$(size_of "$out.part")" = "$size" ]; then
        mv "$out.part" "$out"; echo "   done: $out"; return 0
      fi
      echo "   size mismatch ($(size_of "$out.part") != $size); trying the next source"
      rm -f "$out.part"
    }
  done
  echo "   FAILED: no source reachable for $(basename "$out") (partial download kept; re-run later)"
  return 1
}

failed=0
for entry in "${RELEASES[@]}"; do
  IFS="|" read -r rel file wb size <<<"$entry"
  if [ -n "${*:-}" ] && [[ "$WANTED" != *" $rel "* ]]; then continue; fi
  out="$RAW/dbpedia_${rel}_instance_types_en.ttl.bz2"
  if [ -s "$out" ]; then echo "== $rel: already downloaded"; continue; fi
  echo "== $rel ($((size / 1000000)) MB)"
  enc_file="${file//=/%3D}"
  fetch "$out" "$size" "$DBUS/$file" "$LTS/$file" ${wb:+"$WB/${wb}id_/$LTS/$enc_file"} || failed=1
done

out="$RAW/dbpedia_ontology_2016-10.owl"
if [ -s "$out" ]; then echo "== ontology: already downloaded"
else echo "== DBpedia ontology (2016-10)"; fetch "$out" "" "$ONTOLOGY_URL" "$ONTOLOGY_WB" || failed=1
fi
exit $failed
