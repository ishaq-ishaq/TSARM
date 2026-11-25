#!/usr/bin/env bash
# Keep a provenance record of the Wikidata data preparation and experiments on
# the data VM, in ~/provenance (packed into ~/provenance.tgz at the end):
#   system.txt        hardware, OS, Python/Java versions (refreshed each pass)
#   azure-instance.json  Azure instance metadata: VM id, size, region, image
#   pip-freeze.txt    Python packages
#   git-<repo>.txt    commit, status and uncommitted diff of each checkout
#   crontab.txt       the @reboot entries that drive the run
#   *.log, *.sh       copies of the run logs and the operational scripts
#   expected.tsv      archive.org's published size and md5 of every dump
#   raw.md5           md5 of each raw dump on disk, checked against expected.tsv
#                     (computed once, before download_wikidata.sh deletes it)
#   extracted.sha256  sha256 of the extracted class-statement files
# Runs every 10 minutes until ~/experiments.log reports EXPERIMENTS-DONE.
#     nohup bash scripts/cluster/record_provenance.sh &
set -u
P=$HOME/provenance; RAW=$HOME/wikidata-raw; OUT=$HOME/TSARM/data/processed/wikidata
ITEMS="wikidatawiki-20170101 wikidatawiki-20180101 wikidatawiki-20190101 wikidatawiki-20200920 wikidatawiki-20210120 wikidatawiki-20220120"
mkdir -p "$P"

expected() {  # name<TAB>size<TAB>md5 for the pages-articles dump of each archive.org item
  for id in $ITEMS; do
    awk -F'\t' -v n="$id-pages-articles.xml.bz2" '$1==n{f=1} END{exit !f}' "$P/expected.tsv" 2>/dev/null && continue
    curl -sL "https://archive.org/download/$id/${id}_files.xml" | python3 -c '
import sys, xml.etree.ElementTree as ET
for f in ET.parse(sys.stdin).getroot().iter("file"):
    n = f.get("name", "")
    if n.endswith("-pages-articles.xml.bz2"):
        print(n, f.findtext("size", ""), f.findtext("md5", ""), sep="\t")
' >> "$P/expected.tsv" 2>/dev/null
  done
}

rawsums() {  # md5 each complete raw dump once, at idle priority
  for f in "$RAW"/*.bz2 "$RAW"/*.gz; do
    [ -f "$f" ] || continue
    n=$(basename "$f")
    awk -F'\t' -v n="$n" '$2==n{f=1} END{exit !f}' "$P/raw.md5" 2>/dev/null && continue
    exp=$(awk -F'\t' -v n="$n" '$1==n' "$P/expected.tsv" 2>/dev/null)
    esize=$(echo "$exp" | cut -f2); emd5=$(echo "$exp" | cut -f3)
    [ -n "$esize" ] && [ "$(stat -c %s "$f")" != "$esize" ] && continue  # still downloading
    md5=$(nice -n 19 ionice -c3 md5sum "$f" | cut -d' ' -f1) || continue
    [ -f "$f" ] || continue  # deleted while hashing: try nothing more for it
    v=unverified; [ -n "$emd5" ] && { [ "$md5" = "$emd5" ] && v=MATCH || v=MISMATCH; }
    printf '%s\t%s\t%s\t%s\t%s\n' "$(date -u +%FT%TZ)" "$n" "$(stat -c %s "$f")" "$md5" "$v" >> "$P/raw.md5"
  done
}

snapshot() {
  { date -u; uname -a; lscpu | sed -n '1,20p'; free -g; df -h /; python3 --version; java -version 2>&1; } > "$P/system.txt"
  # Azure's own record of this machine (VM id, size, region, image), as evidence of where the runs took place.
  [ -s "$P/azure-instance.json" ] || curl -s -H Metadata:true --max-time 5 \
    "http://169.254.169.254/metadata/instance?api-version=2021-02-01" > "$P/azure-instance.json"
  python3 -m pip freeze > "$P/pip-freeze.txt" 2>/dev/null
  for r in TSARM TSARM-exp; do
    [ -d "$HOME/$r" ] && (cd "$HOME/$r" && { git rev-parse HEAD; git status --short; git diff; }) > "$P/git-$r.txt" 2>&1
  done
  crontab -l > "$P/crontab.txt" 2>&1
  cp "$HOME"/*.log "$HOME"/*.sh "$HOME"/*.patch "$P/" 2>/dev/null
  (cd "$OUT" && sha256sum ./*.tsv.gz) > "$P/extracted.sha256" 2>/dev/null
}

while :; do
  expected; rawsums; snapshot
  if grep -q EXPERIMENTS-DONE "$HOME/experiments.log" 2>/dev/null; then
    tar czf "$HOME/provenance.tgz" -C "$HOME" provenance
    echo "[$(date -u +%FT%TZ)] packed ~/provenance.tgz" >> "$P/recorder.log"
    exit 0
  fi
  sleep 600
done
