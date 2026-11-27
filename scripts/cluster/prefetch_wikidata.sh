#!/usr/bin/env bash
# Download the next Wikidata dump in parallel with download_wikidata.sh, one
# snapshot ahead of it, so that the dump is complete before it is needed.
#
# The file is written under a temporary name and moved into place only when
# complete. A partial file is never handed over: curl decides where to resume
# when it starts but opens its output only when data arrives, so a file moved
# into place while the main loop's curl is starting gets truncated. If the main
# loop reaches a snapshot first, the prefetch stops and discards its copy.
# download_wikidata.sh verifies every file's size; record_provenance.sh checks
# its md5 against archive.org.
#     nohup bash scripts/cluster/prefetch_wikidata.sh &
set -u
RAW=$HOME/wikidata-raw; OUT=$HOME/TSARM/data/processed/wikidata; IA=https://archive.org/download
log() { echo "[$(date -u +%FT%TZ)] $*" >> $HOME/prefetch.log; }
fsize() { if [ -f "$1" ]; then stat -c %s "$1"; else echo 0; fi; }
started() {  # has the main loop started on snapshot $1 (raw file $2)?
  [ -e "$2" ] || [ -e "$OUT/$1.tsv.gz.part" ] || [ -s "$OUT/$1.tsv.gz" ]
}
for job in "2020-09 wikidatawiki-20200920 2021-01 wikidatawiki-20210120" \
           "2021-01 wikidatawiki-20210120 2022-01 wikidatawiki-20220120"; do
  set -- $job; prev=$1; prev_id=$2; snap=$3; id=$4
  name=$id-pages-articles.xml.bz2; url=$IA/$id/$name; real=$RAW/$name; tmp=$RAW/.prefetch-$name
  prev_raw=$RAW/$prev_id-pages-articles.xml.bz2
  started "$snap" "$real" && continue                         # main loop already on it
  until started "$prev" "$prev_raw"; do sleep 60; done        # stay one snapshot ahead
  size=$(curl -sIL "$url" | tr -d '\r' | awk 'tolower($1)=="content-length:"{n=$2} END{print n}')
  [ -n "$size" ] || { log "$snap: size unknown; skipped"; continue; }
  while [ "$(df --output=avail -B1 "$RAW" | tail -1)" -lt $(( size - $(fsize "$tmp") + 15000000000 )) ]; do
    started "$snap" "$real" && continue 2
    log "$snap: waiting for disk space"; sleep 600
  done
  log "$snap: prefetch start ($size bytes, from $(fsize "$tmp"))"
  # One long-lived connection; restarted with -C - only if it stalls or drops.
  while [ "$(fsize "$tmp")" != "$size" ]; do
    curl -sL -C - --speed-limit 100000 --speed-time 300 -o "$tmp" "$url" &
    pid=$!
    while kill -0 "$pid" 2>/dev/null; do
      if started "$snap" "$real"; then
        kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
        log "$snap: main loop reached it first; discarded $(fsize "$tmp") bytes"; rm -f "$tmp"
        continue 3
      fi
      sleep 5
    done
    wait "$pid" 2>/dev/null || sleep 15
  done
  if started "$snap" "$real"; then rm -f "$tmp"; log "$snap: complete, but main loop got there first; discarded"
  else mv "$tmp" "$real"; log "$snap: complete ($size bytes) and in place"; fi
done
log "prefetch finished"
