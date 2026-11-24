#!/usr/bin/env bash
# Download the next Wikidata dump while the current one is being extracted.
# Writes to a temporary name and moves the file into place only when complete;
# download_wikidata.sh still verifies every file's size and resumes if short.
# Run beside download_wikidata.sh on the data VM: nohup bash scripts/cluster/prefetch_wikidata.sh &
set -u
RAW=$HOME/wikidata-raw; OUT=$HOME/TSARM/data/processed/wikidata; IA=https://archive.org/download
log() { echo "[$(date -u +%FT%TZ)] $*" >> $HOME/prefetch.log; }
fsize() { if [ -f "$1" ]; then stat -c %s "$1"; else echo 0; fi; }
for job in "2019-01 2020-09 wikidatawiki-20200920" "2020-09 2021-01 wikidatawiki-20210120" "2021-01 2022-01 wikidatawiki-20220120"; do
  set -- $job; prev=$1; snap=$2; id=$3
  name=$id-pages-articles.xml.bz2; url=$IA/$id/$name; real=$RAW/$name; tmp=$RAW/.prefetch-$name
  [ -s "$OUT/$snap.tsv.gz" ] && continue
  until [ -e "$OUT/$prev.tsv.gz.part" ] || [ -s "$OUT/$prev.tsv.gz" ]; do sleep 60; done
  [ -e "$real" ] && { log "$snap: main loop already has it; skipped"; continue; }
  size=$(curl -sIL "$url" | tr -d '\r' | awk 'tolower($1)=="content-length:"{n=$2} END{print n}')
  [ -n "$size" ] || { log "$snap: size unknown; skipped"; continue; }
  while [ "$(df --output=avail -B1 "$RAW" | tail -1)" -lt $(( size - $(fsize "$tmp") + 15000000000 )) ]; do
    log "$snap: waiting for disk space"; sleep 600
  done
  log "$snap: prefetch start ($size bytes)"
  # One long-lived connection: archive.org starts range requests slowly, so the
  # transfer is only restarted (with -C -) when it stalls or drops.
  while [ "$(fsize "$tmp")" != "$size" ]; do
    curl -sL -C - --speed-limit 100000 --speed-time 300 -o "$tmp" "$url" &
    pid=$!
    while kill -0 "$pid" 2>/dev/null; do
      if [ -s "$OUT/$prev.tsv.gz" ] || [ -e "$real" ]; then  # the main loop has caught up
        kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
        if [ ! -e "$real" ] && [ -f "$tmp" ]; then mv "$tmp" "$real"; log "$snap: handed $(fsize "$real") bytes to the main loop";
        else log "$snap: main loop started first; dropped $(fsize "$tmp") bytes"; rm -f "$tmp"; fi
        continue 3
      fi
      sleep 5
    done
    wait "$pid" 2>/dev/null || sleep 15
  done
  if [ ! -e "$real" ]; then mv "$tmp" "$real"; log "$snap: complete and in place";
  else rm -f "$tmp"; log "$snap: main loop got there first; dropped"; fi
done
log "prefetch finished"
