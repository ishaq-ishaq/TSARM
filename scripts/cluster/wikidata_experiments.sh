#!/usr/bin/env bash
# Unattended Wikidata experiments on the data VM (docs/azure.md, step A4).
#
# Waits until all seven snapshots are extracted (data/processed/wikidata/<snap>.tsv.gz,
# written by scripts/download_wikidata.sh), then runs, in this order:
#   1. TSARM on a 1 % entity sample (quick check)
#   2. TSARM on a 10 % sample                          (main sample result)
#   3. N-Triples exports of the 0.1 % and 1 % samples  (inputs for AMIE+ / RDFRules on the Mac)
#   4. E4 event candidates on the 10 % sample
#   5. TSARM on the 10 % sample with hierarchy depth 2
#   6. TSARM on all data (depth 1)                     (largest; may run out of memory)
#   7. E4 event candidates on all data
# and packs results/ into ~/wikidata-results.tgz. Every step has a time limit,
# writes its own log in results/logs/, and is skipped when its output already
# exists, so the script can simply be started again (e.g. from @reboot) after
# an interruption. Progress: ~/experiments.log ("EXPERIMENTS-DONE" at the end).
#
#     tmux new -d -s experiments "bash scripts/cluster/wikidata_experiments.sh; exec bash"
set -uo pipefail
EXP="$(cd "$(dirname "$0")/../.." && pwd)"                 # this checkout
DATA="${DATA:-$HOME/TSARM/data/processed/wikidata}"        # where download_wikidata.sh writes
SNAPS="${SNAPS:-2017-01 2018-01 2019-01 2020-09 2021-01 2022-01 2026-11}"
LOG="${LOG:-$HOME/experiments.log}"
TGZ="${TGZ:-$HOME/wikidata-results.tgz}"
export TSARM_DRIVER_MEMORY="${TSARM_DRIVER_MEMORY:-22g}"   # Standard_DS12_v2: 28 GB
log() { echo "[$(date -u +%FT%TZ)] $*" >> "$LOG"; }

[ -f "$HOME/tsarm-env.sh" ] && source "$HOME/tsarm-env.sh"
cd "$EXP"
mkdir -p results/logs results/events
log "started in $EXP; data $DATA; driver memory $TSARM_DRIVER_MEMORY"

last=""
while :; do
    missing=""
    for s in $SNAPS; do [ -s "$DATA/$s.tsv.gz" ] || missing="$missing $s"; done
    [ -z "$missing" ] && break
    [ "$missing" != "$last" ] && log "waiting for:$missing"
    last="$missing"
    sleep 600
done
log "all snapshots present: $(cd "$DATA" && ls -s --block-size=M ./*.tsv.gz | tr '\n' ' ')"

step() {  # step <name> <output that marks completion> <time limit> <command...>
    local name=$1 done_marker=$2 limit=$3; shift 3
    if [ -e "$done_marker" ]; then log "$name: already done, skipped"; return; fi
    log "$name: start"
    local t0=$SECONDS
    timeout "$limit" "$@" > "results/logs/$name.log" 2>&1
    local rc=$?
    log "$name: exit $rc after $(( (SECONDS - t0) / 60 )) min$([ $rc = 124 ] && echo ' (TIME LIMIT)')"
}
tsarm() {  # tsarm <tag> <time limit> <extra args...>
    local tag=$1 limit=$2; shift 2
    step "$tag" "results/wikidata_temporal_rules_$tag.json" "$limit" \
        python3 scripts/run_wikidata_temporal.py --data-dir "$DATA" --tag "$tag" "$@"
}

tsarm az_s001 3h --sample 0.01
tsarm az_s010 8h --sample 0.10
step sample_0001 results/wikidata_sample_0001/sample.json 3h \
    python3 scripts/export_wikidata_sample.py --data-dir "$DATA" --sample 0.001 --out results/wikidata_sample_0001
step sample_001 results/wikidata_sample_001/sample.json 4h \
    python3 scripts/export_wikidata_sample.py --data-dir "$DATA" --sample 0.01 --out results/wikidata_sample_001
step events_s010 results/events/wikidata_candidates_s010.csv 8h \
    python3 scripts/propose_events.py propose --data-dir "$DATA" --sample 0.10 \
        --out results/events/wikidata_candidates_s010.csv
tsarm az_s010_h2 10h --sample 0.10 --superclass-depth 2
tsarm az_full 30h
step events_full results/events/wikidata_candidates_full.csv 16h \
    python3 scripts/propose_events.py propose --data-dir "$DATA" \
        --out results/events/wikidata_candidates_full.csv

tar czf "$TGZ" results/wikidata_temporal_rules_az_* results/events results/logs \
    results/wikidata_sample_0001 results/wikidata_sample_001 2>/dev/null
log "packed $(du -h "$TGZ" | cut -f1) into $TGZ"
log "EXPERIMENTS-DONE"
