#!/usr/bin/env bash
# Self-driving node for experiment E2 on AWS (docs/aws.md). EC2 user data
# clones the repository and starts this script as user `ubuntu`:
#
#     ROLE=master bash aws_e2_node.sh                      # Spark master + driver; runs E2
#     ROLE=worker MASTER_IP=172.31.x.x bash aws_e2_node.sh # one Spark worker (2 cores)
#
# Every node installs the software and generates the same deterministic
# datasets at the same path. A worker registers with the master only after its
# data exists, and the master starts E2 when all workers are alive, so no
# manual SSH is needed. Progress goes to ~/e2-node.log; the master writes
# ~/e2-results.tgz and the line "E2 DONE" when finished.
#
# Cost guards: every node powers off after 24 h whatever happens. Workers
# power off (and are terminated, see docs/aws.md) 10 minutes after the master
# finishes. The master powers off (stopped, disk kept) 12 h after finishing,
# so the results stay on its disk until they are copied.
set -uo pipefail
ROLE="${ROLE:?ROLE=master or ROLE=worker}"
WORKERS="${WORKERS:-7}"                 # worker nodes (the master node runs no worker)
CORES="${CORES:-1,2,4,8,14}"            # executor cores tested: 7 workers x 2 cores = 14
ENTITIES_STRONG="${ENTITIES_STRONG:-1500000}"  # strong scaling: 12M triples, as in E1
ENTITIES_WEAK="${ENTITIES_WEAK:-150000}"       # weak scaling: per core, as in E1
DATA="$HOME/tsarm-data"
log() { echo "[$(date -u +%FT%TZ)] $*"; }

sudo shutdown -h +1440 "TSARM E2 24-hour guard" || true

log "setup ($ROLE)"
SUBNET=172.31.0.0/16 bash "$HOME/TSARM/scripts/cluster/setup_node.sh" || { log "SETUP FAILED"; exit 1; }
source "$HOME/tsarm-env.sh"
cd "$HOME/TSARM"

log "generating datasets in $DATA"
python3 scripts/core_scaling.py --generate-only --data-dir "$DATA" --mode strong \
    --cores "$CORES" --entities "$ENTITIES_STRONG" || { log "GENERATE FAILED"; exit 1; }
python3 scripts/core_scaling.py --generate-only --data-dir "$DATA" --mode weak \
    --cores "$CORES" --entities "$ENTITIES_WEAK" || { log "GENERATE FAILED"; exit 1; }
log "datasets ready"

alive_workers() {
    curl -s "http://$1:8080/json/" \
        | python3 -c 'import json,sys; print(json.load(sys.stdin).get("aliveworkers", 0))' 2>/dev/null \
        || echo 0
}

if [ "$ROLE" = worker ]; then
    : "${MASTER_IP:?MASTER_IP is required for a worker}"
    until curl -sf "http://$MASTER_IP:8080/json/" >/dev/null; do sleep 15; done
    WORKER_CORES=2 WORKER_MEMORY=5g bash scripts/cluster/start_worker.sh "spark://$MASTER_IP:7077"
    log "worker registered with $MASTER_IP"
    # Power off once the master has been unreachable for 10 minutes (E2 finished).
    fails=0
    while :; do
        sleep 60
        if curl -sf "http://$MASTER_IP:8080/json/" >/dev/null; then fails=0; else fails=$((fails + 1)); fi
        if [ "$fails" -ge 10 ]; then log "master gone; powering off"; sudo shutdown -h now; exit 0; fi
    done
fi

# ---- master ----
IP="$(hostname -I | awk '{print $1}')"
MASTER="spark://$IP:7077"
bash scripts/cluster/start_master.sh
log "waiting for $WORKERS workers at $MASTER"
waited=0
until [ "$(alive_workers localhost)" -ge "$WORKERS" ]; do
    sleep 30
    waited=$((waited + 30))
    if [ "$waited" -ge 5400 ]; then
        log "only $(alive_workers localhost) of $WORKERS workers after 90 min; running with those"
        break
    fi
done
log "$(alive_workers localhost) workers alive; starting E2"

export TSARM_DRIVER_MEMORY=4g
run() {  # run <mode> <entities> <tag>
    python3 scripts/core_scaling.py --master-url "$MASTER" --data-dir "$DATA" --mode "$1" \
        --cores "$CORES" --entities "$2" --executor-memory 2g --tag "$3"
}
log "E2 strong scaling"
if ! run strong "$ENTITIES_STRONG" aws; then
    log "strong scaling FAILED at $ENTITIES_STRONG entities; retrying at 300000"
    python3 scripts/core_scaling.py --generate-only --data-dir "$DATA" --mode strong \
        --cores "$CORES" --entities 300000
    run strong 300000 aws_small || log "strong scaling FAILED again"
fi
log "E2 weak scaling"
run weak "$ENTITIES_WEAK" aws || log "weak scaling FAILED"

(cd "$HOME/TSARM" && tar czf "$HOME/e2-results.tgz" results/cluster_scaling_* "$HOME/e2-node.log" 2>/dev/null)
log "E2 DONE; results in ~/e2-results.tgz"
bash scripts/cluster/stop_all.sh
sudo shutdown -c || true
sudo shutdown -h +720 "TSARM E2 finished; master powers off (disk kept)" || true
