#!/usr/bin/env bash
# Start a Spark standalone worker on THIS node and register it with the master.
#     bash start_worker.sh spark://<node1-private-ip>:7077
# Defaults suit a 1-OCPU / 6 GB Always Free VM; override with WORKER_CORES /
# WORKER_MEMORY.
set -euo pipefail
MASTER_URL="${1:?usage: start_worker.sh spark://HOST:7077}"
source "$HOME/tsarm-env.sh"
HOST="$(hostname -I | awk '{print $1}')"
mkdir -p "$HOME/spark-logs" "$HOME/spark-work"
nohup spark-class org.apache.spark.deploy.worker.Worker \
    --host "$HOST" \
    --cores "${WORKER_CORES:-1}" --memory "${WORKER_MEMORY:-4g}" \
    --work-dir "$HOME/spark-work" \
    "$MASTER_URL" > "$HOME/spark-logs/worker.log" 2>&1 &
echo "Worker on $HOST -> $MASTER_URL"
