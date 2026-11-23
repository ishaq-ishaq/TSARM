#!/usr/bin/env bash
# Start the Spark standalone master on THIS node (node 1).
#     bash start_master.sh            -> prints the master URL
set -euo pipefail
source "$HOME/tsarm-env.sh"
HOST="${SPARK_MASTER_HOST:-$(hostname -I | awk '{print $1}')}"
mkdir -p "$HOME/spark-logs"
nohup spark-class org.apache.spark.deploy.master.Master \
    --host "$HOST" --port 7077 --webui-port 8080 \
    > "$HOME/spark-logs/master.log" 2>&1 &
echo "Master starting: spark://$HOST:7077  (UI: http://$HOST:8080, via SSH tunnel)"
