#!/usr/bin/env bash
# Stop any Spark master/worker processes on THIS node.
pkill -f org.apache.spark.deploy.worker.Worker || true
pkill -f org.apache.spark.deploy.master.Master || true
echo "Spark daemons stopped on $(hostname)"
