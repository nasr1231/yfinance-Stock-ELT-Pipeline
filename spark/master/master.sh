#!/bin/bash
set -e

export SPARK_HOME=/spark
export SPARK_MASTER_PORT=${SPARK_MASTER_PORT:-7077}
export SPARK_MASTER_WEBUI_PORT=${SPARK_MASTER_WEBUI_PORT:-8080}

echo "Starting Spark Master on port ${SPARK_MASTER_PORT} with Web UI on ${SPARK_MASTER_WEBUI_PORT} ..."
$SPARK_HOME/sbin/start-master.sh \
  --host spark-master \
  --port 7077 \
  --webui-port 8080

# Tail logs to keep container running
tail -f $SPARK_HOME/logs/*