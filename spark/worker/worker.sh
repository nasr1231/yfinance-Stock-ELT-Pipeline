#!/bin/bash
set -e
# Set environment variables with default values if not already set
: "${SPARK_MASTER_URL:?Need to set SPARK_MASTER_URL (e.g. spark://spark-master:7077)}"

echo "Starting Spark Worker..."
echo "Connecting to Master at $SPARK_MASTER_URL"

$SPARK_HOME/sbin/start-worker.sh \
    --cores ${SPARK_WORKER_CORES} \
    --memory ${SPARK_WORKER_MEMORY} \
    --port ${SPARK_WORKER_PORT} \
    --webui-port ${SPARK_WORKER_WEBUI_PORT} \
    ${SPARK_MASTER_URL}

# Keep the container running to maintain the Spark Worker
tail -f $SPARK_HOME/logs/*
