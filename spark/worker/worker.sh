#!/bin/bash
set -e

# ============================
#  حدد هنا عنوان الـ Spark Master
#  مثال: spark://spark-master:7077
#  أو مرره كمتغير بيئة عند التشغيل: -e SPARK_MASTER_URL=...
# ============================
: "${SPARK_MASTER_URL:?Need to set SPARK_MASTER_URL (e.g. spark://spark-master:7077)}"

echo "Starting Spark Worker..."
echo "Connecting to Master at $SPARK_MASTER_URL"

$SPARK_HOME/sbin/start-worker.sh \
    --cores ${SPARK_WORKER_CORES} \
    --memory ${SPARK_WORKER_MEMORY} \
    --port ${SPARK_WORKER_PORT} \
    --webui-port ${SPARK_WORKER_WEBUI_PORT} \
    ${SPARK_MASTER_URL}

# إبقاء الـ container شغال
tail -f $SPARK_HOME/logs/*
