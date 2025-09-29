set -e

echo "Starting Spark Master on port ${SPARK_MASTER_PORT} ..."
$SPARK_HOME/sbin/start-master.sh --host 0.0.0.0 --port ${SPARK_MASTER_PORT} --webui-port ${SPARK_MASTER_WEBUI_PORT}

tail -f $SPARK_HOME/logs/*
