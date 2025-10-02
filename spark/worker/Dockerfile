FROM bde2020/spark-base:3.3.0-hadoop3.3

ENV SPARK_HOME=/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV SCALA_VERSION=2.12

USER root

# Install dependencies and add Snowflake JARs
RUN apk add --no-cache wget curl bash \
    && wget --tries=3 --timeout=30 https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.12.0-spark_3.3/spark-snowflake_2.12-2.12.0-spark_3.3.jar \
    && wget --tries=3 --timeout=30 https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.33/snowflake-jdbc-3.13.33.jar \
    && mv spark-snowflake_2.12-2.12.0-spark_3.3.jar $SPARK_HOME/jars/ \
    && mv snowflake-jdbc-3.13.33.jar $SPARK_HOME/jars/

RUN mkdir -p $SPARK_HOME/logs && chmod -R 777 $SPARK_HOME/logs

COPY worker.sh /worker.sh
RUN chmod +x /worker.sh

# Spark Worker configs
ENV SPARK_WORKER_CORES=2
ENV SPARK_WORKER_MEMORY=4G
ENV SPARK_WORKER_PORT=7078
ENV SPARK_WORKER_WEBUI_PORT=8081
ENV SPARK_WORKER_LOG=$SPARK_HOME/logs
ENV SPARK_MASTER_URL=spark://spark-master:7077

EXPOSE 8081 7078

CMD ["/bin/bash", "/worker.sh"]
