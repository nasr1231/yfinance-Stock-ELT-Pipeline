# نفس الـ base image حتى يتطابق مع الـ Master
FROM openjdk:11-jre-slim

ENV SPARK_VERSION=3.3.0
ENV SCALA_VERSION=2.12
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Downloading Spark Standalone without Hadoop Ecosystem
RUN apt-get update && apt-get install -y wget curl bash \
    && wget https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-without-hadoop.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-without-hadoop.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-without-hadoop $SPARK_HOME \
    && rm spark-${SPARK_VERSION}-bin-without-hadoop.tgz \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Downloading Snowflake Spark Connector + JDBC Libraries
RUN wget https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.12.0-spark_3.3/spark-snowflake_2.12-2.12.0-spark_3.3.jar \
    && wget https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.33/snowflake-jdbc-3.13.33.jar \
    && mv spark-snowflake_2.12-2.12.0-spark_3.3.jar $SPARK_HOME/jars/ \
    && mv snowflake-jdbc-3.13.33.jar $SPARK_HOME/jars/


COPY worker.sh /

ENV SPARK_WORKER_CORES 2
ENV SPARK_WORKER_MEMORY 4G
ENV SPARK_WORKER_PORT 7078
ENV SPARK_WORKER_WEBUI_PORT 8081

EXPOSE 8081 7078

CMD ["/bin/bash", "/worker.sh"]
