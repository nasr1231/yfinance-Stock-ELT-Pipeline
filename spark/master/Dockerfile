FROM bitnami/spark:3.3.0

# The 'bitnami' base image uses /opt/bitnami/spark directory as SPARK_HOME

ENV SPARK_HOME=/opt/bitnami/spark  
ENV PATH=$SPARK_HOME/bin:$PATH
ENV SCALA_VERSION=2.12

USER root

# Downloading Snowflake Spark Connector + JDBC Libraries (بس دول، عشان Spark جاهز)
RUN mkdir -p /var/lib/apt/lists/partial \
    && apt-get update \
    && apt-get install -y --no-install-recommends wget curl \
    && wget --tries=3 --timeout=30 https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.12.0-spark_3.3/spark-snowflake_2.12-2.12.0-spark_3.3.jar \
    && wget --tries=3 --timeout=30 https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.33/snowflake-jdbc-3.13.33.jar \
    && mv spark-snowflake_2.12-2.12.0-spark_3.3.jar $SPARK_HOME/jars/ \
    && mv snowflake-jdbc-3.13.33.jar $SPARK_HOME/jars/ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


RUN mkdir -p $SPARK_HOME/logs && chmod -R 777 $SPARK_HOME/logs

COPY master.sh /

ENV SPARK_MASTER_PORT=7077
ENV SPARK_MASTER_WEBUI_PORT=8080
ENV SPARK_MASTER_LOG=/opt/bitnami/spark/logs

EXPOSE 8080 7077 6066

CMD ["/bin/bash", "/master.sh"]