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

COPY master.sh /master.sh
RUN chmod +x /master.sh

ENV SPARK_MASTER_PORT=7077
ENV SPARK_MASTER_WEBUI_PORT=8080
ENV SPARK_MASTER_LOG=$SPARK_HOME/logs

EXPOSE 8080 7077 6066

CMD ["/bin/bash", "/master.sh"]