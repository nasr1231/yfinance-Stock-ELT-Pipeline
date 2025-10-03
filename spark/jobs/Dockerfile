FROM bde2020/spark-python-template:3.3.0-hadoop3.3

COPY . /app/

RUN wget --tries=3 --timeout=30 https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.12.0-spark_3.3/spark-snowflake_2.12-2.12.0-spark_3.3.jar \
    && wget --tries=3 --timeout=30 https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.33/snowflake-jdbc-3.13.33.jar \
    && mv spark-snowflake_2.12-2.12.0-spark_3.3.jar /spark/jars/ \
    && mv snowflake-jdbc-3.13.33.jar /spark/jars/

RUN pip3 install python-dotenv    

ENV ENABLE_INIT_DAEMON=false
ENV SPARK_APPLICATION_PYTHON_LOCATION=/app/spark.py
ENV SPARK_MASTER_NAME=spark-master
ENV SPARK_MASTER_PORT=7077