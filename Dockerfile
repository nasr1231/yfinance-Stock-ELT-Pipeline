FROM apache/airflow:2.9.2-python3.11

# Prevents Python from writing .pyc files and buffer stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /opt/airflow

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       openjdk-17-jdk \
       gcc \
       libxml2-dev \
       libxslt1-dev \
       git \
       curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Spark version
ENV SPARK_VERSION=3.3.0
ENV HADOOP_VERSION=3
ENV SCALA_VERSION=2.12
ENV SPARK_HOME=/spark

# Install Spark client
RUN curl -L "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    | tar -xz -C / \
    && mv /spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$SPARK_HOME/bin:$JAVA_HOME/bin:$PATH


# Install Python dependencies for Airflow
COPY requirements.txt /requirements.txt

USER airflow
RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r /requirements.txt


