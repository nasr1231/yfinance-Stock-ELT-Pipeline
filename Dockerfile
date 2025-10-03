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

# Install Spark client (pre-built with Hadoop support)
RUN curl -L https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz \
    | tar -xz -C /opt/ \
    && mv /opt/spark-3.5.1-bin-hadoop3 /spark

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/spark
ENV PATH=$SPARK_HOME/bin:$JAVA_HOME/bin:$PATH


# Install Python dependencies for Airflow
COPY requirements.txt /requirements.txt

USER airflow
RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r /requirements.txt


