FROM apache/airflow:2.9.2-python3.11

# Prevents Python from writing .pyc files and buffer stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /opt/airflow

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       gcc \
       libxml2-dev \
       libxslt1-dev \
       git \
       curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /requirements.txt

USER airflow

RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r /requirements.txt

CMD ["webserver"]