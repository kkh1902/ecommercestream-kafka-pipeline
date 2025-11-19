FROM apache/airflow:2.7.3-python3.10

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Apache Spark provider
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==4.3.0
