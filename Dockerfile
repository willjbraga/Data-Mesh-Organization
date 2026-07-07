FROM apache/airflow:2.7.1-python3.10

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Agora sim, este caminho está garantido pelo sistema operacional
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

RUN pip install --no-cache-dir pyspark pyyaml