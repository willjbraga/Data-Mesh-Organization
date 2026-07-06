FROM apache/airflow:2.7.1-python3.10

USER root

# Instala o Java (JVM) necessário para o PySpark rodar dentro do contentor
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configura a variável de ambiente do Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

USER airflow

# Garante a instalação do PySpark e PyYAML no ambiente do Airflow
RUN pip install --no-cache-dir pyspark pyyaml