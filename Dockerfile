FROM apache/airflow:2.7.1-python3.10

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN JAVA_PATH=$(dirname $(dirname $(readlink -f $(which java)))) && \
    ln -s $JAVA_PATH /usr/lib/jvm/java-current

ENV JAVA_HOME=/usr/lib/jvm/java-current
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

RUN pip install --no-cache-dir pyspark==4.1.0 pyyaml