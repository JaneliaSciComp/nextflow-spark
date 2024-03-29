FROM oraclelinux:8

ENV SPARK_VERSION=3.0.1
ENV HADOOP_VERSION=3.2

RUN dnf install -y \
        tar wget \
        hostname \
        net-tools \
        java-1.8.0-openjdk.x86_64 \
        python3 \
        && wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
        && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
        && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark \
        && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
        && cd /

