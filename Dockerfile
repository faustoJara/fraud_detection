FROM python:3.11-slim

ARG SPARK_VERSION=3.5.0
ARG HADOOP_PROFILE=hadoop3

ENV DEBIAN_FRONTEND=noninteractive
ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=python3
ENV PATH=/opt/spark/bin:/opt/spark/sbin:${PATH}

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash curl default-jre-headless procps tini && rm -rf /var/lib/apt/lists/*

# 1. Instalar Spark

RUN echo "Descargando Spark ${SPARK_VERSION}..." && \
    curl --fail --location --show-error \
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz" \
    --output /tmp/spark.tgz || \
    curl --fail --location --show-error \
    "https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz" \
    --output /tmp/spark.tgz && \
    mkdir -p /opt && tar -xzf /tmp/spark.tgz -C /opt && \
    mv "/opt/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}" "${SPARK_HOME}" && \
    rm /tmp/spark.tgz

# 2. INSTALAR DEPENDENCIAS CRÍTICAS (JARs para Iceberg, AWS y Postgres)
RUN curl -o ${SPARK_HOME}/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
    && curl -o ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    && curl -o ${SPARK_HOME}/jars/postgresql-42.7.2.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.2/postgresql-42.7.2.jar

# 3. Instalar librerías de Python
RUN pip install --no-cache-dir pyspark==${SPARK_VERSION} kafka-python-ng

WORKDIR /opt/project

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/bin/bash", "-lc", "tail -f /dev/null"]