# StreamCart platform image.
#
# Airflow, the Spark master/worker, the Spark drivers and the data generator all
# run from this one image. A PySpark driver and its executors must share a Python
# minor version and Spark version, so using a single image removes that whole
# class of version-skew failure.
FROM apache/airflow:2.8.1-python3.11

ARG SPARK_VERSION=3.5.0
ARG ICEBERG_VERSION=1.5.2
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG POSTGRES_JDBC_VERSION=42.7.3
ARG AIRFLOW_CONSTRAINTS=https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.11.txt

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless curl procps ca-certificates \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# dbt lives in its own virtualenv: its pins conflict with Airflow's constraints.
# The base image sets PIP_USER=true, which pip refuses inside a virtualenv.
RUN python -m venv /opt/dbt-venv \
    && PIP_USER=false /opt/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.7.13 dbt-postgres==1.7.13 \
    && ln -s /opt/dbt-venv/bin/dbt /usr/local/bin/dbt

USER airflow
WORKDIR /opt/airflow
ENV PYTHONPATH=/opt/airflow
ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
ENV PYSPARK_PYTHON=python
ENV PYSPARK_DRIVER_PYTHON=python

COPY --chown=airflow:root requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt --constraint "${AIRFLOW_CONSTRAINTS}"

# Bake the connector jars into Spark, so jobs never resolve --packages at runtime.
RUN set -e; cd "${SPARK_HOME}/jars"; \
    M=https://repo1.maven.org/maven2; \
    for jar in \
      "org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar" \
      "org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar" \
      "org/apache/spark/spark-avro_2.12/${SPARK_VERSION}/spark-avro_2.12-${SPARK_VERSION}.jar" \
      "org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
      "org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" \
      "org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
      "com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
      "org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
      "org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"; \
    do curl -fsSLO "${M}/${jar}"; done
