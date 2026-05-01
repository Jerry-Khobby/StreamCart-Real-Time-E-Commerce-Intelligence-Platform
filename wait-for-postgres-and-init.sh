#!/bin/bash
set -e

echo "Waiting for Postgres to be ready..."
until pg_isready -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER"; do
  sleep 2
done

echo "Postgres is ready. Initializing Airflow DB..."
airflow db init



echo "Creating Airflow admin user..."
airflow users create \
    --username ${AIRFLOW_ADMIN_USER} \
    --password ${AIRFLOW_ADMIN_PASSWORD} \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

echo "Creating Spark connection..."
airflow connections add spark_default \
    --conn-type spark \
    --conn-host spark://spark-master \
    --conn-port 7077 \
    || echo "spark_default connection already exists — skipping"

echo "Airflow DB initialized. Exiting."