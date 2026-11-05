#!/bin/bash
# One-shot Airflow bootstrap. Safe to re-run on every `docker compose up`.
set -euo pipefail

echo "Waiting for Postgres to be ready..."
until pg_isready -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER"; do
  sleep 2
done

echo "Migrating the Airflow metadata database..."
airflow db migrate

echo "Ensuring the Airflow admin user exists..."
airflow users create \
    --username "$AIRFLOW_ADMIN_USER" \
    --password "$AIRFLOW_ADMIN_PASSWORD" \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# The Spark cluster has one worker: a pool keeps batch jobs from starving each other.
airflow pools set spark_batch 1 "Serialises Spark batch jobs on the single worker"

echo "Airflow bootstrap complete."
