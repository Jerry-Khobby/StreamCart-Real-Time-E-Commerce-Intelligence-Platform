#!/bin/bash
# Applies the ksqlDB definitions. Safe to re-run on every `docker compose up`.
set -euo pipefail

SERVER=http://ksqldb-server:8088

ksql "$SERVER" --file /ksql/streamcart.sql

queries=$(ksql "$SERVER" --execute "SHOW QUERIES;")
if grep -q "INSERTQUERY" <<<"$queries"; then
  echo "Velocity insert query already running."
else
  ksql "$SERVER" --file /ksql/velocity_insert.sql
fi

# The CLI exits 0 even when a statement fails, so confirm the result ourselves.
queries=$(ksql "$SERVER" --execute "SHOW QUERIES;")
for expected in FRAUD_ALERTS RESTOCK_REQUIRED USER_VELOCITY_1H INSERTQUERY; do
  if ! grep -q "$expected" <<<"$queries"; then
    echo "ksqlDB setup failed: no running query for $expected" >&2
    exit 1
  fi
done
echo "ksqlDB setup complete."
