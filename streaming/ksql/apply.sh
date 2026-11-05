#!/bin/bash
# Applies the ksqlDB definitions. Safe to re-run on every `docker compose up`.
set -euo pipefail

SERVER=http://ksqldb-server:8088

ksql "$SERVER" --file /ksql/streamcart.sql

if ksql "$SERVER" --execute "SHOW QUERIES;" | grep -q "INSERTQUERY"; then
  echo "Velocity insert query already running."
else
  ksql "$SERVER" --file /ksql/velocity_insert.sql
fi
