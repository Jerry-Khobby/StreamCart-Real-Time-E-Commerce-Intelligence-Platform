#!/bin/bash
# One-shot Superset bootstrap. Safe to re-run: assets have stable UUIDs.
set -euo pipefail

superset db upgrade
superset fab create-admin \
    --username "$SUPERSET_ADMIN_USER" \
    --password "$SUPERSET_ADMIN_PASSWORD" \
    --firstname Admin --lastname User --email admin@example.com
superset init

python /app/streamcart/build_bundle.py /tmp/streamcart_bundle.zip
superset import-dashboards -p /tmp/streamcart_bundle.zip -u "$SUPERSET_ADMIN_USER"
echo "Superset bootstrap complete."
