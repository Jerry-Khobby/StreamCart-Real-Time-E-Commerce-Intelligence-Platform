#!/bin/bash
# A read-only login for Grafana, Superset and the REST API, so a dashboard or API bug
# can't change data. Runs once, when the Postgres volume is first initialised.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
CREATE ROLE "$POSTGRES_READER_USER" LOGIN PASSWORD '$POSTGRES_READER_PASSWORD';
-- Scripts run alphabetically, so silver_schema.sql hasn't run yet on a fresh install.
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
GRANT USAGE ON SCHEMA silver, gold, realtime, observability, public TO "$POSTGRES_READER_USER";
GRANT SELECT ON ALL TABLES IN SCHEMA silver, gold, realtime, observability TO "$POSTGRES_READER_USER";
-- dbt recreates its tables, so future tables need the grant too.
ALTER DEFAULT PRIVILEGES FOR ROLE "$POSTGRES_USER" IN SCHEMA silver, gold, realtime, observability
    GRANT SELECT ON TABLES TO "$POSTGRES_READER_USER";
SQL
