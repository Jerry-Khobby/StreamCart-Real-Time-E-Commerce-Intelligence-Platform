-- A read-only login for Grafana, Superset and the REST API, so a dashboard or API bug
-- can't change data. Flyway fills in reader_user and reader_password from the environment.

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${reader_user}') THEN
        CREATE ROLE "${reader_user}" LOGIN PASSWORD '${reader_password}';
    END IF;
END
$$;

-- gold is created by dbt later, but the grants need it to exist now.
CREATE SCHEMA IF NOT EXISTS gold;

GRANT USAGE ON SCHEMA silver, gold, realtime, observability, public TO "${reader_user}";
GRANT SELECT ON ALL TABLES IN SCHEMA silver, gold, realtime, observability TO "${reader_user}";

-- dbt recreates its tables, so tables created later need the grant too.
ALTER DEFAULT PRIVILEGES IN SCHEMA silver, gold, realtime, observability
    GRANT SELECT ON TABLES TO "${reader_user}";
