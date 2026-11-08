-- Superset keeps its own metadata (users, charts, dashboards) in the superset database.
-- It gets its own login that owns only that database, instead of the Postgres superuser.
-- Flyway fills in superset_user and superset_password from the environment.

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${superset_user}') THEN
        CREATE ROLE "${superset_user}" LOGIN PASSWORD '${superset_password}'
            NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
    END IF;
    -- The database is created when Postgres is first initialised (batch/db/init).
    IF EXISTS (SELECT FROM pg_database WHERE datname = 'superset') THEN
        EXECUTE format('ALTER DATABASE superset OWNER TO %I', '${superset_user}');
    END IF;
END
$$;

-- Superset never needs the warehouse: it reads Gold through the read-only login.
REVOKE CONNECT ON DATABASE superset FROM PUBLIC;
