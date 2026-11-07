-- Least privilege: Airflow, Spark, dbt, the simulator and the serving sink log in as an
-- application role instead of the Postgres superuser. That role can read and write the
-- pipeline's schemas but can't create roles or databases, read other databases' files,
-- or bypass permissions. The superuser is left to Postgres itself and to these migrations.
-- Flyway fills in app_user, app_password and reader_user from the environment.

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${app_user}') THEN
        CREATE ROLE "${app_user}" LOGIN PASSWORD '${app_password}'
            NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
    END IF;
END
$$;

-- Airflow's metadata and the Iceberg catalog live in public; the rest are the pipeline's layers.
-- CREATE lets dbt rebuild its models and Spark stage its upserts.
GRANT USAGE, CREATE ON SCHEMA public, oltp, silver, gold, realtime, observability TO "${app_user}";

-- Hand over what already exists (on a database older than this migration, that includes
-- Airflow's tables and the Iceberg catalog). Flyway's own history stays with the superuser.
-- Changing a table's owner also moves its indexes and the sequences its columns own.
DO $$
DECLARE
    rel record;
BEGIN
    FOR rel IN
        SELECT n.nspname, c.relname, c.relkind
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname IN ('public', 'oltp', 'silver', 'gold', 'realtime', 'observability')
          AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
          AND c.relname <> 'flyway_schema_history'
          AND pg_get_userbyid(c.relowner) <> '${app_user}'
          -- Sequences owned by a column move with their table
          AND NOT (c.relkind = 'S' AND EXISTS (
              SELECT FROM pg_depend d WHERE d.objid = c.oid AND d.deptype IN ('a', 'i')))
    LOOP
        EXECUTE format('ALTER %s %I.%I OWNER TO %I',
            CASE rel.relkind WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW'
                             WHEN 'f' THEN 'FOREIGN TABLE' WHEN 'S' THEN 'SEQUENCE' ELSE 'TABLE' END,
            rel.nspname, rel.relname, '${app_user}');
    END LOOP;
END
$$;

-- Tables that later migrations create belong to the superuser, so the app needs rights on them.
ALTER DEFAULT PRIVILEGES IN SCHEMA oltp, silver, gold, realtime, observability
    GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLES TO "${app_user}";
ALTER DEFAULT PRIVILEGES IN SCHEMA oltp, silver, gold, realtime, observability
    GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO "${app_user}";

-- The read-only login's default grant (V5) only covers tables the superuser creates.
-- dbt and Spark now create tables as the app role, so they need the same grant.
ALTER DEFAULT PRIVILEGES FOR ROLE "${app_user}" IN SCHEMA silver, gold, realtime, observability
    GRANT SELECT ON TABLES TO "${reader_user}";
