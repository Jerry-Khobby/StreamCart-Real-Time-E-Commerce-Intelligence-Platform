-- Write-audit-publish for Gold: dbt builds and tests in gold_next, and the Gold DAG moves
-- the tables into gold only when every test has passed. Readers never see a table that
-- failed its tests. Flyway fills in app_user and reader_user from the environment.

CREATE SCHEMA IF NOT EXISTS gold_next;
GRANT USAGE, CREATE ON SCHEMA gold_next TO "${app_user}";

-- A table keeps its grants when it moves schema, so give the read-only login access as
-- dbt creates each one. Otherwise published tables would lose the reader's access.
ALTER DEFAULT PRIVILEGES FOR ROLE "${app_user}" IN SCHEMA gold_next
    GRANT SELECT ON TABLES TO "${reader_user}";
