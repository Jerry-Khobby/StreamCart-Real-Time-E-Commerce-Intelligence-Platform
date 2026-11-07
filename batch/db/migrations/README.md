# Database migrations

Flyway applies these to Postgres through the `db-migrate` service on every `docker compose up`. It records each one in `public.flyway_schema_history`, so each runs exactly once per database.

## Changing the schema

1. Add a new file named `V<next number>__<description>.sql`, such as `V7__add_order_channel.sql`.
2. Never edit a migration that has already run. Flyway checks each file's checksum and stops if an applied one has changed.
3. Restart the service with `docker compose up db-migrate`. Existing databases get only the new file.

## Notes

- Early migrations use `IF NOT EXISTS`, so they also apply cleanly to databases created before Flyway was added. Flyway baselines those at version 0.
- `${reader_user}` and `${reader_password}` are filled in from `POSTGRES_READER_USER` and `POSTGRES_READER_PASSWORD` in `.env`.
- `batch/db/init/00_databases.sql` still runs through Postgres's own init, because `CREATE DATABASE` can't run inside a migration's transaction.
