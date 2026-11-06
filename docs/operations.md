# Operations

Common tasks for running StreamCart locally. Commands run from the repository root.

## Start and stop

```bash
docker compose up -d --build   # build and start everything
docker compose ps              # check service state
docker compose stop            # stop, keeping all data
docker compose down -v         # remove containers AND delete all data volumes
```

To run only part of the stack, set `COMPOSE_PROFILES` in `.env`, as described in the README.

To turn on the weekly reprocess, which is paused on a fresh install:

```bash
docker compose exec airflow-scheduler airflow dags unpause historical_backfill
```

## Run the tests

```bash
docker compose exec airflow-scheduler python -m pytest tests -v
```

## Trigger a DAG

```bash
docker compose exec airflow-scheduler airflow dags trigger silver_pipeline
docker compose exec airflow-scheduler airflow dags trigger gold_dbt_pipeline
```

To replay a longer window through Silver:

```bash
docker compose exec airflow-scheduler airflow dags trigger historical_backfill --conf '{"lookback_days": 30}'
```

## Run dbt by hand

```bash
docker compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt deps && dbt build"
```

## Inspect the data

Row counts by layer:

```bash
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT count(*) FROM silver.transactions"'
```

Watch fraud alerts as they happen:

```bash
docker compose exec ksqldb-server ksql http://localhost:8088 --execute "SELECT * FROM fraud_alerts EMIT CHANGES;"
```

Query a Bronze table as it was at an earlier time (Iceberg time travel), from a Spark SQL shell inside the scheduler container:

```sql
SELECT count(*) FROM lake.bronze.transactions TIMESTAMP AS OF '2026-11-05 10:00:00';
```

## Change a Kafka contract

1. Edit the schema in `schemas/avro`. Give any new field a default, or the registry will reject the change as incompatible.
2. Re-register it with `docker compose up schema-init`.
3. The next `observability_monitor` run records the new version in the schema change log.

## Troubleshooting

| Symptom | Likely cause | What to do |
| --- | --- | --- |
| `port is already allocated` | Another service is using the port | Change the host port in `.env` |
| Spark job waits on `Initial job has not accepted any resources` | Worker cores or memory are used up | Check the Spark UI, and confirm the Bronze job still has `spark.cores.max=1` |
| Everything is slow, or Docker returns 500 errors | Docker is short of memory and swapping | Check `docker stats`, run fewer layers with `COMPOSE_PROFILES`, or allocate more memory to Docker |
| A DAG fails right after a fresh start | A service it calls isn't up yet | It retries, and later runs pass |
| Gold tests pass but dashboards are empty | Silver hadn't run yet | Check `gold.fct_orders`, then rerun `gold_dbt_pipeline` |
| Empty Superset charts | Gold hasn't been built yet | Run `silver_pipeline`, then `gold_dbt_pipeline` |
| Schema changes in `batch/db/init` have no effect | Those scripts only run when the Postgres volume is first created | Apply the change by hand, or recreate the volume, which deletes its data |
