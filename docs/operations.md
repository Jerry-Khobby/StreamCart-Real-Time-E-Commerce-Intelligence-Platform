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

CI runs these on every push, along with lint, a dbt build on Postgres, and image builds.

## Inspect rejected rows

Rows that didn't reach Silver are kept in `lake.silver.rejected_rows`:

```sql
SELECT source_table, record_key, record FROM lake.silver.rejected_rows WHERE source_table = 'transactions' LIMIT 10;
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

## Change the database schema

Add a new versioned file to `batch/db/migrations`, then run `docker compose up db-migrate`. Never edit a migration that has already run. The folder's README has the details.

## Change a Kafka contract

1. Edit the schema in `schemas/avro`. Give any new field a default, or the registry will reject the change as incompatible.
2. Re-register it with `docker compose up schema-init`. The producers serialise with the latest registered version, so restart the simulator to pick up new fields.
3. The Bronze job stops on the first event written with the new version and restarts by itself. On restart it adds any new fields as columns. Its log shows `added column`.
4. The next `observability_monitor` run records the new version in the schema change log.
5. To carry the field further, add it to `SILVER_COLUMNS` in `consumer/silver_processor.py` and to the Silver table with a migration.

## Turn on failure alerts

Set `ALERT_WEBHOOK_URL` in `.env` to a Slack or Mattermost incoming webhook, then recreate Airflow with `docker compose up -d`. Failed tasks and volume drops are posted with the task, the error and a link to its log.

## When a Gold build fails its tests

Nothing reaches `gold`: Superset and Grafana keep the last good tables. The failed build is left in `gold_next`, so you can query it to see what went wrong. Fix the model, then rerun `gold_dbt_pipeline`. Its first step clears `gold_next`.

## Check the fraud rules' accuracy

The monitor scores each rule every 15 minutes against the simulator's hidden labels. The Real-Time Operations dashboard shows the latest scores, or:

```sql
SELECT fraud_signal, true_positives, false_positives, false_negatives, precision_pct, recall_pct
FROM observability.fraud_evaluation
WHERE evaluated_at = (SELECT max(evaluated_at) FROM observability.fraud_evaluation);
```

Neither figure should reach 100%: some fraud is built to evade the rules, and some innocent orders look suspicious.

## Retention

| Data | Kept for | Removed by |
| --- | --- | --- |
| Bronze rows | 90 days, then copied to the archive bucket | `bronze_maintenance` |
| Dead-letter rows | 90 days | `bronze_maintenance` |
| Iceberg snapshots | 7 days of time travel | `bronze_maintenance` |
| Row counts, freshness, quality results, fraud scores | 30 days | `housekeeping` |
| Failures, Silver runs, schema changes, reconciliation, volume checks | 365 days | `housekeeping` |
| Airflow run history and task logs | 30 days | `housekeeping` |
| OLTP, Silver and Gold | Kept | They're the history the reports are built from |

## After upgrading from a UTC-only Gold

Gold now dates events in each market's local time. Incremental Gold tables built before that change keep their old UTC dates, so rebuild them once:

```bash
docker compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt build --full-refresh"
```

## Database logins

| Login | `.env` setting | Used by |
| --- | --- | --- |
| MinIO lakehouse login | `MINIO_APP_USER` | Spark and Airflow. Limited to the two lakehouse buckets |
| MinIO root | `MINIO_ROOT_USER` | MinIO itself, `minio-init` and the console |
| Superuser | `POSTGRES_USER` | Postgres and the migrations. Use it for admin work by hand |
| Application | `POSTGRES_APP_USER` | Airflow, Spark, dbt, the simulator and the serving sink |
| Read-only | `POSTGRES_READER_USER` | Grafana, Superset's Gold connection and the REST API |

Migration V8 creates the application login and gives it the existing tables, so an older database is upgraded on the next start.

## Troubleshooting

| Symptom | Likely cause | What to do |
| --- | --- | --- |
| `port is already allocated` | Another service is using the port | Change the host port in `.env` |
| Spark job waits on `Initial job has not accepted any resources` | Worker cores or memory are used up | Check the Spark UI, and confirm the Bronze job still has `spark.cores.max=1` |
| Everything is slow, or Docker returns 500 errors | Docker is short of memory and swapping | Check `docker stats`, run fewer layers with `COMPOSE_PROFILES`, or allocate more memory to Docker |
| A DAG fails right after a fresh start | A service it calls isn't up yet | It retries, and later runs pass |
| Gold tests pass but dashboards are empty | Silver hadn't run yet | Check `gold.fct_orders`, then rerun `gold_dbt_pipeline` |
| Empty Superset charts | Gold hasn't been built yet | Run `silver_pipeline`, then `gold_dbt_pipeline` |
| The Bronze job restarted with `schema ID ... which this job doesn't know yet` | A new contract version was registered | Nothing: it's expected once per new version, and the restart loads it |
| `permission denied` from a pipeline task | A table was created by hand as the superuser | `ALTER TABLE ... OWNER TO streamcart_app`, or create it in a migration |
| A service won't start and `db-migrate` exited with an error | A migration failed | Check `docker compose logs db-migrate`, fix the newest migration, and run `docker compose up db-migrate` |
