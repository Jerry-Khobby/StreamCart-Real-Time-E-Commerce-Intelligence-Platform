# Data engineering practices

This is an honest checklist of StreamCart against common data engineering practices. No project has all of them. The gaps at the end are deliberate, because they need more than one machine or a real production environment.

## In place

### Reliability

| Practice | How it's done here | Where |
| --- | --- | --- |
| Idempotent writes | Replaying the same rows changes nothing, through `MERGE INTO` and upserts | `consumer/lakehouse.py`, `consumer/realtime_sink.py` |
| Safe incremental loads | The watermark moves only after a successful write, and tracks arrival time so late events are kept | `consumer/silver_processor.py` |
| Replay from raw data | Bronze keeps events unmodified, so corrected rules can be rerun | `historical_backfill` DAG |
| Reproducible replays | Age rules use arrival time, so a row gets the same answer whenever it's processed | `consumer/silver_processor.py` |
| Idempotent dead-letter writes | Rejected rows have an ID from their Kafka position, so retries can't duplicate them | `consumer/silver_processor.py` |
| Stream recovery | Checkpoints are in object storage, and the job restarts itself | `consumer/spark_streaming.py` |
| At-least-once serving | Kafka offsets are committed only after the database commit | `consumer/realtime_sink.py` |
| Retries and timeouts | Every DAG sets retries, and long tasks set an execution timeout | `batch/dags/` |
| Data-aware scheduling | Gold runs when Silver is updated, not at a fixed time | Airflow Datasets |
| Late-arriving dimensions | Orders wait until their customer exists, then load on the next run | `fct_orders.sql` |

### Data quality

| Practice | How it's done here | Where |
| --- | --- | --- |
| Data contracts | Avro schemas with BACKWARD compatibility, and producers can't register new ones | `schemas/avro/` |
| Schema evolution | Bronze decodes each event with the version it was written in, and adds new fields as columns | `consumer/spark_streaming.py` |
| Rules in one place | Thresholds and value sets live in one module, and a test checks the SQL, YAML and JSON copies match | `quality/rules.py`, `tests/test_rules_consistency.py` |
| Validation gate | Great Expectations blocks a bad Silver write | `quality/` |
| Dead-letter table | Rejected rows are kept as JSON for inspection or replay | `lake.silver.rejected_rows` |
| Tests on every model | Keys, accepted values, ranges and foreign keys | `batch/dbt/models/schema.yml` |
| Tests before downstream use | `dbt build` stops the next layer when a test fails | `gold_dbt_pipeline` DAG |
| Write, audit, publish | Gold is built and tested in `gold_next`, then swapped into `gold` in one transaction only if every test passes, so readers never see a failed table | `gold_dbt_pipeline` DAG, `V9__gold_build_schema.sql` |
| Row count guard | The DAG fails if Gold is empty while Silver has orders | `gold_dbt_pipeline` DAG |
| Uniqueness guard | The Silver job fails if a merge leaves duplicate keys | `consumer/lakehouse.py` |
| Volume anomaly checks | A day with under half the usual rows raises one alert | `observability_monitor` DAG |
| Reconciliation | A daily comparison of source orders with Silver | `observability_monitor` DAG |
| Honest rule evaluation | Fraud rules are scored for precision and recall against hidden labels, including fraud built to evade them and innocent orders that look suspicious | `observability.fraud_evaluation` |

### Observability

| Practice | How it's done here | Where |
| --- | --- | --- |
| Pipeline health dashboard | DAG success rates, row counts, freshness and quality results | Grafana |
| Failure feed | Tasks that use up their retries are recorded and shown | `observability.pipeline_failures` |
| Alerting | Failed tasks and volume drops are posted to a Slack or Mattermost webhook, when `ALERT_WEBHOOK_URL` is set | `batch/dags/common.py` |
| Freshness checks | dbt warns if Silver stops updating, and the monitor samples every table | `sources.yml`, `observability_monitor` |
| Schema change detection | Warehouse columns and Kafka contract versions are compared with their last saved state | `observability_monitor` |
| Pipeline lag | Consumer lag and event latency are sampled per topic | `realtime.pipeline_lag` |

### Storage and modelling

| Practice | How it's done here | Where |
| --- | --- | --- |
| Medallion layers | Raw Bronze, validated Silver and business-ready Gold | Lakehouse and Postgres |
| Table format with transactions | Iceberg snapshots, time travel and atomic commits | `consumer/lakehouse.py` |
| Partitioning | Bronze by region and date, and Kafka by region | `spark_streaming.py`, `producer/config.py` |
| Retention | Rows over 90 days old move to a cold-storage bucket | `consumer/bronze_maintenance.py` |
| Table maintenance | Compaction and snapshot expiry for Bronze and Silver | `consumer/bronze_maintenance.py` |
| Bounded growth | Monitoring samples, audit tables, the dead-letter table and Airflow's history are pruned on a schedule | `housekeeping`, `bronze_maintenance` |
| Local time for regional reports | Gold dates and hours are in each market's own time zone, so a Tokyo order at 1 a.m. counts on the Tokyo day | `seeds/regions.csv`, `macros/market_time.sql` |
| Dimensional model | A star schema with surrogate keys and tested relationships | `batch/dbt/models/` |

### Engineering

| Practice | How it's done here | Where |
| --- | --- | --- |
| Continuous integration | Lint, tests, a real dbt build on Postgres, and image builds | `.github/workflows/ci.yml` |
| Schema migrations | Versioned Flyway migrations run on every start, for new and existing databases | `batch/db/migrations/` |
| Infrastructure as code | The whole stack, its dashboards and its schemas are in the repository | `docker-compose.yml`, `dashboards/`, `batch/db/migrations/` |
| Pinned versions | Image tags, Python constraints and a dbt package lock | `Dockerfile`, `package-lock.yml` |
| Secrets out of git | Settings come from an ignored `.env` file | `.env.example` |
| Least privilege in Kafka | Every service logs in with its own SASL user, and ACLs allow each only the topics, groups and operations it uses. Anything not granted is refused | `streaming/kafka/init.sh` |
| Least privilege in MinIO | Spark and Airflow use a MinIO login that reaches only the lakehouse and archive buckets, and their containers never hold the root account | `storage/minio-init.sh` |
| Least privilege in Postgres | Pipeline services use a login that can't create roles or databases, dashboards and the API use a read-only one, and Superset's metadata has its own login that owns only that database. The superuser is left to Postgres and the migrations | `V5__readonly_role.sql`, `V8__app_role.sql` |
| Documentation | Architecture, operations, a project guide, and generated dbt docs | `docs/`, CI artifact |

## Partly in place

| Practice | Current state | What a full version would add |
| --- | --- | --- |
| Alerting | Failures reach a chat webhook, but there's no on-call rota, paging or escalation | A paging service such as PagerDuty, with routing by severity |
| Secrets management | `.env` is safe locally, but `.env.example` holds shared development passwords | A secrets manager, and unique passwords per environment |
| Encryption in transit | Kafka logins and Postgres connections travel unencrypted inside the Docker network | TLS on Kafka (SASL_SSL), Postgres and MinIO |
| Old secret in history | `airflow.cfg` was committed with a webserver `secret_key` before being removed. It isn't used now, but it stays in git history | Rewrite history if the key was ever reused |

## Deliberately not done

| Practice | Why it isn't here |
| --- | --- |
| High availability | One Kafka broker, Spark worker and Postgres. Replicas would need more than one machine |
| Separate databases | One Postgres holds the warehouse, Airflow and the Iceberg catalog, to fit on a laptop |
| Lake-based Gold | dbt, Superset and Grafana query Postgres directly, which is simpler but wouldn't scale as far |
| Slowly changing dimensions | Simulated customers and products don't change, so type 1 dimensions lose nothing here |
| Masking personal data | All the data is simulated. Real customer data would need masking and access rules |
| Dedicated lineage tool | dbt docs show lineage for Gold. Tools such as OpenLineage would cover the whole path |
| Backups | Everything can be rebuilt, because the data is simulated |
| Load testing | It runs at about 1 order per second, and hasn't been tested at high volume |
