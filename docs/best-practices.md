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
| Validation gate | Great Expectations blocks a bad Silver write | `quality/` |
| Dead-letter table | Rejected rows are kept as JSON for inspection or replay | `lake.silver.rejected_rows` |
| Tests on every model | Keys, accepted values, ranges and foreign keys | `batch/dbt/models/schema.yml` |
| Tests before downstream use | `dbt build` stops the next layer when a test fails | `gold_dbt_pipeline` DAG |
| Row count guard | The DAG fails if Gold is empty while Silver has orders | `gold_dbt_pipeline` DAG |
| Uniqueness guard | The Silver job fails if a merge leaves duplicate keys | `consumer/lakehouse.py` |
| Volume anomaly checks | A day with under half the usual rows raises one alert | `observability_monitor` DAG |
| Reconciliation | A daily comparison of source orders with Silver | `observability_monitor` DAG |

### Observability

| Practice | How it's done here | Where |
| --- | --- | --- |
| Pipeline health dashboard | DAG success rates, row counts, freshness and quality results | Grafana |
| Failure feed | Tasks that use up their retries are recorded and shown | `observability.pipeline_failures` |
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
| Dimensional model | A star schema with surrogate keys and tested relationships | `batch/dbt/models/` |

### Engineering

| Practice | How it's done here | Where |
| --- | --- | --- |
| Continuous integration | Lint, tests, a real dbt build on Postgres, and image builds | `.github/workflows/ci.yml` |
| Schema migrations | Versioned Flyway migrations run on every start, for new and existing databases | `batch/db/migrations/` |
| Infrastructure as code | The whole stack, its dashboards and its schemas are in the repository | `docker-compose.yml`, `dashboards/`, `batch/db/migrations/` |
| Pinned versions | Image tags, Python constraints and a dbt package lock | `Dockerfile`, `package-lock.yml` |
| Secrets out of git | Settings come from an ignored `.env` file | `.env.example` |
| Least privilege | Grafana, Superset and the API use a read-only login | `V5__readonly_role.sql` |
| Documentation | Architecture, operations, a project guide, and generated dbt docs | `docs/`, CI artifact |

## Partly in place

| Practice | Current state | What a full version would add |
| --- | --- | --- |
| Blocking bad data from users | A failed test stops later layers, but the failing table itself stays visible | Build into a staging schema, then swap it in only if tests pass |
| Alerting | Failures are recorded and shown on a dashboard, but nobody is notified | Send them to Slack, email or a paging service |
| Secrets management | `.env` is safe locally, but `.env.example` holds shared development passwords | A secrets manager, and unique passwords per environment |
| Fraud rules in one place | The same signals are defined in ksqlDB and `agg_fraud_summary` | A shared definition, or a test comparing the two |
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
