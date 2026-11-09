# StreamCart: Project Guide

A full explanation of what StreamCart does, how the pipeline works, and what each tool is for.

## Read this first: the status of the project

StreamCart has run end to end from a clean install, and its results are listed under "Verified results" in the README. The data is simulated, so the fraud scores show that the pipeline works, not how it would perform on real fraud.

## Coming back to the project

The original build covered roughly the first half of the batch path, and work stopped while trying to get Spark to run from Airflow. The last commits tried `DockerOperator`, then `BashOperator`, then `SparkSubmitOperator`. The real causes are listed under "Problems that blocked the original build" further down.

### What existed, and what happened to it

| Part | Original version | Now |
| --- | --- | --- |
| Simulator | JSON events through `kafka-python`, with fraud, bots and funnels | Same idea, rewritten: Avro, region partitions, stock tracking, injected bad data |
| Bronze | Spark Streaming writing Parquet to MinIO, partitioned by region and date | Same stream and partitioning, now in Iceberg tables with durable checkpoints |
| Silver | Spark job with cleaning rules, watermarks and a staging-table upsert to Postgres | Same structure and upsert. Added Great Expectations, Iceberg `MERGE INTO`, rate-based bot detection and historical FX |
| dbt | All 11 Gold models, with staging and tests | Same models with their bugs fixed. Two models added: `agg_customer_retention` and `agg_weather_demand` |
| Airflow | `silver_pipeline`, `gold_dbt_pipeline`, `fx_rates_refresh` | All three kept and fixed. Five DAGs added |
| Compose | Postgres, MinIO, Zookeeper, Kafka, Airflow, Spark, generator | Same core, with one shared image and the new services |

### What didn't exist before

- Schema Registry and Avro contracts
- ksqlDB fraud and low-stock rules
- The serving sink and REST API
- Grafana and Superset dashboards
- Great Expectations suites and observability monitoring
- The OLTP database, 90-day retention and weekly backfill
- Automated tests and all documentation

## The project in plain English

StreamCart is the data system behind an online shop: it collects everything happening on the site and turns it into instant alerts and trustworthy business reports. There is no real shop or website. A simulator pretends to be one, generating customers, orders and warehouse activity across five markets: US, EU, Asia, Africa and Latin America.

### What a shop like this produces

An e-commerce company produces three constant streams of activity:

- **Transactions:** a customer places an order and pays.
- **Clickstream:** every page a visitor views, including searches, product pages, the cart and checkout.
- **Inventory updates:** stock leaves the warehouse with orders, arrives in restocks, or is written off when damaged.

### The three business problems

| Problem | What goes wrong today | What StreamCart does |
| --- | --- | --- |
| Fraud is caught too late | A nightly report finds a stolen card after the goods have shipped | Checks every order against fraud rules as it arrives |
| Stock decisions are stale | Warehouse teams restock using yesterday's numbers | Raises a low-stock alert as soon as a level drops below its reorder point |
| Teams disagree on the numbers | Marketing, finance and operations each keep their own figures | Gives everyone one cleaned, tested set of tables |

### The core idea

The same events are used in two ways, because the two needs conflict. A fraud alert must be fast, even if the data isn't perfect yet. A finance report must be correct, even if it's an hour or a day old. So the data takes two paths: a **speed path** that answers in seconds and a **batch path** that cleans and validates before anyone reports on it.

### Who uses the results

| Person | What they open | What they get |
| --- | --- | --- |
| Fraud analyst | Grafana | A live feed of suspicious orders |
| Warehouse manager | Grafana | Which stock is running low, and where |
| Business analyst | Superset | Revenue trends, customer retention, conversion funnel, top products |
| Data engineer | Grafana and Airflow | Whether the pipelines ran, on time and with good data |
| Fraud model (software) | REST API | A customer's recent order count, spend and alerts |

## One order's journey

The easiest way to understand the system is to follow one order, split into two copies that travel at different speeds. Imagine a customer in Lagos buys a $120 item, paying in Nigerian naira.

### The order is placed

1. The simulator creates the order, priced in USD and converted to naira.
2. It writes the order and payment to the transactional database (the `oltp` schema), as a real shop's backend would.
3. It publishes an event to the Kafka `transactions` topic. Schema Registry's contract defines what that event must look like.
4. The event goes to the Africa partition, because each region has its own partition.
5. A warehouse in the same market ships it, so an `inventory_updates` event records the lower stock level.

### Within seconds: the speed path

1. ksqlDB reads the order and checks the fraud rules. This one is normal, so no alert fires.
2. If it had been $9,000 placed 20 seconds after login, ksqlDB would publish a `HIGH_VALUE_NEW_SESSION` alert to the `fraud_alerts` topic.
3. ksqlDB also checks the stock event. If stock fell below 20 units, it publishes to `restock_required`.
4. The serving sink copies these events into Postgres, and Grafana shows them on its next refresh.
5. A fraud model could call the REST API for that customer's recent activity.

### Within the hour: the batch path

1. Spark Streaming saves the raw event to the Bronze table on the next one-minute cycle, unchanged.
2. The hourly Silver job picks it up and Great Expectations profiles the incoming data.
3. The job checks that the user exists, the amount is sensible and the currency is valid. It removes duplicates and converts to USD at the rate in force when the order was placed.
4. Great Expectations checks the cleaned rows. If that check fails, nothing is written.
5. The row is merged into the Silver table and published to Postgres.

### By next morning: reporting

1. When Silver finishes, Airflow triggers dbt, which runs its source tests, then rebuilds the Gold tables.
2. The order becomes a row in `fct_orders`, linked to its customer, product, date and region.
3. Its value adds to the Africa total for that day in `agg_daily_revenue`.
4. dbt tests the output, and the analyst sees the updated figure in Superset.

## The architecture

StreamCart combines a **lakehouse** for batch data with a separate fast path for live events. Both start from the same Kafka topics.

![StreamCart architecture: a simulator feeds Kafka and a transactional database. The speed path runs through ksqlDB and a serving sink to Grafana and a REST API. The batch path runs through Spark into Bronze and Silver Iceberg tables, then dbt builds Gold in Postgres for Superset. Airflow orchestrates the batch path.](images/architecture.svg)

All the Postgres schemas sit in one database instance, which also holds Airflow's metadata and the Iceberg catalog.

### The layers of the lakehouse

A lakehouse stores data as cheap files, like a data lake, but adds the reliability of database tables. This project organises it in the common medallion pattern, where data gets more trustworthy at each layer:

| Layer | Quality | Stored in | Refreshed | Used for |
| --- | --- | --- | --- | --- |
| **Bronze** | Raw, exactly as received | Iceberg on MinIO | Every minute | Replaying history and audits |
| **Silver** | Cleaned and validated | Iceberg on MinIO, copied to Postgres | Hourly | Source for Gold |
| **Gold** | Business-ready tables | Postgres | Daily | Reports and KPIs |

The Bronze layer is what makes mistakes recoverable. Because the raw data is kept, a bug fix in the cleaning rules can be replayed over past data.

### Where it differs from the original brief

- **Gold is in Postgres, not the lake.** The dbt project targets Postgres, and Superset queries it directly.
- **ksqlDB is used without Flink.** The brief allows either.
- **MinIO uses a Bitnami image.** The official images can no longer be pulled, and this one is frozen, so a long-lived deployment would need a replacement.

## What each tool does

Each tool has one main job, and the simplest way to remember them is by that job. All of them run as containers through Docker Compose.

### Moving events

| Tool | What it is | Its job here | Why it's used |
| --- | --- | --- | --- |
| **Apache Kafka** | A durable, ordered event log | Carries `transactions`, `clickstream` and `inventory_updates`, with one partition per region | Producers and consumers stay independent, and several consumers can read the same events |
| **Zookeeper** | A coordination service | Manages Kafka's cluster state | Required by this Kafka setup |
| **Schema Registry** | A central store of data contracts | Holds the Avro schema for each topic and refuses incompatible changes | Stops a producer from silently breaking every consumer |
| **Avro** | A compact binary data format with a schema | The format of every event on the three source topics | Enforces field names and types |

### Real-time processing

| Tool | What it is | Its job here | Why it's used |
| --- | --- | --- | --- |
| **ksqlDB** | SQL over Kafka streams | Runs the fraud and low-stock rules | Streaming logic can be written as SQL |
| **Serving sink** (custom Python) | A Kafka consumer | Copies live topics into Postgres and records pipeline lag | Grafana and the API can't query Kafka directly |

### Storing and processing the lakehouse

| Tool | What it is | Its job here | Why it's used |
| --- | --- | --- | --- |
| **Apache Spark** | A distributed data engine | Runs the Bronze stream and the Silver, extract and maintenance jobs | Handles streaming and batch with one engine |
| **MinIO** | S3-compatible object storage | Stores the lakehouse files | Gives S3-style storage on a laptop |
| **Parquet** | A columnar file format | The file format of the lakehouse tables, compressed with Snappy | Fast and compact for analytics |
| **Apache Iceberg** | A table format over files | Makes those files behave like tables | Adds `MERGE INTO`, snapshots and time travel |

### Quality and modelling

| Tool | What it is | Its job here | Why it's used |
| --- | --- | --- | --- |
| **Great Expectations** | A data validation framework | Checks Silver data against rules kept in JSON files | Bad data is measured and blocked before it spreads |
| **dbt** | SQL transformation and testing | Builds and tests the Gold tables | Models get dependencies and tests, like software |
| **PostgreSQL** | A relational database | Holds the OLTP data, Silver copy, Gold, real-time tables and monitoring tables | Superset, Grafana and dbt all query it directly |

### Orchestrating and serving

| Tool | What it is | Its job here | Why it's used |
| --- | --- | --- | --- |
| **Apache Airflow** | A workflow scheduler | Runs the 8 pipelines (DAGs) on schedule, with retries | Schedules, dependencies and run history in one place |
| **Grafana** | An operational dashboard tool | Shows live operations and pipeline health | Suited to data that refreshes constantly |
| **Apache Superset** | A business intelligence tool | Shows revenue, retention, funnel and top products | Suited to business analysis of historical data |
| **FastAPI** | A Python web framework | Serves live inventory and fraud features over REST | Lets other software use the data |
| **Docker Compose** | A multi-container runner | Starts all 24 services | One command runs the whole platform |

## The pipeline, stage by stage

The pipeline has seven stages, from generating events to showing them on a dashboard. File paths are given so you can open the code as you read.

### 1. Data generation

Code: `producer/data_generator.py`, `producer/config.py`

The simulator is built to give every later stage real work to do. Its default peak of 1.4 orders a second gives more than 50,000 orders on every day of the week, about 52,600 on a Sunday and 75,000 on a weekday. Orders are timed against a schedule, so a slow send doesn't lower the rate.

| Behaviour | How it's simulated |
| --- | --- |
| Daily and weekly patterns | Morning and evening peaks, a night-time trough and weekend dips |
| Seasonal demand | Category weights by month, such as toys peaking in December |
| Multiple currencies | USD, EUR, JPY, NGN and BRL, one per market |
| Card testing fraud | A burst of 12 to 18 small charges from one user |
| Account takeover fraud | An order of $5,001 to $15,000 under 60 seconds into a session |
| Geo anomaly fraud | An order from outside the customer's home market |
| Fraud that evades the rules | About 30% of fraud: 6 to 10 small charges spread over most of an hour, takeovers of $3,000 to $4,999, and geo anomalies routed through the customer's own market (as with a VPN) |
| Innocent orders that look like fraud | 0.3% of orders come from customers travelling abroad, and 0.1% are large purchases made straight after logging in |
| Weather | A market where it's raining (0.5 mm or more in the latest hourly reading) gets 25% more orders |
| Discount codes | 10% of orders carry a promo code, a field added in version 2 of the transactions contract |
| Bot traffic | 5% of sessions. Half reveal a bot user agent and half pretend to be a browser |
| Bad data | About 1% of orders get a fault: null or unknown user, bad amount, bad currency, or a bad timestamp |
| Duplicates | About 1% of orders are sent twice, as a retry would be |
| Stock movement | Orders, restocks, damage write-offs and audit adjustments |

On first start, a one-off service seeds 14 days of history at about 1,000 orders a day, so the batch dashboards have trends to show. Those events are flagged `is_backfill`.

### 2. Ingestion

Code: `schemas/avro/`, `producer/register_schemas.py`

Events enter Kafka in Avro format. The producer can't register new schemas itself, which is what makes the registered schema a real contract. Each region has its own partition, so consumers can process markets in parallel.

Fields that quality checks test, such as `user_id`, stay nullable on purpose. Bad values need to reach Silver so they can be counted there.

### 3. Bronze: raw storage

Code: `consumer/spark_streaming.py`, `consumer/lakehouse.py`

Spark reads each topic every 60 seconds and appends to an Iceberg table. Nothing is cleaned or dropped here. It only adds the Kafka partition, offset and ingestion time.

- **Layout:** partitioned by `region/year/month/day`, stored as Snappy-compressed Parquet.
- **Time travel:** Iceberg snapshots let you query a table as it was earlier.
- **Retention:** rows older than 90 days are copied to a cold-storage bucket, then deleted (`consumer/bronze_maintenance.py`).
- **Other source:** a daily job also pulls orders, payments and users from the OLTP database (`consumer/oltp_extract.py`).

### 4. Silver: cleaning and validation

Code: `consumer/silver_processor.py`, `quality/`

This is where data becomes trustworthy. It runs hourly, one table at a time:

1. Read Bronze rows that arrived since the last run.
2. Run Great Expectations on the raw rows, to measure how dirty they are.
3. Apply the cleaning rules below.
4. Run Great Expectations on the cleaned rows. If any check fails, stop and fail the Airflow task.
5. Merge into the Silver Iceberg table, then publish to Postgres for dbt.
6. Record row counts and results for the monitoring dashboard.

| Rule | How it works |
| --- | --- |
| Required fields | Key IDs, region and event time must be present |
| Time range | When it arrived, an event can't be over 5 minutes in the future or over 7 days old, except flagged history |
| Valid currency | Must be one of the five market currencies |
| Sensible amount | Above $0 and below $50,000 |
| Known users | `user_id` must exist in `oltp.users` |
| Bot filtering | Removes sessions with bot user agents, or with 5+ events averaging under 3 seconds apart |
| Session stitching | A user's events form one session until a gap of more than 30 minutes |
| Currency normalisation | Converts to USD using the latest hourly rate at or before the order time |
| Deduplication | Keeps the latest copy of each ID, and `MERGE INTO` makes writes idempotent |

Idempotent means running the same data twice gives the same result. It's what makes the weekly reprocessing job safe.

### 5. Gold: business tables

Code: `batch/dbt/`

dbt builds the star schema daily in Postgres, in the order staging, dimensions, facts, aggregates. It tests the Silver sources first and the Gold output last. The tables are listed in the data model section below.

### 6. Real-time rules

Code: `streaming/ksql/`, `consumer/realtime_sink.py`

| Output | Rule |
| --- | --- |
| `HIGH_VALUE_NEW_SESSION` | Over $5,000 and under 60 seconds into the session |
| `GEO_ANOMALY` | The order's location differs from the customer's home market |
| `VELOCITY_BREACH` | More than 10 orders from one user in a rolling hour |
| `restock_required` | Stock falls below the reorder point of 20 |

The sink writes these to Postgres, along with live orders and current stock. It keeps event rows for 48 hours, since long-term history lives in the lakehouse.

### 7. Serving

Code: `dashboards/`, `api/main.py`

Dashboards are stored as code, so they appear automatically when the stack starts. The next section lists what each one shows.

## Tables, schedules and dashboards

Gold is a star schema: fact tables record events, dimension tables describe them, and aggregates hold ready-made KPIs.

### Gold tables

| Table | Type | One row per | Answers |
| --- | --- | --- | --- |
| `dim_customers` | Dimension | Customer | Who are our most valuable and most at-risk customers? |
| `dim_products` | Dimension | Product | What sells best, and how does it rank in its category? |
| `dim_dates` | Dimension | Calendar day | Weekends, seasons and shopping events |
| `dim_regions` | Dimension | Market | Currency, timezone and totals per market |
| `fct_orders` | Fact | Order | Every non-failed order, linked to all four dimensions |
| `fct_sessions` | Fact | Visitor session | How long visitors stayed and how far they got |
| `fct_inventory_snapshots` | Fact | Product, warehouse and day | Stock on hand, demand and days of cover |
| `agg_daily_revenue` | Aggregate | Region and day | How much did we earn? |
| `agg_fraud_summary` | Aggregate | Region and day | How much revenue is at risk from fraud? |
| `agg_inventory_health` | Aggregate | Warehouse and day | Which warehouses are at risk of running out? |
| `agg_funnel_conversion` | Aggregate | Region and day | Where do visitors drop out before buying? |
| `agg_customer_retention` | Aggregate | Weekly cohort and week | Do customers come back? |
| `agg_weather_demand` | Aggregate | Market and hour | Does rain change demand? |

### Where data lives in Postgres

One Postgres instance holds all of these, which keeps memory use practical on one machine.

| Schema | Holds | Written by | Read by |
| --- | --- | --- | --- |
| `oltp` | Users, products, orders, payments | Simulator | Spark extract, Silver user check |
| `silver` | Clean data, FX rates, weather, stock snapshots | Silver job, Airflow tasks | dbt, Silver FX lookup |
| `gold` | Star schema and KPIs | dbt | Superset |
| `realtime` | Live orders, alerts, stock, pipeline lag | Serving sink | Grafana, REST API |
| `observability` | Quality results, row counts, freshness, schema changes | Silver job, monitor DAG | Grafana |
| `public` | Airflow's own tables, Iceberg catalog | Airflow, Spark | Grafana, Spark |

### Airflow schedules

| DAG | Runs (UTC) | Purpose |
| --- | --- | --- |
| `observability_monitor` | Every 15 minutes | Records row counts and freshness, detects schema changes and volume drops, and scores the fraud rules |
| `silver_pipeline` | Hourly | Bronze to Silver |
| `fx_rates_refresh` | Hourly | Appends exchange rates from open.er-api.com |
| `external_sources` | Hourly at :05 | Fetches weather from Open-Meteo, and stock from the inventory API |
| `oltp_daily_batch` | Daily 00:30 | Extracts orders, payments and users into Bronze |
| `gold_dbt_pipeline` | When Silver is updated | Builds and tests Gold in `gold_next`, then publishes it to `gold` |
| `bronze_maintenance` | Daily 04:00 | Archives after 90 days, compacts files, expires old snapshots, prunes the dead-letter table |
| `housekeeping` | Daily 04:30 | Prunes old monitoring samples, audit rows and Airflow's own history |
| `historical_backfill` | Sundays 02:00 | Replays 7 days through Silver, then rebuilds Gold |

### Dashboards and API

| Where | Name | Shows |
| --- | --- | --- |
| Grafana | Real-Time Operations | Live transactions by region, fraud alert feed, inventory risk heatmap, pipeline lag |
| Grafana | Pipeline Observability | DAG success rates, row counts, freshness, quality results, schema changes |
| Superset | Business Intelligence | Revenue trends, retention cohorts, conversion funnel, top products |
| REST API | `/inventory/stock`, `/inventory/low-stock` | Current stock and items below their reorder point |
| REST API | `/features/fraud/{user_id}` | A customer's order velocity, spend, regions and recent alerts |

## Engineering decisions worth explaining

If you can explain why each of these choices was made and what it costs, you understand the project.

### Problems that blocked the original build

| Problem | Cause | Fix |
| --- | --- | --- |
| The Silver DAG wouldn't import | The Spark provider and `pyspark` weren't installed, and `master=` isn't a valid argument | Installed both and removed the argument |
| Python version mismatch | The driver ran on Airflow's Python and workers on the Spark image's | One shared image for Airflow and Spark |
| The batch job would wait forever | A streaming job takes every Spark core by default | Capped Bronze at 1 of 3 cores, and allowed one batch job at a time through an Airflow pool |
| Executors couldn't reach the driver | The driver ran inside the scheduler container without a resolvable name | Set `spark.driver.host` to the scheduler's service name |
| Empty MinIO credentials | They came from Airflow Variables that were never set | Read them from the container's environment |
| MinIO images unavailable | `minio/minio` and `minio/mc` can no longer be pulled | Switched to a pinned `bitnamilegacy/minio` image |

### Design choices

| Decision | Why | The trade-off |
| --- | --- | --- |
| Two paths, speed and batch | Alerts need speed and reports need accuracy | The same rules exist in ksqlDB and dbt, so they can drift apart |
| Iceberg catalog in Postgres | Iceberg's Hadoop catalog needs atomic rename, which S3-style storage lacks | Adds another dependency on Postgres |
| Gold in Postgres, not the lake | dbt, Superset and Grafana query Postgres directly | Differs from the brief's Gold zone, and wouldn't scale like a lakehouse engine |
| Watermark on ingestion time | Tracking event time would skip events that arrive late | Depends on Bronze recording arrival time |
| Two quality checks per table | The raw check measures dirt, and the clean check blocks bad writes | Each run takes longer |
| Bots detected, not labelled | A simulator label would make the filter meaningless | Rate rules can misjudge real users |
| Stable session IDs | A hash of user and session start stays the same across runs | Each run must also read two hours of earlier events |
| FX rates kept as history | Orders convert at the rate when they were placed | Old orders need fixed fallback rates |
| Velocity alert as a separate query | ksqlDB can't join a stream to a windowed table | One breach can emit several times, so the sink removes repeats |

### Bugs that only a real run exposed

These all passed code review and unit tests, and only showed up when the system ran. They make good stories, because each has a clear symptom, cause and fix.

| Symptom | Cause | Fix |
| --- | --- | --- |
| Iceberg Silver held about twice its real rows, while Postgres looked correct | Consistent with a Spark 3.5.0 bug (SPARK-45282): a join on cached data can lose matches, so `MERGE INTO` kept rows and inserted them again | Turn off `canChangeCachedPlanOutputPartitioning`, and fail the job if a merge leaves duplicate keys |
| A replay would have rejected rows that passed the first time | The 7-day rule compared event time with the current time | Compare with arrival time, so the answer never changes |
| Retries could duplicate rows in the dead-letter table | It was a plain append | Use an ID from the Kafka position, and `MERGE` |
| Gold failed only on a fresh install | `dbt build --select dimensions` also ran foreign key tests from `fct_orders`, which didn't exist yet | `--indirect-selection buildable`, and CI now builds layer by layer on empty tables |
| 68 orders had no customer key | Silver added orders between the dimension and fact builds, so their customers weren't in the dimension yet (a late-arriving dimension). A new `dbt build` test caught it | Hold those orders with an inner join. The 3-day window loads them on the next run |
| A fresh install's read-only login would fail | Init scripts run alphabetically, so the grant ran before `silver_schema.sql` | Create the schema first, and run the scripts in order in CI |
| Gold stayed empty forever, while all 70 tests passed | On an empty table, `max()` is `NULL`, so the incremental filter matched nothing | `coalesce` the filter, and fail the DAG if Gold is empty while Silver has data |
| Fresh events were rejected as being in the future, then lost | Age was measured from the job's start, but earlier tables take minutes, and the watermark moved past the rejected rows | Measure age when each table is read |
| Live revenue was inflated by about $2M | The sink filtered bad timestamps but not injected $999,999 amounts | Apply Silver's amount bounds in the sink |
| Retention always aborted | The default serializable isolation treats stream appends as a conflict | Snapshot isolation for the delete |
| ksqlDB timed out writing its command topic | Kafka's transaction log defaults need 3 brokers | Set both to 1 for a single broker |
| Some card testing bursts were missed | `SET` is per session, so one query started from `latest` | Set `earliest` in that file too |
| Superset failed only on a fresh install | Postgres's temporary init server passed a socket healthcheck | Check health over TCP |
| The whole stack stalled | Healthchecks started a JVM or Airflow process every few seconds, on top of high memory limits | Port checks, one webserver worker, lower heaps |
| The sink crashed 12 times during a stall | A metric's Kafka timeout killed the service | Catch it and skip that sample |

### Bugs found in the original code

- **Frozen aggregates:** `agg_*` models used `> max(date)`, so today's partial figures were never updated.
- **Resetting stock:** the running stock total restarted on every incremental dbt run.
- **Tests that tested nothing:** `dbt test --select gold` matched no models.
- **Invalid test syntax:** the `arguments:` form isn't valid in dbt 1.7.
- **Undervalued orders:** prices were set in local units, so a naira order was worth a few dollars.
- **Future timestamps:** clickstream sessions contained events dated ahead of the current time.
- **Crash in backfill:** `backfill()` passed a `verbose=` argument that `emit()` doesn't accept.

## How the core concepts work

Each concept below is explained in general first, then in how this project uses it. If you understand these, most questions about the project answer themselves.

### Kafka: topics, partitions and offsets

Kafka is an append-only log. A **topic** is a named log, split into **partitions**, and each message in a partition has a numbered position called an **offset**. Consumers track their own offset, so reading a message doesn't delete it and many consumers can read the same data.

- **Ordering** is only guaranteed within a partition.
- **Consumer groups** share a topic's partitions between members, so adding consumers adds throughput.
- **Here:** 5 partitions per topic, one per region, set explicitly through `REGION_PARTITIONS`. Hashing the key could have put two regions on one partition.
- **Here:** ksqlDB, Spark and the serving sink all read the same topics independently.

### Avro and Schema Registry

Avro encodes data in binary using a schema, so messages are small and fields have fixed types. Schema Registry stores each schema with an ID. Each message starts with a 5-byte header, a marker byte plus that 4-byte ID, followed by the Avro data.

- **Here:** Bronze reads the 4-byte ID from that header and decodes each message with the version it names, then resolves it to the latest version. That's how old and new events share one table.
- **BACKWARD compatibility** means consumers on a new schema can read old data. Adding a field needs a default value.
- **`auto.register.schemas: False`** stops producers from changing the contract by accident. Only `register_schemas.py` registers schemas.

### Spark Structured Streaming

Spark treats a stream as a table that keeps growing and processes it in small **micro-batches**. A **checkpoint** records which offsets have been processed, so a restart continues where it stopped.

- **Here:** a 60-second trigger on three separate streams, one per topic.
- **Here:** checkpoints are in MinIO, so they survive a container restart. The original `/tmp` checkpoints wouldn't have.
- **Driver and executors:** the driver plans the work and executors run it. Executors must connect back to the driver, which is why `spark.driver.host` is set.
- **Client mode:** the driver runs where `spark-submit` was called. For batch jobs that's the Airflow scheduler container.

### Apache Iceberg

A plain folder of Parquet files has no transactions, so a reader can see half-finished writes and updates are hard. Iceberg adds a metadata layer that tracks exactly which files belong to a table.

- **Snapshots:** each commit creates a new version of the table, which is what makes time travel possible.
- **`MERGE INTO`:** updates rows that match a key and inserts the rest, so rerunning a batch doesn't create duplicates.
- **Catalog:** records which metadata file is current. Swapping that pointer is the atomic commit, and this project stores it in Postgres.
- **Maintenance:** one-minute commits create many small files and snapshots, so `bronze_maintenance` compacts files and expires old snapshots.

### ksqlDB: streams, tables and windows

ksqlDB runs continuous SQL queries over Kafka. A **stream** is a series of events, and a **table** holds the latest value per key. A persistent query keeps running and writes its results to a new topic.

- **Hopping window:** `SIZE 1 HOUR, ADVANCE BY 5 MINUTES` means overlapping one-hour windows starting every 5 minutes. That's how a rolling hourly count works.
- **Here:** one breach falls into several overlapping windows, which can produce repeat alerts. The sink's primary key removes them.
- **Here:** `HAVING COUNT(*) > 10` makes the table emit only breaches.

### Star schema and dbt

A star schema puts measurable events in a central **fact** table, linked to descriptive **dimension** tables. Analysts can then slice orders by customer, product, date or region with simple joins.

dbt turns `SELECT` statements into tables and works out the build order from `ref()`.

- **Materialisations:** staging models are views, dimensions are rebuilt as tables, and facts and most aggregates are incremental.
- **Incremental:** only recent data is reprocessed. The `is_incremental()` blocks choose the window, and `unique_key` merges changes in.
- **Surrogate key:** a hash that identifies each row, from `dbt_utils.generate_surrogate_key`.
- **Tests:** rules such as `unique`, `not_null`, `accepted_values` and `relationships`, run as queries that should return no rows.

### Airflow

A **DAG** is a workflow of tasks with dependencies, written in Python. The scheduler starts tasks on schedule, and an **executor** decides where they run.

- **Here:** `LocalExecutor` runs tasks as processes inside the scheduler container.
- **Pool:** `spark_batch` has one slot, so only one Spark batch job runs at a time.
- **`catchup=False`:** Airflow doesn't run every missed interval after downtime.
- **Idempotency:** retries and replays are safe, because the writes they repeat are upserts.

### Watermarks

A watermark records how far a batch job has read, so the next run only handles new rows. Silver stores it in MinIO as the latest `_ingested_at` value per table, and moves it only after a successful write. That gives **at-least-once** processing, and `MERGE INTO` makes a repeat harmless.

## The code, file by file

Read the files in this order, because it follows the data from source to dashboard. The functions listed are the ones worth opening first.

### Producing events

| File | What it does | Look at |
| --- | --- | --- |
| `producer/config.py` | Markets, currencies, warehouses, product catalogue and demand weights | `REGION_PARTITIONS`, `USER_HOME_REGION` |
| `producer/data_generator.py` | Generates all events and sends them to Kafka and Postgres | `generate_transaction`, `generate_fraud_scenario`, `corrupt_transaction`, `generate_session`, `StockBook`, `EventPublisher`, `OltpWriter`, `backfill` |
| `producer/register_schemas.py` | Registers the Avro contracts and sets BACKWARD compatibility | `main` |
| `schemas/avro/*.avsc` | The field contract for each topic | `transactions-value.avsc` |

### Bronze and Silver

| File | What it does | Look at |
| --- | --- | --- |
| `consumer/lakehouse.py` | Shared Spark session, Iceberg catalog settings and the merge helper | `create_spark_session`, `merge_into` |
| `consumer/spark_streaming.py` | Streams Kafka into Bronze | `read_topic`, `ensure_table` |
| `consumer/silver_processor.py` | Cleans and validates Bronze into Silver | `process_table`, `filter_bots`, `stitch_sessions`, `normalise_currency`, `write_silver` |
| `quality/validation.py` | Runs a table's Great Expectations suite | `validate` |
| `quality/expectations/*.json` | The validation rules | `transactions.json` |
| `consumer/oltp_extract.py` | Daily pull from the transactional database | `load` |
| `consumer/bronze_maintenance.py` | Retention, compaction and snapshot expiry | `archive_expired` |

`process_table` is the most important function in the project. It contains the whole Silver sequence in one place, so start there.

### Real-time

| File | What it does | Look at |
| --- | --- | --- |
| `streaming/ksql/streamcart.sql` | Defines streams, the velocity table and the alert rules | `fraud_alerts`, `user_velocity_1h`, `restock_required` |
| `streaming/ksql/velocity_insert.sql` | Adds velocity breaches to `fraud_alerts` | The `INSERT INTO` query |
| `streaming/ksql/apply.sh` | Applies the SQL, running the insert only once | The `SHOW QUERIES` guard |
| `consumer/realtime_sink.py` | Copies live topics into Postgres | `to_row`, `flush`, `sample_lag` |
| `api/main.py` | The REST endpoints | `fraud_features`, `low_stock` |

### Orchestration and modelling

| File | What it does | Look at |
| --- | --- | --- |
| `batch/dags/common.py` | Spark settings shared by DAGs | `SPARK_CONF` |
| `batch/dags/*_dag.py` | The 8 DAGs | `silver_pipeline_dag.py`, `gold_dbt_pipeline_dag.py` |
| `batch/dags/observability_monitor_dag.py` | Row counts, freshness and schema changes | `detect_schema_changes` |
| `batch/dbt/models/staging/` | Views that rename and cast Silver columns | `stg_transactions.sql` |
| `batch/dbt/models/dimensions/`, `facts/`, `aggregates/` | The Gold tables | `fct_orders.sql`, `agg_fraud_summary.sql` |
| `batch/dbt/models/schema.yml` | dbt tests for Gold | The `fct_orders` section |
| `batch/db/migrations/` | Flyway migrations for the Postgres schema | `V1__oltp_schema.sql`, `V5__readonly_role.sql` |

### Infrastructure and dashboards

| File | What it does |
| --- | --- |
| `Dockerfile` | Builds the shared image: Airflow, PySpark, connector jars, and dbt in its own virtualenv |
| `docker-compose.yml` | Defines the 24 services and their start order |
| `wait-for-postgres-and-init.sh` | Prepares Airflow's database, admin user and Spark pool |
| `.env.example` | Template for the local settings |
| `dashboards/grafana/` | Grafana datasource and dashboard JSON |
| `dashboards/superset/build_bundle.py` | Generates the Superset database, datasets, charts and dashboard |
| `tests/` | Simulator, Silver and DAG tests |

### Service start order

One-shot services run and exit, and later services wait for them:

1. Postgres, MinIO and Zookeeper start, then Kafka.
2. `kafka-init` creates the topics.
3. Schema Registry starts, then `schema-init` registers the contracts.
4. `data-seed` loads history, and `data-generator` starts streaming.
5. ksqlDB starts, `ksqldb-init` applies the SQL, then `realtime-sink` starts.
6. `airflow-init` runs, then the Airflow webserver and scheduler start.
7. The Spark master and worker start, then the Bronze job.
8. The API and Grafana start, and `superset-init` runs before Superset.

## What happens when something fails

Most failures recover by replaying data, because Kafka and Bronze keep it and the writes are upserts. Postgres is the main exception, since so much depends on it. The Silver gate, Bronze restart, Kafka restart and host sleep were tested live. The rest describe the design.

| What fails | What happens | How it recovers |
| --- | --- | --- |
| A producer sends an event that breaks the contract | Serialisation fails, the generator exits and Docker restarts it | Fix the code or contract |
| Bronze streaming job crashes | Nothing is written to Bronze, but events wait in Kafka for 7 days | Docker restarts it, and it resumes from its checkpoint |
| Silver crashes mid-run | The watermark doesn't move | The next run rereads those rows, and `MERGE INTO` absorbs the repeats |
| Silver quality gate fails | That table isn't written, its watermark stays put and the task fails, but other tables continue | Retries twice, and it stays blocked until the data or rule is fixed |
| Postgres publish fails after the Iceberg merge | The upsert rolls back and the watermark doesn't move | The next run repeats the merge and the publish |
| Airflow scheduler stops | Batch jobs stop, but Kafka, Bronze and the speed path continue | `catchup=False` skips missed intervals, and the watermark means the next Silver run reads everything new |
| FX rate API is down | `fx_rates_refresh` fails and retries 3 times | Silver keeps using the latest stored rate |
| dbt source tests fail | Gold isn't rebuilt, so yesterday's tables stay in place | Fix Silver, then rerun the DAG |
| dbt Gold tests fail | Nothing is published: the build stays in `gold_next`, and readers keep the last good Gold. The failure feed records it, and the webhook alerts | Fix the model, inspect `gold_next`, and rerun |
| ksqlDB restarts | Alerts pause | Queries resume from their Kafka offsets |
| Serving sink crashes | Dashboards go stale | Docker restarts it and it replays from its last commit, with upserts removing repeats |
| MinIO is down | Bronze, Silver and maintenance fail, but the speed path continues | Jobs recover through their checkpoint or watermark |
| Postgres is down | Almost everything stops: Airflow, the Iceberg catalog, dbt, the sink and the dashboards | Kafka keeps events, and jobs catch up after it returns. This is the biggest single point of failure |

### Known limitations

- **One of everything:** one Kafka broker, one Spark worker and one Postgres, so nothing is highly available.
- **Simple fraud rules:** fixed thresholds, scored against hidden labels. They miss the fraud built to evade them and flag some innocent orders, and no model has been trained. The API only serves features.
- **Simulator state:** the stock ledger is held in memory, so stock levels reset when the generator restarts.
- **Tied together:** the simulator writes to both Kafka and Postgres without a transaction, so they could briefly disagree.
- **Heavy for one machine:** the full stack needs about 8 GB for Docker, so `COMPOSE_PROFILES` can run one layer at a time.
- **Local credentials:** `.env.example` has development passwords. Postgres, MinIO and Kafka give each service its own limited login, but nothing is encrypted in transit inside the Docker network.
- **Simulated effects:** weather really is fetched, but its effect on demand is built into the simulator, so `agg_weather_demand` shows that the pipeline can measure an effect, not that rain changes real demand.
- **Inventory source isn't independent:** the inventory REST pull reads this project's own API, which serves stock the simulator streamed, so it can't catch errors in the stream.

## Questions to test yourself

Try to answer each question aloud before reading the answer. If you can't, go back to that topic and read the code.

### The big picture

| Question | Answer |
| --- | --- |
| What does the project do in one sentence? | It turns a simulated shop's events into live fraud and stock alerts, and into validated analytics tables |
| Why two paths rather than one? | Alerts need seconds and reports need accuracy, and one pipeline optimises badly for both |
| Is any of the data real? | No. Orders, clicks and stock are simulated. FX rates and weather come from real public APIs |
| How does weather affect the data? | The simulator gives a market 25% more orders while it's raining there, and `agg_weather_demand` measures that against the usual demand for the same hour of day |
| What is a lakehouse? | Cheap file storage with database features such as transactions and updates, here from Iceberg on MinIO |
| Why keep raw data in Bronze? | So you can replay it through corrected rules, and audit what actually arrived |
| Why does Gold live in Postgres? | dbt targets Postgres, and Superset queries it directly. It's simple, but wouldn't scale like a lake engine |

### Streaming

| Question | Answer |
| --- | --- |
| Why partition by region? | Markets can be processed in parallel, and order is kept within each market |
| How is a partition chosen? | Explicitly, from `REGION_PARTITIONS`, because hashing could send two regions to one partition |
| What stops a producer from breaking consumers? | Avro contracts, BACKWARD compatibility, and producers being unable to register schemas |
| Why are `user_id` and `amount_usd` nullable? | So bad values reach Silver, where they can be counted and rejected |
| How does velocity detection work? | A hopping one-hour window counts orders per user, and `HAVING COUNT(*) > 10` emits the breaches |
| Why isn't velocity in the main fraud `CASE`? | ksqlDB can't join a stream to a windowed table, so it's inserted by a separate query |
| How do repeat alerts get removed? | The sink's primary key on transaction and signal ignores repeats |
| Why is there a sink at all? | Grafana and the API query Postgres, not Kafka |
| How does seed data avoid raising alerts? | `transactions_live` filters out `IS_BACKFILL = true` |

### Batch and quality

| Question | Answer |
| --- | --- |
| How are duplicates handled? | Window deduplication by ID within a batch, then `MERGE INTO` on the primary key across batches |
| What does idempotent mean here? | Reprocessing the same rows leaves the table unchanged |
| Why track watermarks on ingestion time? | Tracking event time would skip late arrivals |
| Where do rejected rows go? | `lake.silver.rejected_rows`, as JSON, so they can be inspected or replayed |
| How do you know no orders went missing? | A daily reconciliation compares `oltp.orders` with Silver |
| What is a late-arriving dimension? | A fact that arrives before its dimension row. Here, those orders wait for the next run |
| Why do dashboards have their own database login? | Least privilege: a dashboard or API bug can't change data |
| Why doesn't the pipeline use the Postgres superuser? | A bug or a leaked password in Airflow or Spark could otherwise drop any database or create logins. The application role can only read and write the pipeline's schemas |
| What happens when a Kafka contract gets a new version? | Bronze decodes each event with the version in its header and adds new fields as columns. A version registered while it runs stops the job once, and the restart loads it |
| Where is each business rule defined? | In `quality/rules.py`. Copies that can't import Python (ksqlDB, dbt, Great Expectations) are checked against it by a test |
| How do you know the fraud rules work? | The monitor scores precision and recall against hidden labels. The test set includes fraud built to evade the rules and innocent orders that look like fraud, so the scores aren't flattering |
| Why convert to local time in Gold, not Silver? | Silver stays in UTC, which is unambiguous. A report about a market's day needs that market's calendar |
| How does Gold know Silver has finished? | The Silver task updates an Airflow Dataset, and Gold is scheduled on that Dataset |
| How is the database schema changed? | With a new Flyway migration, which runs once on every database, old or new |
| How would you notice that data stopped arriving? | The volume check compares each day with the median of the week before, and freshness checks warn too |
| Why validate twice? | The raw check measures input quality, and the clean check is the gate that blocks writes |
| How is a bot detected? | A bot user agent, or 5+ events in a session averaging under 3 seconds apart |
| Why not use a bot label from the simulator? | The filter would prove nothing, and real traffic has no such label |
| How do session IDs stay stable? | They hash the user and the session's first event time, using two hours of earlier events for context |
| Which FX rate is used? | The latest hourly rate at or before the order, or a fixed fallback if none is that old |
| What does `fx_discrepancy` mean? | The reported USD amount differs from the recalculated one by more than 1% |
| How is a user checked as real? | A broadcast join against `oltp.users` |
| Why are old events rejected, but seed data isn't? | Seed rows are flagged `is_backfill`, which leaves their event age null |
| Why measure age against arrival time? | The answer never changes, so replaying old data gives the same result as the first run |
| How do you know the merge really is idempotent? | The job checks that rows equal distinct keys after every merge. Checking only Postgres once hid a real duplicate bug in Iceberg |
| Why does Silver write to Iceberg and Postgres? | Iceberg is the lakehouse copy, and Postgres is the copy dbt builds from |

### Modelling and orchestration

| Question | Answer |
| --- | --- |
| Fact versus dimension? | Facts record events and measures, and dimensions describe them |
| What is the grain of `fct_orders`? | One row per non-failed order |
| Why reprocess recent days in aggregates? | Today's figures are partial, so a `> max(date)` filter would freeze them |
| Why does `fct_inventory_snapshots` read full history? | Its 7-day demand window needs earlier rows, and only recent days are merged |
| What is a retention cohort? | Customers grouped by the week of their first order, tracked across later weeks |
| Why run dbt per layer, not per model? | Concurrent dbt runs would conflict over `target/`, and dbt already orders models within a layer |
| Why one shared Docker image? | The driver and executors must share Python and Spark versions |
| Why is Bronze capped at 1 core? | Otherwise it takes all 3, and batch jobs wait forever for resources |
| What does the `spark_batch` pool do? | Allows one Spark batch job at a time on the single worker |
| What triggers Gold? | An update to Silver, from the hourly job or the weekly backfill (paused on a fresh install) |
| How would you find out why a machine is slow? | Check the VM's memory, swap and pressure stall figures, then the biggest containers, before changing anything |
| How are schema changes detected? | `observability_monitor` compares warehouse columns and Kafka contract versions against the last saved state |

### Judgement

| Question | Answer |
| --- | --- |
| What's the weakest part? | Postgres does too many jobs, and every core component is a single instance |
| What would you change for production? | Separate databases, a replicated Kafka, managed object storage, lake-based Gold, and alerts on failures |
| How would it handle far more data? | Add Kafka partitions and Spark workers, and move Gold off Postgres to a lake query engine |
| How would you improve fraud detection? | Train a model on the API's features, using the simulator's `_is_fraud` labels, and compare its precision and recall with the rules' scores in `observability.fraud_evaluation` |
| How do you stop bad Gold tables being seen? | Write, audit, publish: dbt builds and tests in `gold_next`, and one transaction moves every table into `gold` only if all the tests passed |
| Why copy Gold into `gold_next` first? | Incremental models build on their existing rows. At this size a copy is cheap; a large warehouse would use a zero-copy clone |
| Why move tables, not rename the schema? | Renaming a schema needs CREATE on the whole database, which the application login deliberately doesn't have |
| Who hears about a failure? | It's recorded for Grafana, and posted to a Slack or Mattermost webhook when `ALERT_WEBHOOK_URL` is set |
| How is Kafka secured? | Each service logs in with its own SASL user, and ACLs allow only what it uses: the simulator can write the source topics but not read them, Spark can read them but not write, and so on. Only `kafka-init` uses the admin login |
| What can Spark do in MinIO? | Read, write and delete objects in the lakehouse and archive buckets. It can't create or delete buckets or manage users |
| Why not Flink? | The brief allows either, and ksqlDB expresses these rules in SQL. Flink would suit more complex stateful logic |

## Learning it hands-on

These experiments will teach you more than rereading will, because you'll see each behaviour happen. Each one tests a claim made in this guide, and running them yourself is the best way to make the project your own.

- [ ] **See an event in Kafka.** In the ksqlDB CLI, run `PRINT 'transactions' FROM BEGINNING LIMIT 3;`. Find the region and the partition.
- [ ] **Watch fraud alerts arrive.** Run `SELECT * FROM fraud_alerts EMIT CHANGES;` in the ksqlDB CLI. Match each signal to its rule.
- [ ] **Count what Silver rejected.** Query `observability.silver_runs`, then `observability.quality_results`, and name the faults that were caught.
- [ ] **Prove idempotency.** Count `silver.transactions`, trigger `historical_backfill`, then count again. Rows already there shouldn't be duplicated.
- [ ] **Crash the Bronze stream.** Run `docker compose restart spark-bronze-job` and confirm no Kafka offsets are missed, using `_kafka_offset` in Bronze.
- [ ] **Trip the quality gate.** Temporarily tighten a rule in `quality/expectations/transactions.json`. Confirm the task fails, nothing is written and the next run retries those rows.
- [ ] **Break a contract.** Remove a required field from a schema in `schemas/avro` and run `docker compose up schema-init`. The registry should refuse it.
- [ ] **Use time travel.** Query `lake.bronze.transactions` with `TIMESTAMP AS OF` an earlier time, and compare the count.
- [ ] **Follow one order.** Pick a `transaction_id` and find it in `realtime.transactions`, `silver.transactions` and `gold.fct_orders`.
- [ ] **Starve Spark on purpose.** Remove `spark.cores.max=1` from the Bronze job and watch the Silver job hang. That recreates part of the original blocker.

The exact commands for common tasks are in [operations.md](operations.md).
