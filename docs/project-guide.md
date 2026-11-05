# StreamCart: Project Guide

A full explanation of what StreamCart does, how the pipeline works, and what each tool is for.

## Read this first: the status of the project

StreamCart is fully written but has never run from start to finish, so it isn't ready to list on a CV as working. Everything below describes what the code is designed to do. Treat each behaviour as a design until the end-to-end run confirms it.

| Part | State |
| --- | --- |
| Code for all 9 requirement modules | Written and pushed |
| `docker compose config` | Passes |
| Platform image build | Built before two unused packages were removed; rebuild not confirmed |
| Automated tests | Written, not yet run |
| Pipeline, dashboards and fraud alerts | Not yet run |

Some failures are likely on the first run, because this much new code hasn't been executed yet.

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

1. At 03:00 UTC, dbt runs its source tests, then rebuilds the Gold tables.
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
| **Docker Compose** | A multi-container runner | Starts all 22 services | One command runs the whole platform |

## The pipeline, stage by stage

The pipeline has seven stages, from generating events to showing them on a dashboard. File paths are given so you can open the code as you read.

### 1. Data generation

Code: `producer/data_generator.py`, `producer/config.py`

The simulator is built to give every later stage real work to do. At its default rate it's designed to produce more than 50,000 orders a day.

| Behaviour | How it's simulated |
| --- | --- |
| Daily and weekly patterns | Morning and evening peaks, a night-time trough and weekend dips |
| Seasonal demand | Category weights by month, such as toys peaking in December |
| Multiple currencies | USD, EUR, JPY, NGN and BRL, one per market |
| Card testing fraud | A burst of 12 to 18 small charges from one user |
| Account takeover fraud | An order of $5,001 to $15,000 under 60 seconds into a session |
| Geo anomaly fraud | An order from outside the customer's home market |
| Bot traffic | 5% of sessions. Half reveal a bot user agent and half pretend to be a browser |
| Bad data | About 1% of orders get a fault: null or unknown user, bad amount, bad currency, or a bad timestamp |
| Duplicates | About 1% of orders are sent twice, as a retry would be |
| Stock movement | Orders, restocks, damage write-offs and audit adjustments |

On first start, a one-off service seeds 14 days of history at about 2,000 orders a day, so the batch dashboards have trends to show. Those events are flagged `is_backfill`.

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
| Time range | No events over 5 minutes in the future or older than 7 days, except flagged history |
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
| `observability_monitor` | Every 15 minutes | Records row counts and freshness, and detects schema changes |
| `silver_pipeline` | Hourly | Bronze to Silver |
| `fx_rates_refresh` | Hourly | Appends exchange rates from open.er-api.com |
| `external_sources` | Hourly at :05 | Fetches weather from Open-Meteo, and stock from the inventory API |
| `oltp_daily_batch` | Daily 00:30 | Extracts orders, payments and users into Bronze |
| `gold_dbt_pipeline` | Daily 03:00 | Builds and tests Gold |
| `bronze_maintenance` | Daily 04:00 | Archives after 90 days, compacts files, expires old snapshots |
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

These decisions are what separate this project from a tutorial, so learn the reasoning behind each. An interviewer will care more about why than what.

### Problems that blocked the original build

| Problem | Cause | Fix |
| --- | --- | --- |
| The Silver DAG wouldn't import | The Spark provider and `pyspark` weren't installed, and `master=` isn't a valid argument | Installed both and removed the argument |
| Python version mismatch | The driver ran on Airflow's Python and workers on the Spark image's | One shared image for Airflow and Spark |
| The batch job would wait forever | A streaming job takes every Spark core by default | Capped Bronze at 2 of 4 cores, and allowed one batch job at a time through an Airflow pool |
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

### Bugs found in the original code

- **Frozen aggregates:** `agg_*` models used `> max(date)`, so today's partial figures were never updated.
- **Resetting stock:** the running stock total restarted on every incremental dbt run.
- **Tests that tested nothing:** `dbt test --select gold` matched no models.
- **Invalid test syntax:** the `arguments:` form isn't valid in dbt 1.7.
- **Undervalued orders:** prices were set in local units, so a naira order was worth a few dollars.
- **Future timestamps:** clickstream sessions contained events dated ahead of the current time.
- **Crash in backfill:** `backfill()` passed a `verbose=` argument that `emit()` doesn't accept.

## Using this on your CV

Use these bullets only after the end-to-end run works, and change any that the run doesn't confirm. An interviewer can ask you to start the project or open a dashboard.

### Before you list it

- [ ] Rebuild the image and pass the automated tests
- [ ] Run the full pipeline from Kafka to Superset
- [ ] See real fraud alerts in Grafana
- [ ] Add dashboard screenshots to the README
- [ ] Replace "Not yet run" in the README's status table
- [ ] Merge `feat/complete-platform` into `main`, since `main` is what visitors see first
- [ ] Be able to explain every section of this guide without notes

### Draft CV entry

**StreamCart: Real-Time E-Commerce Data Lakehouse** | Kafka, Spark, Iceberg, ksqlDB, Airflow, dbt, Great Expectations, PostgreSQL, Grafana, Superset, Docker

- Built a data lakehouse serving real-time fraud and inventory alerts and batch analytics from one Kafka event stream, across 5 simulated markets.
- Implemented Bronze, Silver and Gold layers using Spark Structured Streaming and Apache Iceberg on MinIO, with idempotent `MERGE INTO` writes and time travel.
- Wrote ksqlDB streaming rules that detect high-value, velocity and geographic fraud signals, and publish low-stock alerts to Kafka.
- Enforced data quality with Great Expectations gates in Silver, Avro contracts in Schema Registry, and dbt tests including foreign key checks.
- Modelled a dbt star schema of 4 dimensions, 3 facts and 5 KPI aggregates, covering revenue, fraud, inventory, funnel and retention.
- Orchestrated 8 Airflow DAGs for ingestion, reprocessing, 90-day retention and monitoring, tracking row counts, freshness and schema changes in Grafana.

Add real measurements once you have them, such as alert latency or rows processed. Only use numbers you've observed, and describe the data as simulated.

### Likely interview questions

| Question | The core of the answer |
| --- | --- |
| Why two paths? | Fraud alerts need seconds and reports need accuracy, and one pipeline optimises badly for both |
| What does Iceberg add over plain Parquet? | Transactions, `MERGE INTO`, snapshots and schema evolution |
| How do you avoid duplicates? | Deduplicate by ID, then `MERGE INTO` on the primary key, so replays change nothing |
| What happens if bad data arrives? | Rules remove it, the counts are recorded, and a failed gate blocks the write and fails the task |
| What happens if a Silver run fails? | The watermark doesn't move, so the next run retries the same rows |
| How do you handle late events? | The watermark uses arrival time, so they're picked up, and the 7-day rule decides if they're accepted |
| Why partition Kafka by region? | Markets can be consumed in parallel and order is kept per region |
| What are the weak points? | One Postgres and one Spark worker are single points of failure, and fraud rules are duplicated across layers |
| What would you change for production? | Separate databases, replicated Kafka, a managed object store, lake-based Gold, and alerting on failures |
