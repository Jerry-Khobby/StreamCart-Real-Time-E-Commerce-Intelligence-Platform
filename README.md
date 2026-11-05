# StreamCart: Real-Time E-Commerce Intelligence Platform

A data lakehouse for a simulated global e-commerce company. One event stream serves two needs: fraud and low-stock alerts within seconds, and validated historical analytics for the business.

**Stack:** Kafka, Schema Registry, ksqlDB, Spark Structured Streaming, Apache Iceberg, MinIO, Airflow, dbt, Great Expectations, PostgreSQL, Grafana, Superset, FastAPI, Docker Compose.

> **Project status:** all modules are implemented, but the full stack has not yet completed an end-to-end run. See [Project status](#project-status) for what has been checked.

## The problem

The business has three pain points, all caused by siloed, slow data:

| Pain point | How StreamCart addresses it |
| --- | --- |
| Fraud is caught after the money has gone | ksqlDB evaluates fraud rules on every order as it arrives |
| Warehouses restock from yesterday's numbers | Low-stock events are published to a Kafka topic and shown live in Grafana |
| Teams maintain conflicting numbers | Marketing, finance and operations share one tested dimensional model |

## Architecture

![StreamCart architecture: a simulator feeds Kafka and a transactional database. The speed path runs through ksqlDB and a serving sink to Grafana and a REST API. The batch path runs through Spark into Bronze and Silver Iceberg tables, then dbt builds Gold in Postgres for Superset. Airflow orchestrates the batch path.](docs/images/architecture.svg)

**Speed path (seconds).** ksqlDB reads the live Kafka topics and writes `fraud_alerts` and `restock_required` back to Kafka. A small Python sink keeps Postgres serving tables current for Grafana and the REST API.

**Batch path (hourly and daily).** Spark streams Kafka into Bronze Iceberg tables on MinIO. An hourly Spark job validates and cleans that data into Silver, then publishes it to Postgres, where dbt builds the Gold star schema that Superset reads.

Design decisions and data flows are explained in [docs/architecture.md](docs/architecture.md).

## What's in each layer

### Data simulator

[producer/data_generator.py](producer/data_generator.py) produces more than 50,000 orders a day across five markets (US, EU, Asia, Africa, LatAm), with:

- Daily demand peaks, weekend dips and seasonal category weights
- Multi-currency pricing
- Pre-placed fraud: card testing bursts, account takeovers and geo anomalies
- Bot sessions, half of which spoof a browser user agent
- Stock levels that move with orders, restocks and write-offs
- About 1% deliberately corrupted events, so the quality checks have real faults to catch

### Ingestion

- Kafka topics `transactions`, `clickstream` and `inventory_updates`, with one partition per region
- [Avro contracts](schemas/avro) in Schema Registry with BACKWARD compatibility. Producers can't register new schemas themselves.
- Airflow DAGs for the daily OLTP pull, hourly FX rates and the weekly backfill

### Lakehouse

| Layer | Stored in | What happens there |
| --- | --- | --- |
| **Bronze** | Iceberg on MinIO | Raw events, partitioned by `region/year/month/day`, as Snappy Parquet. Kept 90 days, then archived to a cold-storage bucket. |
| **Silver** | Iceberg on MinIO, published to Postgres | Great Expectations gate, bot filtering, 30-minute session stitching, USD conversion at the rate in force at transaction time, and deduplication with `MERGE INTO`. |
| **Gold** | Postgres, built by dbt | 4 dimensions, 3 facts and 5 KPI aggregates, with dbt tests on every model. |

### Real-time

[streaming/ksql](streaming/ksql) defines the streaming logic:

| Signal | Rule |
| --- | --- |
| `HIGH_VALUE_NEW_SESSION` | Over $5,000 within 60 seconds of the session starting |
| `VELOCITY_BREACH` | More than 10 orders from one user in a rolling hour |
| `GEO_ANOMALY` | Order placed from outside the customer's home market |
| `restock_required` | Stock falls below the reorder point |

### Data quality and observability

- [Great Expectations suites](quality/expectations) run twice per table. The run on raw data measures how dirty the input is. The run on cleaned data is a gate: if it fails, nothing is written and the watermark doesn't move.
- dbt tests cover keys, accepted values, ranges and foreign key relationships.
- The Grafana observability dashboard tracks DAG success rates, row counts, data freshness, quality results and schema changes.

### Dashboards

| Tool | Dashboard | Shows |
| --- | --- | --- |
| Grafana | Real-Time Operations | Live transactions by region, fraud alert feed, inventory risk heatmap, pipeline lag |
| Grafana | Pipeline Observability | DAG reliability, row counts, freshness, schema changes, quality results |
| Superset | Business Intelligence | Revenue trends, retention cohorts, conversion funnel, top products |

### REST API

| Endpoint | Purpose |
| --- | --- |
| `GET /inventory/stock` | Current stock, filterable by product and warehouse |
| `GET /inventory/low-stock` | Items below their reorder point, most urgent first |
| `GET /features/fraud/{user_id}` | Online features for a fraud model: order velocity, spend, regions and recent alerts |

Interactive API docs are at `/docs`.

## Getting started

You need Docker with Compose and at least 8 GB of memory allocated to Docker. The first build downloads several gigabytes.

```bash
git clone https://github.com/Jerry-Khobby/StreamCart-Real-Time-E-Commerce-Intelligence-Platform.git
cd StreamCart-Real-Time-E-Commerce-Intelligence-Platform
cp .env.example .env
docker compose up -d --build
```

On first start, a one-shot service seeds 14 days of history and the live generator begins streaming. Gold is built daily at 03:00 UTC. To build it straight away, wait for one `silver_pipeline` run to finish, then trigger `gold_dbt_pipeline` in Airflow.

| Service | URL | Login |
| --- | --- | --- |
| Airflow | <http://localhost:8080> | `admin` / `admin` |
| Grafana | <http://localhost:3000> | `admin` / `admin` |
| Superset | <http://localhost:8089> | `admin` / `admin` |
| REST API docs | <http://localhost:8000/docs> | none |
| MinIO console | <http://localhost:9001> | see `.env` |
| Spark master | <http://localhost:8081> | none |

If a port is already in use, change it in the host ports section of `.env`.

To run the tests and other day-to-day tasks, see [docs/operations.md](docs/operations.md).

## Project status

| Deliverable | Implemented | Checked so far |
| --- | --- | --- |
| Data simulator | Yes | Not yet run |
| Kafka topics and Avro schemas | Yes | Not yet run |
| Airflow DAGs (8) | Yes | Not yet run |
| Lakehouse (Bronze, Silver, Gold) | Yes | Not yet run |
| dbt models and tests | Yes | Not yet run |
| Great Expectations suites | Yes | Not yet run |
| Real-time fraud detection | Yes | Not yet run |
| Grafana and Superset dashboards | Yes | Not yet run |
| Observability dashboard | Yes | Not yet run |
| Architecture diagram and docs | Yes | Diagram rendered and reviewed |
| Docker Compose setup | Yes | `docker compose config` passes; image build in progress |
| Automated tests | Yes | Not yet run |

## Repository layout

```text
api/                 FastAPI service for inventory and fraud features
batch/dags/          Airflow DAGs
batch/db/init/       Postgres schemas, applied on first start
batch/dbt/           dbt project for the Gold layer
consumer/            Spark jobs and the real-time serving sink
dashboards/          Grafana and Superset dashboards as code
docs/                Architecture and operations docs
producer/            Data simulator and schema registration
quality/             Great Expectations suites and runner
schemas/avro/        Kafka event contracts
streaming/ksql/      ksqlDB streams and tables
tests/               Automated tests
```
