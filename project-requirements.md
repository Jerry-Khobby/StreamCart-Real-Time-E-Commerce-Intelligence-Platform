#  README.md (Ready to Copy)

```markdown
# StreamCart: Real-Time E-Commerce Intelligence Platform

**You are a Senior Data Engineer at a global e-commerce company. Your system processes millions of transactions, clickstream events, and inventory updates daily across multiple regions. The business needs both real-time operational decisions and historical batch analytics from a single unified data platform.**

---

## The Core Problem

The company has three critical pain points:

1. **Fraud detection is too slow** — by the time batch jobs catch suspicious transactions, money is already gone  
2. **Inventory decisions are stale** — warehouse teams are working off yesterday's data when making restocking decisions  
3. **Customer analytics is disconnected** — marketing, finance, and operations all maintain separate data silos with conflicting numbers  

Your job is to build a Data Lakehouse that serves all three needs from one unified architecture.

---

## Architecture Overview

```

Data Sources
│
├── Transactional DB (PostgreSQL) — orders, payments, users
├── Clickstream Events (simulated Kafka) — page views, add-to-cart, searches
├── Inventory System (REST API) — stock levels, warehouse data
└── External APIs — currency exchange rates, weather (affects demand)
│
▼
Ingestion Layer
├── Apache Kafka — real-time event streaming
└── Apache Airflow — batch orchestration
│
▼
Storage Layer (Lakehouse)
├── Bronze Zone — raw, unmodified data (Parquet on S3/MinIO)
├── Silver Zone — cleaned, validated, deduplicated
└── Gold Zone — business-ready aggregated tables
│
▼
Serving Layer
├── Real-Time: ksqlDB / Flink — fraud alerts, live inventory
└── Batch: dbt — dimensional models, KPIs, trend analysis
│
▼
Consumption Layer
├── Grafana — real-time operational dashboards
├── Apache Superset — self-service BI for business teams
└── REST API — serves ML feature store for fraud model

````

---

## What You Need to Build

---

### Module 1 — Data Generation & Simulation

Build a realistic data simulator that generates:

**Transactions** — 50,000+ orders per day across 5 regional markets (US, EU, Asia, Africa, LatAm) with:
- Realistic purchase patterns (morning peaks, weekend dips)
- Pre-placed fraud windows (card testing, account takeover patterns)
- Multi-currency amounts with exchange rate conversion
- Product categories with seasonal demand weights

**Clickstream events** — user sessions with:
- Page views, search queries, product views, add-to-cart, checkout abandonment
- Bot traffic mixed in (to be filtered in the Silver layer)
- Session stitching (same user across multiple events)

**Inventory updates** — warehouse stock changes triggered by:
- Orders reducing stock
- Restocking events
- Damaged goods write-offs

---

### Module 2 — Ingestion Layer

**Kafka setup (via Docker Compose):**
- Topic per event type: `transactions`, `clickstream`, `inventory_updates`
- Partitioned by region for parallel consumption
- Schema Registry with Avro schemas enforcing field contracts

**Airflow DAGs:**
- Daily batch pull from the transactional PostgreSQL database
- Hourly currency exchange rate fetch from an external API
- Weekly historical backfill DAG for reprocessing

---

### Module 3 — Bronze Layer (Raw Storage)

Everything lands here unmodified:
- Partitioned by `region/year/month/day`
- Stored as Parquet with Snappy compression
- Metadata tracked in Apache Iceberg table format (enables time travel)
- Data retention: raw data kept 90 days, then archived to cold storage

---

### Module 4 — Silver Layer (Cleaning & Validation)

This is where data quality is enforced:

**Validation checks (Great Expectations):**
- Required fields present
- Timestamps within acceptable range (no future events, no events older than 7 days)
- Currency codes valid ISO 4217
- Amount values positive and within reasonable bounds
- User IDs match known users (referential integrity)

**Transformations:**
- Bot traffic filtered from clickstream (user-agent patterns, request rate thresholds)
- Session stitching — assign session IDs to clickstream events from the same user within a 30-minute window
- Currency normalisation — all amounts converted to USD using the exchange rate at transaction time
- Deduplication — idempotent writes using Iceberg MERGE INTO

---

### Module 5 — Gold Layer (dbt Models)

Build a proper dimensional model:

**Dimension tables:**
- `dim_customers`
- `dim_products`
- `dim_dates`
- `dim_regions`

**Fact tables:**
- `fct_orders`
- `fct_sessions`
- `fct_inventory_snapshots`

**Aggregated Gold tables (business KPIs):**
- `agg_daily_revenue`
- `agg_fraud_summary`
- `agg_inventory_health`
- `agg_funnel_conversion`

---

### Module 6 — Real-Time Layer

```sql
CREATE STREAM fraud_alerts AS
SELECT
    transaction_id,
    user_id,
    amount_usd,
    region,
    CASE
        WHEN amount_usd > 5000 AND session_age_seconds < 60 THEN 'HIGH_VALUE_NEW_SESSION'
        WHEN transaction_count_1h > 10 THEN 'VELOCITY_BREACH'
        WHEN country_mismatch = true THEN 'GEO_ANOMALY'
    END AS fraud_signal
FROM transactions_stream
WHERE fraud_signal IS NOT NULL;
````

**Live inventory alerts:**

* Detect low stock in real time
* Publish to `restock_required` Kafka topic
* Visualize in Grafana

---

### Module 7 — Data Quality & Observability

* Great Expectations validation suites
* dbt tests on all models
* Observability dashboard:

  * DAG success/failure rates
  * Row count tracking
  * Data freshness monitoring
  * Schema change detection

---

### Module 8 — Dashboards

**Grafana (real-time):**

* Live transactions by region
* Fraud alerts feed
* Inventory risk heatmap
* Pipeline lag monitoring

**Apache Superset (batch):**

* Revenue trends
* Customer retention cohorts
* Funnel conversion rates
* Top products analysis

---

### Module 9 — Infrastructure

```
PostgreSQL, Kafka, Zookeeper, Schema Registry, ksqlDB,
Airflow, MinIO, Spark, dbt, Great Expectations, Grafana, Superset
```

Run everything:

```bash
docker compose up -d
```

---

## Deliverables Checklist

* [ ] Data simulator
* [ ] Kafka topics + schemas
* [ ] Airflow DAGs
* [ ] Lakehouse (Bronze/Silver/Gold)
* [ ] dbt models
* [ ] Data quality suite
* [ ] Real-time fraud detection
* [ ] Dashboards
* [ ] Observability dashboard
* [ ] README + architecture diagram
* [ ] Docker Compose setup

---

## What This Proves to Employers

| Skill                  | Where it shows     |
| ---------------------- | ------------------ |
| Streaming engineering  | Kafka, ksqlDB      |
| Batch orchestration    | Airflow            |
| Data modelling         | dbt                |
| Data quality           | Great Expectations |
| Lakehouse architecture | Bronze/Silver/Gold |
| SQL                    | ksqlDB, dbt        |
| Infrastructure         | Docker Compose     |
| Business thinking      | KPIs and analytics |

---

## Suggested Build Order

**Week 1** — Simulator + Kafka + Bronze
**Week 2** — Silver + dbt + Airflow
**Week 3** — Real-time + Grafana
**Week 4** — Superset + QA + Docs

