-- Serving tables for Grafana and the REST API, kept current by consumer/realtime_sink.py.
-- Event rows are pruned after 48 hours: long-term history lives in the lakehouse.

CREATE SCHEMA IF NOT EXISTS realtime;

CREATE TABLE IF NOT EXISTS realtime.transactions (
    transaction_id TEXT PRIMARY KEY,
    user_id        TEXT,
    region         TEXT NOT NULL,
    amount_usd     DOUBLE PRECISION,
    status         TEXT,
    event_time     TIMESTAMPTZ NOT NULL,
    ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS rt_tx_time_idx ON realtime.transactions (event_time);
CREATE INDEX IF NOT EXISTS rt_tx_user_idx ON realtime.transactions (user_id, event_time);

CREATE TABLE IF NOT EXISTS realtime.fraud_alerts (
    transaction_id TEXT NOT NULL,
    fraud_signal   TEXT NOT NULL,
    user_id        TEXT,
    amount_usd     DOUBLE PRECISION,
    region         TEXT,
    event_time     TIMESTAMPTZ NOT NULL,
    ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (transaction_id, fraud_signal)
);
CREATE INDEX IF NOT EXISTS rt_fraud_time_idx ON realtime.fraud_alerts (event_time);

-- Current stock per product per warehouse (not pruned: this is state, not events).
CREATE TABLE IF NOT EXISTS realtime.stock_levels (
    product_id    TEXT NOT NULL,
    warehouse_id  TEXT NOT NULL,
    region        TEXT NOT NULL,
    stock         INT NOT NULL,
    reorder_point INT NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (product_id, warehouse_id)
);

CREATE TABLE IF NOT EXISTS realtime.restock_events (
    event_id      TEXT PRIMARY KEY,
    product_id    TEXT NOT NULL,
    warehouse_id  TEXT NOT NULL,
    region        TEXT NOT NULL,
    stock         INT NOT NULL,
    reorder_point INT NOT NULL,
    event_time    TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS rt_restock_time_idx ON realtime.restock_events (event_time);

-- Sampled by the sink: how far behind the head of each topic it is.
CREATE TABLE IF NOT EXISTS realtime.pipeline_lag (
    sampled_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    topic         TEXT NOT NULL,
    consumer_lag  BIGINT NOT NULL,
    latency_ms    DOUBLE PRECISION,
    PRIMARY KEY (sampled_at, topic)
);
