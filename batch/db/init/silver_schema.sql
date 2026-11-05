-- Silver tables in Postgres: the serving copy dbt builds Gold from.
-- The Spark job merges into Iceberg first, then publishes here.

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.transactions (
    transaction_id          TEXT PRIMARY KEY,
    user_id                 TEXT,
    product_id              TEXT,
    category                TEXT,
    region                  TEXT,
    ip_region               TEXT,
    currency                TEXT,
    amount_local            DOUBLE PRECISION,
    amount_usd              DOUBLE PRECISION,
    amount_usd_normalised   DOUBLE PRECISION,
    fx_rate_used            DOUBLE PRECISION,
    quantity                INT,
    payment_method          TEXT,
    status                  TEXT,
    session_age_seconds     DOUBLE PRECISION,
    event_time              TIMESTAMP,
    fx_discrepancy          BOOLEAN
);

CREATE TABLE IF NOT EXISTS silver.clickstream (
    event_id                TEXT PRIMARY KEY,
    session_id              TEXT,
    session_id_silver       TEXT,
    user_id                 TEXT,
    region                  TEXT,
    page_type               TEXT,
    product_id              TEXT,
    search_query            TEXT,
    referrer                TEXT,
    user_agent              TEXT,
    event_time              TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.inventory (
    event_id                TEXT PRIMARY KEY,
    product_id              TEXT,
    warehouse_id            TEXT,
    region                  TEXT,
    update_type             TEXT,
    quantity_delta          INT,
    stock_after             INT,
    reorder_point           INT,
    event_time              TIMESTAMP
);

-- Hourly history, so each order converts at the rate in force when it was placed.
CREATE TABLE IF NOT EXISTS silver.fx_rates (
    currency     TEXT NOT NULL,
    rate_to_usd  DOUBLE PRECISION NOT NULL,
    fetched_at   TIMESTAMP NOT NULL,
    source       TEXT,
    PRIMARY KEY (currency, fetched_at)
);

CREATE TABLE IF NOT EXISTS silver.weather_hourly (
    region            TEXT NOT NULL,
    observed_at       TIMESTAMP NOT NULL,
    temperature_c     DOUBLE PRECISION,
    precipitation_mm  DOUBLE PRECISION,
    PRIMARY KEY (region, observed_at)
);

-- Hourly stock positions pulled from the inventory system's REST API.
CREATE TABLE IF NOT EXISTS silver.stock_snapshots (
    captured_at    TIMESTAMP NOT NULL,
    product_id     TEXT NOT NULL,
    warehouse_id   TEXT NOT NULL,
    region         TEXT NOT NULL,
    stock          INT NOT NULL,
    reorder_point  INT NOT NULL,
    PRIMARY KEY (captured_at, product_id, warehouse_id)
);
