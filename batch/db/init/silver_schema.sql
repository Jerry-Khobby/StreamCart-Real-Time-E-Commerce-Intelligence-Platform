-- ./db/init/silver_schema.sql

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.transactions (
    transaction_id          TEXT PRIMARY KEY,
    user_id                 TEXT,
    product_id              TEXT,
    category                TEXT,
    region                  TEXT,
    currency                TEXT,
    amount_local            FLOAT,
    amount_usd              FLOAT,
    amount_usd_normalised   FLOAT,
    quantity                INT,
    payment_method           TEXT,
    status                  TEXT,
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
    event_time              TIMESTAMP
);