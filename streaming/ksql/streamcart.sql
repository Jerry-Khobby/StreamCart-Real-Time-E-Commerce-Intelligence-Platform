-- StreamCart real-time layer (ksqlDB).
-- Applied by streaming/ksql/apply.sh. Every statement here is idempotent.

SET 'auto.offset.reset' = 'earliest';

-- ── Sources ─────────────────────────────────────────────────────────────────
-- Column definitions are inferred from the Schema Registry contracts.
CREATE STREAM IF NOT EXISTS transactions_stream
  WITH (KAFKA_TOPIC = 'transactions', VALUE_FORMAT = 'AVRO');

CREATE STREAM IF NOT EXISTS inventory_stream
  WITH (KAFKA_TOPIC = 'inventory_updates', VALUE_FORMAT = 'AVRO');

-- Live, scorable orders only: seed data and malformed events can't raise alerts.
CREATE STREAM IF NOT EXISTS transactions_live AS
  SELECT
    TRANSACTION_ID,
    USER_ID,
    AMOUNT_USD,
    REGION,
    IP_REGION,
    SESSION_AGE_SECONDS,
    (IP_REGION <> REGION) AS COUNTRY_MISMATCH,
    `TIMESTAMP` AS EVENT_TIME
  FROM transactions_stream
  WHERE IS_BACKFILL = false
    AND USER_ID IS NOT NULL
    AND AMOUNT_USD > 0
  EMIT CHANGES;

-- ── Fraud alerts ────────────────────────────────────────────────────────────
-- Per-event signals. ksqlDB can't join a stream to a windowed table, so
-- VELOCITY_BREACH can't sit in this CASE; see velocity_insert.sql.
CREATE STREAM IF NOT EXISTS fraud_alerts
  WITH (KAFKA_TOPIC = 'fraud_alerts', VALUE_FORMAT = 'AVRO', PARTITIONS = 5) AS
  SELECT
    TRANSACTION_ID,
    USER_ID,
    AMOUNT_USD,
    REGION,
    CASE
      WHEN AMOUNT_USD > 5000 AND SESSION_AGE_SECONDS < 60 THEN 'HIGH_VALUE_NEW_SESSION'
      ELSE 'GEO_ANOMALY'
    END AS FRAUD_SIGNAL,
    EVENT_TIME
  FROM transactions_live
  WHERE (AMOUNT_USD > 5000 AND SESSION_AGE_SECONDS < 60)
     OR COUNTRY_MISMATCH = true
  EMIT CHANGES;

-- Rolling one-hour order count per user, updated every five minutes.
CREATE TABLE IF NOT EXISTS user_velocity_1h
  WITH (KAFKA_TOPIC = 'user_velocity_1h', VALUE_FORMAT = 'AVRO', PARTITIONS = 5) AS
  SELECT
    USER_ID AS VELOCITY_USER_ID,
    AS_VALUE(USER_ID) AS USER_ID,
    COUNT(*) AS TRANSACTION_COUNT_1H,
    LATEST_BY_OFFSET(TRANSACTION_ID) AS TRANSACTION_ID,
    LATEST_BY_OFFSET(AMOUNT_USD) AS AMOUNT_USD,
    LATEST_BY_OFFSET(REGION) AS REGION,
    LATEST_BY_OFFSET(EVENT_TIME) AS EVENT_TIME
  FROM transactions_live
  WINDOW HOPPING (SIZE 1 HOUR, ADVANCE BY 5 MINUTES)
  GROUP BY USER_ID
  HAVING COUNT(*) > 10
  EMIT CHANGES;

-- Reads the table's changelog as a stream of breaches, without its windowed key.
CREATE STREAM IF NOT EXISTS user_velocity_breaches (
    USER_ID VARCHAR,
    TRANSACTION_COUNT_1H BIGINT,
    TRANSACTION_ID VARCHAR,
    AMOUNT_USD DOUBLE,
    REGION VARCHAR,
    EVENT_TIME VARCHAR
  ) WITH (KAFKA_TOPIC = 'user_velocity_1h', VALUE_FORMAT = 'AVRO');

-- ── Live inventory alerts ───────────────────────────────────────────────────
CREATE STREAM IF NOT EXISTS restock_required
  WITH (KAFKA_TOPIC = 'restock_required', VALUE_FORMAT = 'AVRO', PARTITIONS = 5) AS
  SELECT
    EVENT_ID,
    PRODUCT_ID,
    WAREHOUSE_ID,
    REGION,
    STOCK_AFTER,
    REORDER_POINT,
    `TIMESTAMP` AS EVENT_TIME
  FROM inventory_stream
  WHERE IS_BACKFILL = false
    AND STOCK_AFTER < REORDER_POINT
  EMIT CHANGES;
