-- Kept separate because INSERT INTO has no IF NOT EXISTS: running it twice would
-- start a second persistent query. apply.sh only runs it when none exists.

-- SET applies per session, so streamcart.sql's setting doesn't reach this query.
-- Without it, breaches found before this query starts would never become alerts.
SET 'auto.offset.reset' = 'earliest';

INSERT INTO fraud_alerts
  SELECT
    TRANSACTION_ID,
    USER_ID,
    AMOUNT_USD,
    REGION,
    'VELOCITY_BREACH' AS FRAUD_SIGNAL,
    EVENT_TIME
  FROM user_velocity_breaches
  EMIT CHANGES;
