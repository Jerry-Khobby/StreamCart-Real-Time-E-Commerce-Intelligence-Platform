-- Kept separate because INSERT INTO has no IF NOT EXISTS: running it twice would
-- start a second persistent query. apply.sh only runs it when none exists.

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
