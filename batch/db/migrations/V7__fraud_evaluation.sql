-- Ground truth for scoring the fraud rules. The simulator labels the fraud it injects;
-- these labels are never used to decide an alert, only to measure the rules afterwards.
ALTER TABLE realtime.transactions ADD COLUMN IF NOT EXISTS is_fraud BOOLEAN;
ALTER TABLE realtime.transactions ADD COLUMN IF NOT EXISTS fraud_type TEXT;

-- Precision and recall of each rule over a rolling 24 hours.
CREATE TABLE IF NOT EXISTS observability.fraud_evaluation (
    evaluated_at    TIMESTAMPTZ NOT NULL,
    fraud_signal    TEXT NOT NULL,
    true_positives  BIGINT NOT NULL,
    false_positives BIGINT NOT NULL,
    false_negatives BIGINT NOT NULL,
    precision_pct   DOUBLE PRECISION,
    recall_pct      DOUBLE PRECISION,
    PRIMARY KEY (evaluated_at, fraud_signal)
);
