-- Daily row counts per Silver table against the median of the 7 days before,
-- written by the observability_monitor DAG.
CREATE TABLE IF NOT EXISTS observability.volume_checks (
    business_date DATE NOT NULL,
    table_name    TEXT NOT NULL,
    row_count     BIGINT NOT NULL,
    baseline      DOUBLE PRECISION,
    ratio         DOUBLE PRECISION,
    flagged       BOOLEAN NOT NULL,
    checked_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (business_date, table_name)
);
