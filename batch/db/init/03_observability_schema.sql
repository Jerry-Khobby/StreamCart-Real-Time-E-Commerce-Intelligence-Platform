-- Pipeline health data behind the Grafana observability dashboard.
-- DAG success rates come straight from Airflow's own dag_run table.

CREATE SCHEMA IF NOT EXISTS observability;

-- Great Expectations outcomes, one row per expectation per Silver run.
CREATE TABLE IF NOT EXISTS observability.quality_results (
    run_at            TIMESTAMPTZ NOT NULL,
    table_name        TEXT NOT NULL,
    stage             TEXT NOT NULL,  -- 'raw' (before cleaning) or 'clean' (the write gate)
    expectation       TEXT NOT NULL,
    column_name       TEXT NOT NULL,
    success           BOOLEAN NOT NULL,
    unexpected_count  BIGINT,
    PRIMARY KEY (run_at, table_name, stage, expectation, column_name)
);

-- Row accounting for each Silver run: what came in, what survived, what was rejected.
CREATE TABLE IF NOT EXISTS observability.silver_runs (
    run_at     TIMESTAMPTZ NOT NULL,
    table_name TEXT NOT NULL,
    rows_in    BIGINT NOT NULL,
    rows_out   BIGINT NOT NULL,
    rejected   BIGINT NOT NULL,
    gate       TEXT NOT NULL,
    PRIMARY KEY (run_at, table_name)
);

CREATE TABLE IF NOT EXISTS observability.row_counts (
    sampled_at TIMESTAMPTZ NOT NULL,
    table_name TEXT NOT NULL,
    row_count  BIGINT NOT NULL,
    PRIMARY KEY (sampled_at, table_name)
);

CREATE TABLE IF NOT EXISTS observability.freshness (
    sampled_at  TIMESTAMPTZ NOT NULL,
    table_name  TEXT NOT NULL,
    latest_at   TIMESTAMPTZ,
    age_seconds DOUBLE PRECISION,
    PRIMARY KEY (sampled_at, table_name)
);

-- The most recent column list per table, and every difference ever detected.
CREATE TABLE IF NOT EXISTS observability.schema_state (
    object_name TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    definition  TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS observability.schema_changes (
    detected_at TIMESTAMPTZ NOT NULL,
    object_name TEXT NOT NULL,
    change_kind TEXT NOT NULL,
    detail      TEXT NOT NULL
);
