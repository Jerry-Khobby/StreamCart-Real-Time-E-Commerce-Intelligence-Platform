"""
StreamCart REST API.

Two consumers, one service:
  - /inventory/*      the inventory system's view of stock, polled hourly by Airflow
  - /features/fraud/* the online feature store a fraud-scoring model would call
"""

import os
from contextlib import contextmanager
from typing import Optional

import psycopg
from fastapi import FastAPI, HTTPException, Query
from psycopg.rows import dict_row

app = FastAPI(
    title="StreamCart API",
    version="1.0.0",
    description="Live inventory and online fraud features, served from the real-time layer.",
)


@contextmanager
def db_cursor():
    with psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        row_factory=dict_row,
    ) as conn, conn.cursor() as cur:
        yield cur


@app.get("/health", tags=["ops"])
def health():
    with db_cursor() as cur:
        cur.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/inventory/stock", tags=["inventory"])
def stock_levels(
    product_id: Optional[str] = None,
    warehouse_id: Optional[str] = None,
    limit: int = Query(default=100, ge=1, le=5000),
):
    """Current stock per product per warehouse."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT product_id, warehouse_id, region, stock, reorder_point, updated_at
            FROM realtime.stock_levels
            WHERE (%(product_id)s::text IS NULL OR product_id = %(product_id)s)
              AND (%(warehouse_id)s::text IS NULL OR warehouse_id = %(warehouse_id)s)
            ORDER BY product_id, warehouse_id
            LIMIT %(limit)s
            """,
            {"product_id": product_id, "warehouse_id": warehouse_id, "limit": limit},
        )
        return cur.fetchall()


@app.get("/inventory/low-stock", tags=["inventory"])
def low_stock(limit: int = Query(default=100, ge=1, le=5000)):
    """Products currently below their reorder point, most urgent first."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT product_id, warehouse_id, region, stock, reorder_point, updated_at
            FROM realtime.stock_levels
            WHERE stock < reorder_point
            ORDER BY stock - reorder_point
            LIMIT %(limit)s
            """,
            {"limit": limit},
        )
        return cur.fetchall()


@app.get("/features/fraud/{user_id}", tags=["ml-features"])
def fraud_features(user_id: str):
    """Online features for one user, computed from the last 48 hours of activity."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT
                count(*) FILTER (WHERE event_time > now() - interval '1 hour')   AS transaction_count_1h,
                count(*) FILTER (WHERE event_time > now() - interval '24 hours') AS transaction_count_24h,
                coalesce(sum(amount_usd) FILTER (WHERE event_time > now() - interval '24 hours'), 0)
                                                                                 AS spend_usd_24h,
                coalesce(avg(amount_usd), 0)                                     AS avg_amount_usd_48h,
                count(DISTINCT region)                                           AS distinct_regions_48h,
                max(event_time)                                                  AS last_transaction_at
            FROM realtime.transactions
            WHERE user_id = %(user_id)s
            """,
            {"user_id": user_id},
        )
        features = cur.fetchone()

        cur.execute(
            """
            SELECT count(*) AS fraud_alert_count_48h,
                   coalesce(array_agg(DISTINCT fraud_signal), '{}') AS fraud_signals_48h
            FROM realtime.fraud_alerts
            WHERE user_id = %(user_id)s
            """,
            {"user_id": user_id},
        )
        alerts = cur.fetchone()

    if features["last_transaction_at"] is None and alerts["fraud_alert_count_48h"] == 0:
        raise HTTPException(status_code=404, detail="No recent activity for this user")
    return {"user_id": user_id, **features, **alerts}
