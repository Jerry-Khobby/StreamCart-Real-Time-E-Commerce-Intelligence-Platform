"""
DAG: external_sources
=====================
Hourly pulls from the two remaining batch sources:
  - Weather per market, from Open-Meteo (free, no key). Weather affects demand.
  - Stock levels from the inventory system's REST API.
"""

from datetime import datetime, timedelta, timezone

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# A representative city per market
MARKET_LOCATIONS = {
    "US": (40.71, -74.01),
    "EU": (52.52, 13.41),
    "Asia": (1.35, 103.82),
    "Africa": (6.45, 3.39),
    "LatAm": (-23.55, -46.63),
}
INVENTORY_API = "http://api:8000/inventory/stock"


def fetch_weather():
    observed_at = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0, tzinfo=None)
    rows = []
    for region, (latitude, longitude) in MARKET_LOCATIONS.items():
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={"latitude": latitude, "longitude": longitude, "current": "temperature_2m,precipitation"},
            timeout=15,
        )
        response.raise_for_status()
        current = response.json()["current"]
        rows.append((region, observed_at, current["temperature_2m"], current["precipitation"]))

    PostgresHook(postgres_conn_id="streamcart_postgres").insert_rows(
        table="silver.weather_hourly",
        rows=rows,
        target_fields=["region", "observed_at", "temperature_c", "precipitation_mm"],
        replace=True,
        replace_index=["region", "observed_at"],
    )
    print(f"Stored weather for {len(rows)} markets at {observed_at}")


def snapshot_inventory_api():
    response = requests.get(INVENTORY_API, params={"limit": 5000}, timeout=30)
    response.raise_for_status()
    captured_at = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    rows = [
        (captured_at, item["product_id"], item["warehouse_id"], item["region"], item["stock"], item["reorder_point"])
        for item in response.json()
    ]
    if rows:
        PostgresHook(postgres_conn_id="streamcart_postgres").insert_rows(
            table="silver.stock_snapshots",
            rows=rows,
            target_fields=["captured_at", "product_id", "warehouse_id", "region", "stock", "reorder_point"],
        )
    print(f"Captured {len(rows)} stock positions")


with DAG(
    dag_id="external_sources",
    description="Hourly weather and inventory API pulls",
    schedule_interval="5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "streamcart",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=5),
    },
    tags=["external", "silver"],
) as dag:

    PythonOperator(task_id="fetch_weather", python_callable=fetch_weather)
    PythonOperator(task_id="snapshot_inventory_api", python_callable=snapshot_inventory_api)
