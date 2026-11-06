"""
DAG: fx_rates_refresh
=====================
Fetches currency exchange rates every hour and appends them to silver.fx_rates.

Rates are kept as history, not overwritten: Silver converts each order at the rate
in force when it was placed. If the API fails, the task fails and retries, and Silver
keeps using the most recent rate it has. That's better than storing invented numbers.

Source: open.er-api.com (free, no key)
"""

from datetime import datetime, timedelta, timezone

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from common import record_failure

CURRENCIES = ["USD", "EUR", "JPY", "NGN", "BRL"]


def fetch_and_store_fx_rates():
    response = requests.get("https://open.er-api.com/v6/latest/USD", timeout=15)
    response.raise_for_status()
    usd_to_x = response.json()["rates"]

    fetched_at = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    # The API gives USD → X. Silver needs X → USD.
    rows = [(c, 1.0 / usd_to_x[c], fetched_at, "open.er-api.com") for c in CURRENCIES]

    PostgresHook(postgres_conn_id="streamcart_postgres").insert_rows(
        table="silver.fx_rates",
        rows=rows,
        target_fields=["currency", "rate_to_usd", "fetched_at", "source"],
    )
    for currency, rate, _, _ in rows:
        print(f"1 {currency} = {rate:.6f} USD")


with DAG(
    dag_id="fx_rates_refresh",
    description="Hourly FX rate fetch → silver.fx_rates",
    schedule_interval="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "streamcart",
        "on_failure_callback": record_failure,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=5),
    },
    tags=["fx", "external", "silver"],
) as dag:

    fetch_rates = PythonOperator(
        task_id="fetch_and_store_fx_rates",
        python_callable=fetch_and_store_fx_rates,
    )
