"""
DAG: fx_rates_refresh
=====================
Fetches live currency exchange rates every hour and stores them
in Postgres so the Silver layer uses accurate FX rates.

Source: exchangerate-api.com (free tier, 1500 requests/month)
Fallback: hardcoded rates if API is unavailable.

Why hourly: FX rates move throughout the day. Using stale rates
introduces systematic error into amount_usd_normalised in Silver.
"""

from datetime import datetime, timedelta
import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Fallback rates if API is down
FALLBACK_RATES = {
    "USD": 1.00,
    "EUR": 1.09,
    "JPY": 0.0067,
    "NGN": 0.00064,
    "BRL": 0.20,
}

def fetch_and_store_fx_rates(**context):
    """
    Fetches USD-base exchange rates and upserts into postgres.
    Creates the table if it doesn't exist.
    """
    hook = PostgresHook(postgres_conn_id="streamcart_postgres")

    # Ensure table exists
    hook.run("""
        CREATE TABLE IF NOT EXISTS silver.fx_rates (
            currency        TEXT PRIMARY KEY,
            rate_to_usd     DOUBLE PRECISION NOT NULL,
            fetched_at      TIMESTAMP NOT NULL,
            source          TEXT
        );
    """)

    # Try live API first
    rates = {}
    source = "fallback"
    try:
        resp = requests.get(
            "https://open.er-api.com/v6/latest/USD",
            timeout=10
        )
        if resp.status_code == 200:
            data = resp.json()
            raw_rates = data.get("rates", {})
            # Convert: API gives USD→X, we need X→USD
            for currency in FALLBACK_RATES:
                if currency in raw_rates and raw_rates[currency] != 0:
                    rates[currency] = 1.0 / raw_rates[currency]
            source = "open.er-api.com"
            print(f"[fx_rates] Live rates fetched: {rates}")
        else:
            print(f"[fx_rates] API returned {resp.status_code}, using fallback")
            rates = FALLBACK_RATES
    except Exception as e:
        print(f"[fx_rates] API error: {e} — using fallback rates")
        rates = FALLBACK_RATES

    # Upsert rates into Postgres
    fetched_at = context["execution_date"].isoformat()
    rows = [
        (currency, rate, fetched_at, source)
        for currency, rate in rates.items()
    ]

    hook.insert_rows(
        table="silver.fx_rates",
        rows=rows,
        target_fields=["currency", "rate_to_usd", "fetched_at", "source"],
        replace=True,
        replace_index="currency",
    )

    print(f"[fx_rates] Upserted {len(rows)} rates from {source}")
    for currency, rate in rates.items():
        print(f"  1 {currency} = {rate:.6f} USD")

default_args = {
    "owner":             "streamcart",
    "retries":           3,
    "retry_delay":       timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=5),
}

with DAG(
    dag_id="fx_rates_refresh",
    description="Hourly FX rate fetch → silver.fx_rates in Postgres",
    schedule_interval="0 * * * *",   # Every hour on the hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["fx", "silver", "streamcart"],
) as dag:

    fetch_rates = PythonOperator(
        task_id="fetch_and_store_fx_rates",
        python_callable=fetch_and_store_fx_rates,
        provide_context=True,
    )