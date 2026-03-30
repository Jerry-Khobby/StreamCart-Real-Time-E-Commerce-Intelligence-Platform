-- models/staging/stg_transactions.sql
-- Thin view over the silver.transactions table.
-- Casts types and renames columns to canonical names used across all Gold models.







with source as (
    select * from {{ source('silver', 'transactions') }}
),

renamed as (
    select
        transaction_id,
        user_id,
        product_id,
        category as product_category,
        region,
        currency,
        amount_local,
        amount_usd,
        amount_usd_normalised,
        quantity,
        payment_method,
        status as order_status,
        event_time::timestamptz as occurred_at,
        date_trunc('day', event_time)::date as event_date,
        extract(year from event_time)::int as event_year,
        extract(month from event_time)::int as event_month,
        extract(dow from event_time)::int as day_of_week,
        extract(hour from event_time)::int as hour_of_day,
        fx_discrepancy
    from source
    where status != 'failed'   -- move filter here, use original column name
)

select *
from renamed