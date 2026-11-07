-- models/staging/stg_transactions.sql
-- Thin view over the silver.transactions table.
-- Casts types and renames columns to canonical names used across all Gold models.
-- amount_usd here is the Silver value converted at the rate on the transaction date,
-- so every Gold revenue figure shares one definition.
-- Dates and hours are in the market's own time (see macros/market_time.sql).

with source as (
    select s.*, r.timezone
    from {{ source('silver', 'transactions') }} s
    left join {{ ref('regions') }} r on r.region_code = s.region
),

renamed as (
    select
        transaction_id,
        user_id,
        product_id,
        category as product_category,
        region,
        ip_region,
        session_age_seconds,
        currency,
        amount_local,
        amount_usd_normalised as amount_usd,
        amount_usd as amount_usd_reported,
        fx_rate_used,
        quantity,
        payment_method,
        status as order_status,
        {{ utc_instant('event_time') }} as occurred_at,
        {{ market_time('event_time', 'timezone') }}::date as event_date,
        extract(year from {{ market_time('event_time', 'timezone') }})::int as event_year,
        extract(month from {{ market_time('event_time', 'timezone') }})::int as event_month,
        extract(dow from {{ market_time('event_time', 'timezone') }})::int as day_of_week,
        extract(hour from {{ market_time('event_time', 'timezone') }})::int as hour_of_day,
        fx_discrepancy
    from source
    where status != 'failed'
)

select *
from renamed
