-- models/facts/fct_orders.sql
-- Grain: one row per order.
-- Incremental: appends new orders since last run using occurred_at.








{{
    config(
        materialized   = 'incremental',
        unique_key     = 'surrogate_key',
        incremental_strategy = 'merge',
        on_schema_change = 'append_new_columns'
    )
}}

with transactions as (
    select * from {{ ref('stg_transactions') }}

    {% if is_incremental() %}
        -- Only pick up rows newer than the latest record already in the table
        where occurred_at > (select max(occurred_at) from {{ this }})
    {% endif %}
),

joined as (
    select
        t.transaction_id,
        t.user_id,
        t.product_id,
        t.region,
        t.product_category,
        t.order_status,
        t.payment_method,
        t.currency,
        t.amount_local,
        t.amount_usd,
        t.amount_usd_normalised,
        t.quantity,
        t.occurred_at,
        t.event_date,
        t.event_year,
        t.event_month,
        t.day_of_week,
        t.hour_of_day,
        t.fx_discrepancy,

        -- FK lookups
        c.customer_key,
        p.product_key,
        d.date_key,
        r.region_key,

        -- Derived metrics
        t.amount_usd / nullif(t.quantity, 0)    as revenue_per_unit_usd,

        case
            when t.hour_of_day between 6  and 11  then 'Morning'
            when t.hour_of_day between 12 and 17  then 'Afternoon'
            when t.hour_of_day between 18 and 22  then 'Evening'
            else                                       'Night'
        end                                     as time_of_day_bucket

    from transactions t
    left join {{ ref('dim_customers') }} c using (user_id)
    left join {{ ref('dim_products')  }} p using (product_id)
    left join {{ ref('dim_dates')     }} d on d.date_id = t.event_date
    left join {{ ref('dim_regions')   }} r on r.region_code = t.region
)

select
    {{ dbt_utils.generate_surrogate_key(['transaction_id']) }} as surrogate_key,
    *
from joined