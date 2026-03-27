-- models/aggregates/agg_daily_revenue.sql
-- Grain: one row per region per day.
-- Powers the Grafana/Superset revenue trend dashboards.








{{
    config(
        materialized   = 'incremental',
        unique_key     = 'surrogate_key',
        incremental_strategy = 'merge'
    )
}}

with orders as (
    select * from {{ ref('fct_orders') }}

    {% if is_incremental() %}
        where event_date > (select max(event_date) from {{ this }})
    {% endif %}
)

select
    {{ dbt_utils.generate_surrogate_key(['event_date', 'region']) }} as surrogate_key,

    event_date,
    event_year,
    event_month,
    region,

    -- Volume
    count(transaction_id)                           as total_orders,
    count(distinct user_id)                         as unique_customers,
    sum(quantity)                                   as total_units_sold,

    -- Revenue
    sum(amount_usd)                                 as gross_revenue_usd,
    sum(case when order_status = 'refunded'
             then amount_usd else 0 end)            as refund_amount_usd,
    sum(amount_usd) - sum(
        case when order_status = 'refunded'
             then amount_usd else 0 end
    )                                               as net_revenue_usd,

    -- Averages
    avg(amount_usd)                                 as avg_order_value_usd,
    sum(amount_usd) / nullif(count(distinct user_id), 0)
                                                    as revenue_per_customer_usd,

    -- Order status breakdown
    count(case when order_status = 'completed' then 1 end) as completed_orders,
    count(case when order_status = 'pending'   then 1 end) as pending_orders,
    count(case when order_status = 'refunded'  then 1 end) as refunded_orders,

    -- Payment mix
    count(case when payment_method = 'credit_card'   then 1 end) as credit_card_orders,
    count(case when payment_method = 'debit_card'    then 1 end) as debit_card_orders,
    count(case when payment_method = 'paypal'        then 1 end) as paypal_orders,
    count(case when payment_method = 'crypto'        then 1 end) as crypto_orders,

    -- Category breakdown (top 3 by revenue)
    sum(case when product_category = 'Electronics'   then amount_usd else 0 end) as revenue_electronics,
    sum(case when product_category = 'Clothing'      then amount_usd else 0 end) as revenue_clothing,
    sum(case when product_category = 'Jewellery'     then amount_usd else 0 end) as revenue_jewellery,
    sum(case when product_category = 'Sports'        then amount_usd else 0 end) as revenue_sports,
    sum(case when product_category = 'Home & Garden' then amount_usd else 0 end) as revenue_home_garden,

    current_timestamp                               as dbt_updated_at

from orders
where order_status != 'failed'
group by event_date, event_year, event_month, region