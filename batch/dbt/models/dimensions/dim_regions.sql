-- models/dimensions/dim_regions.sql
-- One row per market, from the regions seed, with its activity totals.

with regions as (
    select * from {{ ref('regions') }}
),

activity as (
    select
        region,
        sum(amount_usd)                     as total_revenue_usd,
        count(distinct transaction_id)      as total_orders,
        count(distinct user_id)             as unique_customers

    from {{ ref('stg_transactions') }}
    where order_status = 'completed'
    group by region
)

select
    {{ dbt_utils.generate_surrogate_key(['r.region_code']) }} as region_key,
    r.region_code,
    r.region_name,
    r.super_region,
    r.local_currency,
    r.timezone,
    coalesce(a.total_revenue_usd, 0)        as total_revenue_usd,
    coalesce(a.total_orders, 0)             as total_orders,
    coalesce(a.unique_customers, 0)         as unique_customers,
    current_timestamp                       as dbt_updated_at

from regions r
left join activity a on a.region = r.region_code
