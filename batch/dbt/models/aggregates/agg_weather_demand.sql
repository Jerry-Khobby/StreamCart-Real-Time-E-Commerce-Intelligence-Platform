-- models/aggregates/agg_weather_demand.sql
-- Grain: one row per market per hour that has a weather reading and orders.
-- Does rain change demand? demand_index compares an hour's orders with that market's
-- average for the same local hour of day, so the daily shopping curve doesn't hide the effect.
-- Above 1 means busier than usual for that time of day.

{{ config(materialized = 'table') }}

with weather as (
    select
        region,
        observed_at                                             as weather_hour,
        temperature_c,
        precipitation_mm,
        coalesce(precipitation_mm, 0) >= {{ var('wet_hour_min_precipitation_mm') }} as is_wet_hour
    from {{ source('silver', 'weather_hourly') }}
),

hourly_orders as (
    select
        region,
        date_trunc('hour', occurred_at at time zone 'UTC')      as weather_hour,
        count(*)                                                as orders,
        sum(amount_usd)                                         as revenue_usd
    from {{ ref('fct_orders') }}
    where order_status != 'failed'
    group by 1, 2
),

joined as (
    select
        w.region,
        w.weather_hour,
        {{ market_time('w.weather_hour', 'r.timezone') }}::date as local_date,
        extract(hour from {{ market_time('w.weather_hour', 'r.timezone') }})::int as local_hour,
        w.temperature_c,
        w.precipitation_mm,
        w.is_wet_hour,
        o.orders,
        o.revenue_usd
    from weather w
    -- Hours with no orders at all mean the simulator wasn't running, not that nobody shopped
    join hourly_orders o using (region, weather_hour)
    join {{ ref('regions') }} r on r.region_code = w.region
)

select
    {{ dbt_utils.generate_surrogate_key(['region', 'weather_hour']) }} as surrogate_key,
    *,
    avg(orders) over (partition by region, local_hour)          as typical_orders,
    orders / nullif(avg(orders) over (partition by region, local_hour), 0) as demand_index,
    current_timestamp                                           as dbt_updated_at
from joined
