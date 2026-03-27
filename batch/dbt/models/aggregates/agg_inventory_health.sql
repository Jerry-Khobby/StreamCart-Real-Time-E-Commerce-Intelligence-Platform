-- models/aggregates/agg_inventory_health.sql
-- Grain: one row per warehouse per day.
-- Feeds Grafana inventory risk heatmap and restock decision engine.





{{
    config(
        materialized   = 'incremental',
        unique_key     = 'surrogate_key',
        incremental_strategy = 'merge'
    )
}}

with snapshots as (
    select * from {{ ref('fct_inventory_snapshots') }}

    {% if is_incremental() %}
        where snapshot_date > (select max(snapshot_date) from {{ this }})
    {% endif %}
)

select
    {{ dbt_utils.generate_surrogate_key(['snapshot_date', 'warehouse_id', 'region']) }}
                                                    as surrogate_key,

    snapshot_date,
    event_year,
    event_month,
    warehouse_id,
    region,

    -- Stock counts
    count(product_id)                               as total_products_tracked,
    sum(stock_on_hand)                              as total_units_on_hand,

    -- Health distribution
    count(case when stock_health = 'Out of Stock'  then 1 end) as sku_out_of_stock,
    count(case when stock_health = 'Critical'      then 1 end) as sku_critical,
    count(case when stock_health = 'Low'           then 1 end) as sku_low,
    count(case when stock_health = 'Healthy'       then 1 end) as sku_healthy,
    count(case when stock_health = 'Overstocked'   then 1 end) as sku_overstocked,

    -- Risk score (0 = perfect, 100 = all SKUs out of stock)
    round(
        (
            count(case when stock_health = 'Out of Stock' then 1 end) * 100.0 +
            count(case when stock_health = 'Critical'     then 1 end) * 60.0  +
            count(case when stock_health = 'Low'          then 1 end) * 30.0
        ) / nullif(count(product_id), 0)
    , 1)                                            as inventory_risk_score,

    -- Movement
    sum(total_outbound)                             as total_units_shipped,
    sum(total_inbound)                              as total_units_received,
    sum(restock_events)                             as restock_events,
    sum(damage_events)                              as damage_events,

    -- Average days of cover across SKUs (nulls excluded)
    round(avg(days_of_cover), 1)                    as avg_days_of_cover,
    min(days_of_cover)                              as min_days_of_cover,

    current_timestamp                               as dbt_updated_at

from snapshots
group by snapshot_date, event_year, event_month, warehouse_id, region