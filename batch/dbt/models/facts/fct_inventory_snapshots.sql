-- models/facts/fct_inventory_snapshots.sql
-- Grain: one row per product per warehouse per day.
-- Computes running stock level from the stream of delta events.













{{
    config(
        materialized   = 'incremental',
        unique_key     = 'surrogate_key',
        incremental_strategy = 'merge',
        on_schema_change = 'append_new_columns'
    )
}}

with deltas as (
    select * from {{ ref('stg_inventory') }}

    {% if is_incremental() %}
        where occurred_at > (select max(snapshot_date) from {{ this }})
    {% endif %}
),

daily_deltas as (
    select
        product_id,
        warehouse_id,
        region,
        event_date                              as snapshot_date,
        event_year,
        event_month,
        sum(quantity_delta)                     as net_delta,
        sum(case when quantity_delta > 0
                 then quantity_delta else 0
            end)                                as total_inbound,
        sum(case when quantity_delta < 0
                 then abs(quantity_delta) else 0
            end)                                as total_outbound,
        count(case when update_type = 'restock'
                   then 1 end)                  as restock_events,
        count(case when update_type = 'damage_writeoff'
                   then 1 end)                  as damage_events

    from deltas
    group by product_id, warehouse_id, region, event_date, event_year, event_month
),

running_stock as (
    select
        *,
        -- Cumulative stock level per product per warehouse (running total)
        sum(net_delta) over (
            partition by product_id, warehouse_id
            order by snapshot_date
            rows between unbounded preceding and current row
        )                                       as stock_on_hand,

        -- 7-day rolling outbound (demand signal)
        sum(total_outbound) over (
            partition by product_id, warehouse_id
            order by snapshot_date
            rows between 6 preceding and current row
        )                                       as outbound_7d

    from daily_deltas
),

enriched as (
    select
        r.*,

        -- Stock health classification
        case
            when r.stock_on_hand <= 0              then 'Out of Stock'
            when r.stock_on_hand <= r.outbound_7d  then 'Critical'
            when r.stock_on_hand <= r.outbound_7d * 2 then 'Low'
            when r.stock_on_hand <= r.outbound_7d * 4 then 'Healthy'
            else                                        'Overstocked'
        end                                     as stock_health,

        -- Days of cover remaining
        case
            when r.outbound_7d = 0 then null
            else (r.stock_on_hand / (r.outbound_7d / 7.0))::int
        end                                     as days_of_cover,

        p.product_key,
        d.date_key,
        reg.region_key

    from running_stock r
    left join {{ ref('dim_products') }} p   using (product_id)
    left join {{ ref('dim_dates')    }} d   on d.date_id = r.snapshot_date
    left join {{ ref('dim_regions')  }} reg on reg.region_code = r.region
)

select
    {{ dbt_utils.generate_surrogate_key(
        ['product_id', 'warehouse_id', 'snapshot_date']
    ) }}                                        as surrogate_key,
    *
from enriched