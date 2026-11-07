-- models/staging/stg_inventory.sql

with source as (
    select s.*, r.timezone
    from {{ source('silver', 'inventory') }} s
    left join {{ ref('regions') }} r on r.region_code = s.region
),

renamed as (
    select
        event_id,
        product_id,
        warehouse_id,
        region,
        update_type,
        quantity_delta,
        stock_after,
        reorder_point,

        {{ utc_instant('event_time') }}                              as occurred_at,
        {{ market_time('event_time', 'timezone') }}::date            as event_date,
        extract(year  from {{ market_time('event_time', 'timezone') }})::int as event_year,
        extract(month from {{ market_time('event_time', 'timezone') }})::int as event_month,

        -- Classify direction of movement
        case
            when quantity_delta > 0 then 'inbound'
            when quantity_delta < 0 then 'outbound'
            else 'neutral'
        end                                     as movement_direction,

        abs(quantity_delta)                     as quantity_abs

    from source
)

select * from renamed
