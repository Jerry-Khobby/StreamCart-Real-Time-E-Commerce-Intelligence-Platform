-- models/staging/stg_inventory.sql







with source as (
    select * from {{ source('silver', 'inventory') }}
),

renamed as (
    select
        event_id,
        product_id,
        warehouse_id,
        region,
        update_type,
        quantity_delta,

        event_time::timestamptz                 as occurred_at,
        date_trunc('day', event_time)::date     as event_date,
        extract(year  from event_time)::int     as event_year,
        extract(month from event_time)::int     as event_month,

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