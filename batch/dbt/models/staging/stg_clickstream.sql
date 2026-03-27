-- models/staging/stg_clickstream.sql
-- Thin view over silver.clickstream.
-- session_id_silver is the stitched session from the Silver processor;
-- we use that here instead of the raw session_id.








with source as (
    select * from {{ source('silver', 'clickstream') }}
),

renamed as (
    select
        event_id,
        coalesce(session_id_silver, session_id)  as session_id,  -- prefer stitched
        user_id,
        region,
        page_type,
        product_id,
        search_query,
        referrer,
        user_agent,

        event_time::timestamptz                  as occurred_at,
        date_trunc('day', event_time)::date      as event_date,
        extract(year  from event_time)::int      as event_year,
        extract(month from event_time)::int      as event_month,
        extract(hour  from event_time)::int      as hour_of_day,

        -- Derived: is this a conversion event?
        case
            when page_type = 'order_confirmation' then true
            else false
        end                                      as is_conversion

    from source
)

select * from renamed