-- models/staging/stg_clickstream.sql
-- Thin view over silver.clickstream.
-- session_id_silver is the stitched session from the Silver processor;
-- we use that here instead of the raw session_id.

with source as (
    select s.*, r.timezone
    from {{ source('silver', 'clickstream') }} s
    left join {{ ref('regions') }} r on r.region_code = s.region
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

        {{ utc_instant('event_time') }}                              as occurred_at,
        {{ market_time('event_time', 'timezone') }}::date            as event_date,
        extract(year  from {{ market_time('event_time', 'timezone') }})::int as event_year,
        extract(month from {{ market_time('event_time', 'timezone') }})::int as event_month,
        extract(hour  from {{ market_time('event_time', 'timezone') }})::int as hour_of_day,

        -- Derived: is this a conversion event?
        case
            when page_type = 'order_confirmation' then true
            else false
        end                                      as is_conversion

    from source
)

select * from renamed
