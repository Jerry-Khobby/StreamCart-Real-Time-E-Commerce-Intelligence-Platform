-- models/facts/fct_sessions.sql
-- Grain: one row per user session.
-- Aggregates raw clickstream events into session-level metrics.





{{
    config(
        materialized   = 'incremental',
        unique_key     = 'surrogate_key',
        incremental_strategy = 'merge',
        on_schema_change = 'append_new_columns'
    )
}}

with events as (
    select * from {{ ref('stg_clickstream') }}

    {% if is_incremental() %}
        where occurred_at > (select max(session_end_at) from {{ this }})
    {% endif %}
),

session_agg as (
    select
        session_id,
        user_id,
        region,

        min(occurred_at)                            as session_start_at,
        max(occurred_at)                            as session_end_at,
        count(event_id)                             as total_events,
        count(distinct page_type)                   as unique_pages_visited,

        -- Engagement metrics
        extract(epoch from
            max(occurred_at) - min(occurred_at)
        )::int                                      as session_duration_seconds,

        -- Funnel stages reached
        bool_or(page_type = 'product')              as reached_product_page,
        bool_or(page_type = 'cart')                 as reached_cart,
        bool_or(page_type = 'checkout')             as reached_checkout,
        bool_or(page_type = 'order_confirmation')   as converted,

        -- Content engagement
        count(case when page_type = 'product'  then 1 end)  as product_views,
        count(case when page_type = 'search'   then 1 end)  as searches,
        count(case when page_type = 'cart'     then 1 end)  as cart_events,

        -- Entry / exit
        min(event_date)                             as session_date,
        min(event_year)                             as event_year,
        min(event_month)                            as event_month,
        min(hour_of_day)                            as entry_hour,
        (array_agg(referrer order by occurred_at)
            filter (where referrer is not null)
        )[1]                                        as entry_referrer

    from events
    group by session_id, user_id, region
),

enriched as (
    select
        s.*,

        -- Session quality score (0-5)
        (
            case when s.reached_product_page then 1 else 0 end +
            case when s.searches > 0         then 1 else 0 end +
            case when s.reached_cart         then 1 else 0 end +
            case when s.reached_checkout     then 1 else 0 end +
            case when s.converted            then 1 else 0 end
        )                                           as engagement_score,

        case
            when s.session_duration_seconds < 30    then 'Bounce'
            when s.session_duration_seconds < 120   then 'Brief'
            when s.session_duration_seconds < 600   then 'Normal'
            else                                         'Deep'
        end                                         as session_length_bucket,

        -- FK lookups
        c.customer_key,
        d.date_key,
        r.region_key

    from session_agg s
    left join {{ ref('dim_customers') }} c using (user_id)
    left join {{ ref('dim_dates')     }} d on d.date_id = s.session_date
    left join {{ ref('dim_regions')   }} r on r.region_code = s.region
)

select
    {{ dbt_utils.generate_surrogate_key(['session_id']) }} as surrogate_key,
    *
from enriched