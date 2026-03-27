-- models/aggregates/agg_funnel_conversion.sql
-- Grain: one row per region per day.
-- Tracks the e-commerce funnel from session start → conversion.
-- Powers the Superset "Funnel Conversion Rates" dashboard.















{{
    config(
        materialized   = 'incremental',
        unique_key     = 'surrogate_key',
        incremental_strategy = 'merge'
    )
}}

with sessions as (
    select * from {{ ref('fct_sessions') }}

    {% if is_incremental() %}
        where session_date > (select max(event_date) from {{ this }})
    {% endif %}
)

select
    {{ dbt_utils.generate_surrogate_key(['session_date', 'region']) }} as surrogate_key,

    session_date                                    as event_date,
    event_year,
    event_month,
    region,

    -- Funnel volumes (each stage is a subset of the one above)
    count(session_id)                               as sessions_started,
    count(case when reached_product_page  then 1 end) as sessions_reached_product,
    count(case when reached_cart          then 1 end) as sessions_reached_cart,
    count(case when reached_checkout      then 1 end) as sessions_reached_checkout,
    count(case when converted             then 1 end) as sessions_converted,

    -- Stage-to-stage conversion rates (%)
    round(
        count(case when reached_product_page then 1 end)::numeric
        / nullif(count(session_id), 0) * 100, 2
    )                                               as browse_rate_pct,

    round(
        count(case when reached_cart then 1 end)::numeric
        / nullif(count(case when reached_product_page then 1 end), 0) * 100, 2
    )                                               as add_to_cart_rate_pct,

    round(
        count(case when reached_checkout then 1 end)::numeric
        / nullif(count(case when reached_cart then 1 end), 0) * 100, 2
    )                                               as checkout_rate_pct,

    round(
        count(case when converted then 1 end)::numeric
        / nullif(count(case when reached_checkout then 1 end), 0) * 100, 2
    )                                               as purchase_rate_pct,

    -- Overall session → purchase rate
    round(
        count(case when converted then 1 end)::numeric
        / nullif(count(session_id), 0) * 100, 2
    )                                               as overall_conversion_rate_pct,

    -- Cart abandonment (sessions that reached cart but didn't convert)
    round(
        (count(case when reached_cart then 1 end) -
         count(case when converted    then 1 end))::numeric
        / nullif(count(case when reached_cart then 1 end), 0) * 100, 2
    )                                               as cart_abandonment_rate_pct,

    -- Engagement
    round(avg(session_duration_seconds), 0)         as avg_session_duration_seconds,
    round(avg(engagement_score), 2)                 as avg_engagement_score,
    count(case when session_length_bucket = 'Bounce' then 1 end) as bounce_sessions,
    round(
        count(case when session_length_bucket = 'Bounce' then 1 end)::numeric
        / nullif(count(session_id), 0) * 100, 2
    )                                               as bounce_rate_pct,

    current_timestamp                               as dbt_updated_at

from sessions
group by session_date, event_year, event_month, region