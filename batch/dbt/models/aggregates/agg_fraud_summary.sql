-- models/aggregates/agg_fraud_summary.sql
-- Grain: one row per region per day.
-- Applies the real-time layer's fraud signals to the complete, corrected order history.
-- This is the BATCH version — for post-hoc analysis, not real-time blocking.

{{
    config(
        materialized   = 'incremental',
        unique_key     = 'surrogate_key',
        incremental_strategy = 'merge'
    )
}}

with orders as (
    select * from {{ ref('fct_orders') }}

    {% if is_incremental() %}
        where event_date >= (select max(event_date) - 3 from {{ this }})
    {% endif %}
),

-- Velocity: count transactions per user per hour
user_hourly_counts as (
    select
        user_id,
        event_date,
        hour_of_day,
        count(transaction_id)   as tx_count_in_hour,
        count(*) filter (where amount_usd < 10) as small_tx_count_in_hour,
        sum(amount_usd)         as hourly_spend_usd
    from orders
    group by user_id, event_date, hour_of_day
),

-- Tag each order with fraud signals
flagged_orders as (
    select
        o.*,

        -- Signal 1: high-value order within 60 seconds of the session starting
        coalesce(o.amount_usd > 5000 and o.session_age_seconds < 60, false)
                                                        as flag_high_value,

        -- Signal 2: velocity breach, more than 10 orders by this user in the hour
        uh.tx_count_in_hour > 10                        as flag_velocity,

        -- Signal 3: card testing, a burst of small probing charges
        o.amount_usd < 10 and uh.small_tx_count_in_hour >= 5
                                                        as flag_card_testing,

        -- Signal 4: order placed from outside the customer's home market
        coalesce(o.ip_region <> o.region, false)        as flag_geo_anomaly,

        uh.tx_count_in_hour,
        uh.hourly_spend_usd

    from orders o
    left join user_hourly_counts uh
        on  uh.user_id    = o.user_id
        and uh.event_date = o.event_date
        and uh.hour_of_day = o.hour_of_day
),

daily_summary as (
    select
        event_date,
        event_year,
        event_month,
        region,

        count(transaction_id)                           as total_orders,
        sum(amount_usd)                                 as total_revenue_usd,

        -- Fraud signal counts
        count(case when flag_high_value   then 1 end)   as high_value_alerts,
        count(case when flag_velocity     then 1 end)   as velocity_alerts,
        count(case when flag_card_testing then 1 end)   as card_testing_alerts,
        count(case when flag_geo_anomaly  then 1 end)   as geo_anomaly_alerts,

        count(case when flag_high_value
                     or flag_velocity
                     or flag_card_testing
                     or flag_geo_anomaly
                   then 1 end)                          as total_flagged_orders,

        sum(case when flag_high_value
                   or flag_velocity
                   or flag_card_testing
                     or flag_geo_anomaly
                 then amount_usd else 0 end)            as flagged_revenue_at_risk_usd,

        -- Fraud rate
        round(
            count(case when flag_high_value
                          or flag_velocity
                          or flag_card_testing
                     or flag_geo_anomaly
                        then 1 end)::numeric
            / nullif(count(transaction_id), 0) * 100, 4
        )                                               as fraud_rate_pct,

        -- Distinct users flagged
        count(distinct case when flag_high_value
                              or flag_velocity
                              or flag_card_testing
                     or flag_geo_anomaly
                            then user_id end)           as flagged_users,

        current_timestamp                               as dbt_updated_at

    from flagged_orders
    group by event_date, event_year, event_month, region
)

select
    {{ dbt_utils.generate_surrogate_key(['event_date', 'region']) }} as surrogate_key,
    *
from daily_summary
