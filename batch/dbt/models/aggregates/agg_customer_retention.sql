-- models/aggregates/agg_customer_retention.sql
-- Grain: one row per acquisition cohort (week of first order) per week since then.
-- Rebuilt in full because a new order can change any cohort's retention.

{{ config(materialized = 'table') }}

with orders as (
    select
        user_id,
        date_trunc('week', event_date)::date as order_week
    from {{ ref('fct_orders') }}
    where order_status = 'completed'
    group by user_id, date_trunc('week', event_date)::date
),

first_orders as (
    select user_id, min(order_week) as cohort_week
    from orders
    group by user_id
),

cohort_activity as (
    select
        f.cohort_week,
        (o.order_week - f.cohort_week) / 7 as weeks_since_first_order,
        count(distinct o.user_id)          as active_customers
    from orders o
    join first_orders f using (user_id)
    group by f.cohort_week, (o.order_week - f.cohort_week) / 7
),

cohort_sizes as (
    select cohort_week, count(*) as cohort_size
    from first_orders
    group by cohort_week
)

select
    {{ dbt_utils.generate_surrogate_key(['a.cohort_week', 'a.weeks_since_first_order']) }}
                                                    as surrogate_key,
    a.cohort_week,
    a.weeks_since_first_order,
    s.cohort_size,
    a.active_customers,
    round(a.active_customers::numeric / s.cohort_size * 100, 2)
                                                    as retention_pct,
    current_timestamp                               as dbt_updated_at
from cohort_activity a
join cohort_sizes s using (cohort_week)
