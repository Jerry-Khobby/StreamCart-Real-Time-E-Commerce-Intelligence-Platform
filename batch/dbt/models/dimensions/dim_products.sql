-- models/dimensions/dim_customers.sql
-- One row per customer derived from transaction history.
-- SCD Type 1 (overwrite) — for SCD2 you'd add valid_from/valid_to
-- using dbt snapshots (see snapshots/ folder).






with customer_activity as (
    select
        user_id,
        min(occurred_at)                        as first_seen_at,
        max(occurred_at)                        as last_seen_at,
        count(distinct transaction_id)          as lifetime_order_count,
        sum(amount_usd)                         as lifetime_revenue_usd,
        avg(amount_usd)                         as avg_order_value_usd,
        mode() within group (order by region)   as primary_region,
        mode() within group (
            order by payment_method
        )                                       as preferred_payment_method,

        -- Recency bucket (days since last order)
        current_date - max(event_date)          as days_since_last_order

    from {{ ref('stg_transactions') }}
    where order_status = 'completed'
    group by user_id
),

customer_segments as (
    select
        *,
        case
            when lifetime_revenue_usd >= 5000                  then 'VIP'
            when lifetime_revenue_usd >= 1000                  then 'High Value'
            when lifetime_revenue_usd >= 200                   then 'Mid Tier'
            else                                                    'Low Value'
        end                                     as value_segment,

        case
            when days_since_last_order <= 30   then 'Active'
            when days_since_last_order <= 90   then 'At Risk'
            when days_since_last_order <= 180  then 'Lapsing'
            else                                    'Churned'
        end                                     as recency_segment

    from customer_activity
)

select
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} as customer_key,
    user_id,
    primary_region,
    preferred_payment_method,
    value_segment,
    recency_segment,
    first_seen_at,
    last_seen_at,
    days_since_last_order,
    lifetime_order_count,
    lifetime_revenue_usd,
    avg_order_value_usd,
    current_timestamp                           as dbt_updated_at

from customer_segments