-- models/dimensions/dim_products.sql
-- One row per product derived from transaction history.
-- Product master data would come from PostgreSQL in production;
-- here we derive it from observed events.


























with product_activity as (
    select
        product_id,
        mode() within group (order by product_category)  as category,
        count(distinct transaction_id)                    as total_orders,
        sum(quantity)                                     as total_units_sold,
        sum(amount_usd)                                   as total_revenue_usd,
        avg(amount_usd)                                   as avg_selling_price_usd,
        min(amount_usd)                                   as min_price_usd,
        max(amount_usd)                                   as max_price_usd,
        min(occurred_at)                                  as first_sold_at,
        max(occurred_at)                                  as last_sold_at,
        count(distinct region)                            as regions_sold_in

    from {{ ref('stg_transactions') }}
    where order_status = 'completed'
    group by product_id
),

product_ranked as (
    select
        *,
        -- Revenue rank within category
        rank() over (
            partition by category
            order by total_revenue_usd desc
        )                                       as revenue_rank_in_category,

        -- Popularity tier
        case
            when total_units_sold >= 1000  then 'Bestseller'
            when total_units_sold >= 500   then 'Popular'
            when total_units_sold >= 100   then 'Regular'
            else                               'Niche'
        end                                     as popularity_tier

    from product_activity
)

select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
    product_id,
    category,
    popularity_tier,
    revenue_rank_in_category,
    total_orders,
    total_units_sold,
    total_revenue_usd,
    avg_selling_price_usd,
    min_price_usd,
    max_price_usd,
    regions_sold_in,
    first_sold_at,
    last_sold_at,
    current_timestamp                           as dbt_updated_at

from product_ranked