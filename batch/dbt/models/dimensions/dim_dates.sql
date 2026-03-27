-- models/dimensions/dim_dates.sql
-- Standard date dimension spanning 5 years (2023-01-01 → 2027-12-31).
-- Generated entirely in SQL — no seed file needed.






with date_spine as (
    {{ dbt_utils.date_spine(
        datepart = "day",
        start_date = "cast('2023-01-01' as date)",
        end_date   = "cast('2027-12-31' as date)"
    ) }}
),

enriched as (
    select
        date_day                                            as date_id,
        date_day,

        -- Calendar attributes
        extract(year  from date_day)::int                   as year,
        extract(month from date_day)::int                   as month_number,
        to_char(date_day, 'Month')                          as month_name,
        extract(quarter from date_day)::int                 as quarter,
        extract(week  from date_day)::int                   as week_of_year,
        extract(dow   from date_day)::int                   as day_of_week,   -- 0=Sunday
        to_char(date_day, 'Day')                            as day_name,
        extract(day   from date_day)::int                   as day_of_month,
        extract(doy   from date_day)::int                   as day_of_year,

        -- Flags
        case when extract(dow from date_day) in (0, 6)
             then true else false end                        as is_weekend,

        case when extract(dow from date_day) not in (0, 6)
             then true else false end                        as is_weekday,

        -- Fiscal quarter (assume fiscal year = calendar year)
        'FY' || extract(year from date_day)::text
            || '-Q' || extract(quarter from date_day)::text as fiscal_quarter,

        -- Retail season
        case
            when extract(month from date_day) in (12, 1, 2)  then 'Winter'
            when extract(month from date_day) in (3, 4, 5)   then 'Spring'
            when extract(month from date_day) in (6, 7, 8)   then 'Summer'
            else                                                   'Autumn'
        end                                                 as retail_season,

        -- Key shopping periods
        case
            when to_char(date_day, 'MM-DD') between '11-25' and '11-30' then 'Black Friday Week'
            when to_char(date_day, 'MM-DD') between '12-20' and '12-26' then 'Christmas Week'
            when to_char(date_day, 'MM-DD') between '07-10' and '07-21' then 'Prime Day Season'
            else null
        end                                                 as shopping_event

    from date_spine
)

select
    {{ dbt_utils.generate_surrogate_key(['date_id']) }} as date_key,
    *
from enriched