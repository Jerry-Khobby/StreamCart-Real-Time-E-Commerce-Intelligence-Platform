{#
    Silver stores UTC without a time zone. These turn it into the exact instant, and
    into the wall-clock time of the market it happened in, using the regions seed.
    Business dates and hours use market time, so a day in Singapore is one day in the
    reports, not split across two UTC dates.
#}

{% macro utc_instant(column) -%}
    ({{ column }} at time zone 'UTC')
{%- endmacro %}

{% macro market_time(column, timezone_column) -%}
    (({{ column }} at time zone 'UTC') at time zone {{ timezone_column }})
{%- endmacro %}
