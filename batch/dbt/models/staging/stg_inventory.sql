-- models / staging / stg_inventory.sql 

WITH source AS (
  SELECT
    *
  FROM
    {{ source(
      'silver',
      'inventory'
    ) }}
),
renamed AS (
  SELECT
    event_id,
    product_id,
    warehouse_id,
    region,
    update_type,
    quantity_delta,
    event_time :: timestamptz AS occurred_at,
    DATE_TRUNC(
      'day',
      event_time
    ) :: DATE AS event_date,
    EXTRACT(
      YEAR
      FROM
        event_time
    ) :: INT AS event_year,
    EXTRACT(
      MONTH
      FROM
        event_time
    ) :: INT AS event_month,-- classify direction OF movement CASE
      WHEN quantity_delta > 0 THEN 'inbound'
      WHEN quantity_delta < 0 THEN 'outbound'
    END AS movement_direction,
    ABS(quantity_delta) AS quantity_abs
  FROM
    source
)
SELECT
  *
FROM
  renamed
