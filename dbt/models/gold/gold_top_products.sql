{{
  config(
    materialized='incremental',
    schema='gold',
    unique_key="date || '|' || product_id",
    incremental_strategy='merge'
  )
}}

/*
  Flatten order line items from the SUPER column (Redshift) or JSON (other engines).
  The lateral join syntax below is Redshift-specific.
  For Snowflake use FLATTEN(input => items), for BigQuery use UNNEST.
*/
WITH order_items AS (
    SELECT
        CAST(so.timestamp AS DATE)       AS date,
        item.product_id::VARCHAR         AS product_id,
        item.quantity::INTEGER           AS quantity,
        item.unit_price::DECIMAL(10, 2)  AS unit_price
    FROM {{ ref('silver_orders') }} so,
         so.items AS item          -- Redshift SUPER lateral unnest
    WHERE so.event_type = 'order_placed'

    {% if is_incremental() %}
      AND so.timestamp >= DATEADD('day', -1, (SELECT MAX(date) FROM {{ this }}))
    {% endif %}
),

product_daily AS (
    SELECT
        oi.date,
        oi.product_id,
        pc.product_name,
        SUM(oi.quantity)                           AS units_sold,
        ROUND(SUM(oi.quantity * oi.unit_price), 2) AS total_revenue_usd
    FROM order_items oi
    LEFT JOIN {{ ref('product_catalog') }} pc
           ON oi.product_id = pc.product_id
    GROUP BY 1, 2, 3
)

SELECT
    date,
    product_id,
    product_name,
    units_sold,
    total_revenue_usd,
    DENSE_RANK() OVER (
        PARTITION BY date
        ORDER BY total_revenue_usd DESC
    )                   AS rank,
    CURRENT_TIMESTAMP   AS _updated_at
FROM product_daily
