{{
  config(
    materialized='incremental',
    schema='gold',
    unique_key="date || '|' || country_code || '|' || payment_method",
    incremental_strategy='merge'
  )
}}

WITH placed AS (
    SELECT
        order_id,
        CAST(timestamp AS DATE) AS date,
        country_code,
        payment_method,
        total_amount_usd
    FROM {{ ref('silver_orders') }}
    WHERE event_type = 'order_placed'

    {% if is_incremental() %}
      AND timestamp >= DATEADD('day', -1, (SELECT MAX(date) FROM {{ this }}))
    {% endif %}
),

refunded AS (
    SELECT DISTINCT order_id
    FROM {{ ref('silver_orders') }}
    WHERE event_type = 'order_refunded'
)

SELECT
    p.date,
    p.country_code,
    p.payment_method,
    COUNT(DISTINCT p.order_id)                                              AS total_orders,
    ROUND(SUM(p.total_amount_usd), 2)                                       AS total_revenue_usd,
    ROUND(AVG(p.total_amount_usd), 2)                                       AS avg_order_value_usd,
    ROUND(
        COUNT(r.order_id)::DECIMAL / NULLIF(COUNT(DISTINCT p.order_id), 0),
        4
    )                                                                       AS refund_rate,
    CURRENT_TIMESTAMP                                                       AS _updated_at
FROM placed p
LEFT JOIN refunded r ON p.order_id = r.order_id
GROUP BY 1, 2, 3
