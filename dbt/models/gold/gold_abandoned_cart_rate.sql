{{
  config(
    materialized='incremental',
    schema='gold',
    unique_key='date',
    incremental_strategy='merge'
  )
}}

WITH cart_adds AS (
    SELECT
        cart_id,
        session_id,
        CAST(timestamp AS DATE)    AS date,
        SUM(quantity * unit_price) AS cart_value_usd
    FROM {{ ref('silver_cart_events') }}
    WHERE event_type = 'add_to_cart'

    {% if is_incremental() %}
      AND timestamp >= DATEADD('day', -1, (SELECT MAX(date) FROM {{ this }}))
    {% endif %}

    GROUP BY 1, 2, 3
),

abandoned AS (
    SELECT DISTINCT cart_id
    FROM {{ ref('silver_cart_events') }}
    WHERE is_abandoned = TRUE
),

converted AS (
    SELECT DISTINCT ce.cart_id
    FROM {{ ref('silver_cart_events') }} ce
    JOIN {{ ref('silver_orders') }} so
        ON ce.session_id = so.session_id
    WHERE ce.event_type  = 'add_to_cart'
      AND so.event_type  = 'order_placed'
)

SELECT
    ca.date,
    COUNT(ab.cart_id)                                                         AS abandoned_carts,
    COUNT(cv.cart_id)                                                         AS converted_carts,
    ROUND(
        COUNT(ab.cart_id)::DECIMAL / NULLIF(COUNT(ca.cart_id), 0),
        4
    )                                                                         AS abandonment_rate_pct,
    ROUND(
        SUM(CASE WHEN ab.cart_id IS NOT NULL THEN ca.cart_value_usd ELSE 0 END),
        2
    )                                                                         AS revenue_lost_usd,
    CURRENT_TIMESTAMP                                                         AS _updated_at
FROM cart_adds ca
LEFT JOIN abandoned ab ON ca.cart_id = ab.cart_id
LEFT JOIN converted cv ON ca.cart_id = cv.cart_id
GROUP BY 1
