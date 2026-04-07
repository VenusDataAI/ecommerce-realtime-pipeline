{{
  config(
    materialized='table',
    schema='silver'
  )
}}

WITH deduped AS (
    SELECT
        event_id,
        event_type,
        cart_id,
        session_id,
        user_id,
        product_id,
        quantity,
        unit_price,
        timestamp,
        _ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY cart_id, product_id, event_type
            ORDER BY timestamp DESC
        ) AS rn
    FROM {{ source('bronze', 'raw_cart_events') }}
),

base AS (
    SELECT * FROM deduped WHERE rn = 1
),

-- Sessions that converted (have a corresponding order_placed)
converted_sessions AS (
    SELECT DISTINCT session_id
    FROM {{ source('bronze', 'raw_order_events') }}
    WHERE event_type = 'order_placed'
),

-- Carts that had add_to_cart but whose session never converted within 24h
abandoned_carts AS (
    SELECT DISTINCT b.cart_id
    FROM base b
    LEFT JOIN converted_sessions cs ON b.session_id = cs.session_id
    WHERE b.event_type = 'add_to_cart'
      AND (
            cs.session_id IS NULL
            OR DATEDIFF('hour', b.timestamp, CURRENT_TIMESTAMP) > 24
          )
)

SELECT
    b.event_id,
    b.event_type,
    b.cart_id,
    b.session_id,
    b.user_id,
    b.product_id,
    b.quantity,
    b.unit_price,
    b.timestamp,
    b._ingested_at,
    CASE WHEN ab.cart_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_abandoned,
    CURRENT_TIMESTAMP AS _transformed_at
FROM base b
LEFT JOIN abandoned_carts ab ON b.cart_id = ab.cart_id
