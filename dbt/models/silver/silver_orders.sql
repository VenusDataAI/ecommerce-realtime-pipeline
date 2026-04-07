{{
  config(
    materialized='table',
    schema='silver'
  )
}}

WITH ranked AS (
    SELECT
        event_id,
        event_type,
        order_id,
        user_id,
        session_id,
        items,
        currency,
        total_amount,
        discount_amount,
        payment_method,
        country_code,
        timestamp,
        _ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY timestamp DESC
        ) AS rn
    FROM {{ source('bronze', 'raw_order_events') }}
),

deduped AS (
    SELECT * FROM ranked WHERE rn = 1
),

-- Normalise total_amount to USD using static exchange rates
with_usd AS (
    SELECT
        event_id,
        event_type,
        order_id,
        user_id,
        session_id,
        items,
        currency,
        total_amount,
        discount_amount,
        payment_method,
        country_code,
        timestamp,
        _ingested_at,
        ROUND(
            total_amount * {{ safe_cast_float(
                "CASE currency
                    WHEN 'USD' THEN '1.00'
                    WHEN 'EUR' THEN '1.08'
                    WHEN 'GBP' THEN '1.26'
                    WHEN 'BRL' THEN '0.19'
                    WHEN 'CAD' THEN '0.74'
                    WHEN 'AUD' THEN '0.65'
                    WHEN 'JPY' THEN '0.0066'
                    WHEN 'MXN' THEN '0.057'
                    ELSE '1.00'
                END"
            ) }},
            2
        ) AS total_amount_usd,
        CURRENT_TIMESTAMP AS _transformed_at
    FROM deduped
)

SELECT * FROM with_usd
