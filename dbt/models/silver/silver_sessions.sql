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
        session_id,
        user_id,
        page_url,
        referrer,
        device_type,
        country_code,
        timestamp,
        _ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY session_id, event_type
            ORDER BY timestamp DESC
        ) AS rn
    FROM {{ source('bronze', 'raw_session_events') }}
),

base AS (
    SELECT * FROM deduped WHERE rn = 1
),

session_start AS (
    SELECT session_id, timestamp AS start_time
    FROM base
    WHERE event_type = 'session_start'
),

session_end AS (
    SELECT session_id, timestamp AS end_time
    FROM base
    WHERE event_type = 'session_end'
),

page_views AS (
    SELECT session_id, COUNT(*) AS pv_count
    FROM base
    WHERE event_type = 'page_view'
    GROUP BY session_id
),

enriched AS (
    SELECT
        b.event_id,
        b.event_type,
        b.session_id,
        b.user_id,
        b.page_url,
        b.referrer,
        b.device_type,
        b.country_code,
        b.timestamp,
        b._ingested_at,
        ss.start_time,
        se.end_time,
        COALESCE(pv.pv_count, 0) AS page_view_count,
        DATEDIFF(
            'second',
            ss.start_time,
            COALESCE(se.end_time, b.timestamp)
        ) AS session_duration_seconds
    FROM base b
    LEFT JOIN session_start ss ON b.session_id = ss.session_id
    LEFT JOIN session_end   se ON b.session_id = se.session_id
    LEFT JOIN page_views    pv ON b.session_id = pv.session_id
)

SELECT
    event_id,
    event_type,
    session_id,
    user_id,
    page_url,
    referrer,
    device_type,
    country_code,
    timestamp,
    _ingested_at,
    start_time,
    end_time,
    page_view_count,
    session_duration_seconds,
    CASE
        WHEN session_duration_seconds < 10 THEN TRUE
        WHEN page_view_count <= 1          THEN TRUE
        ELSE FALSE
    END                        AS is_bounce,
    CURRENT_TIMESTAMP          AS _transformed_at
FROM enriched
