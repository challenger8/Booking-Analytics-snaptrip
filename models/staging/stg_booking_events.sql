/*
    Staging model for booking_events_raw
    
    Purpose: Light cleaning of append-only event stream.
    - Cast types
    - Deduplicate exact duplicate events (same booking_id + event_type + event_ts)
    
    Source: booking_events_raw (append-only event log)
    Grain: One row per booking_id x event_type x event_ts
*/

WITH source AS (
    SELECT
        booking_id,
        event_type,
        CAST(event_ts AS TIMESTAMP) AS event_ts
    FROM {{ ref('booking_events_raw') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY booking_id, event_type, event_ts
            ORDER BY event_ts
        ) AS row_num
    FROM source
)

SELECT
    booking_id,
    event_type,
    event_ts
FROM deduplicated
WHERE row_num = 1
