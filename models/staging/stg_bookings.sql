/*
    Staging model for bookings_raw
    
    Purpose: Light cleaning of CDC snapshot data.
    - Cast types explicitly
    - Remove exact duplicate rows (same booking_id + updated_at)
    - No business logic applied here
    
    Source: bookings_raw (CDC snapshots via Debezium -> Kafka -> Lakehouse)
    Grain: One row per booking_id x updated_at (multiple rows per booking expected)
*/

WITH source AS (
    SELECT
        booking_id,
        user_id,
        hotel_id,
        status,
        CAST(price AS DECIMAL(10,2))        AS price,
        CAST(created_at AS TIMESTAMP)       AS created_at,
        CAST(updated_at AS TIMESTAMP)       AS updated_at
    FROM {{ ref('bookings_raw') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY booking_id, updated_at
            ORDER BY updated_at
        ) AS row_num
    FROM source
)

SELECT
    booking_id,
    user_id,
    hotel_id,
    status,
    price,
    created_at,
    updated_at
FROM deduplicated
WHERE row_num = 1
