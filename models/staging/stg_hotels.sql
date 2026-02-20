/*
    Staging model for hotels_raw
    
    Purpose: Light cleaning of hotel reference/dimension data.
    - Cast types
    - Deduplicate by hotel_id (take the first occurrence)
    
    Source: hotels_raw (reference data, assumed slowly changing)
    Grain: One row per hotel_id
*/

WITH source AS (
    SELECT
        hotel_id,
        city,
        CAST(star_rating AS INT) AS star_rating
    FROM {{ ref('hotels_raw') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY hotel_id
            ORDER BY hotel_id
        ) AS row_num
    FROM source
)

SELECT
    hotel_id,
    city,
    star_rating
FROM deduplicated
WHERE row_num = 1
