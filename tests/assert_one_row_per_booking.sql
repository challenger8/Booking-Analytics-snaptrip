/*
    Test: Silver table must have exactly one row per booking_id.
    Expected result: 0 rows (no duplicates)
*/
SELECT
    booking_id,
    COUNT(*) AS cnt
FROM {{ ref('silver_bookings') }}
GROUP BY booking_id
HAVING COUNT(*) > 1
