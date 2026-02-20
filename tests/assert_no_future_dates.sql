/*
    Test: No bookings should have future created_at dates.
    Expected result: 0 rows
*/
SELECT *
FROM {{ ref('silver_bookings') }}
WHERE CAST(created_at AS DATE) > CURRENT_DATE
