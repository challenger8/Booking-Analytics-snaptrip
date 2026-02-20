/*
    Test: All resolved statuses must be in the valid enum.
    Expected result: 0 rows
*/
SELECT *
FROM {{ ref('silver_bookings') }}
WHERE booking_status NOT IN ('created', 'confirmed', 'cancelled')
