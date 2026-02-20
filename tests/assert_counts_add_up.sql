/*
    Test: confirmed + cancelled + pending must equal total.
    Expected result: 0 rows
*/
SELECT *
FROM {{ ref('gold_daily_city_kpis') }}
WHERE total_bookings != confirmed_bookings + cancelled_bookings + pending_bookings
