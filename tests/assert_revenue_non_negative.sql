/*
    Test: Revenue and prices must be non-negative.
    Expected result: 0 rows
*/
SELECT *
FROM {{ ref('gold_daily_city_kpis') }}
WHERE total_confirmed_revenue < 0
   OR avg_booking_price < 0
