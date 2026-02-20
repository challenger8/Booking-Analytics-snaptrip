/*
    Test: Cancellation rate must be between 0 and 1.
    Expected result: 0 rows
*/
SELECT *
FROM {{ ref('gold_daily_city_kpis') }}
WHERE cancellation_rate < 0 
   OR cancellation_rate > 1
