/*
    Test: Total bookings in Silver must match sum in Gold.
    Cross-layer consistency check.
    Expected result: 0 rows
*/
WITH silver_count AS (
    SELECT COUNT(*) AS cnt FROM {{ ref('silver_bookings') }}
),
gold_sum AS (
    SELECT COALESCE(SUM(total_bookings), 0) AS cnt FROM {{ ref('gold_daily_city_kpis') }}
)
SELECT 
    s.cnt AS silver_count,
    g.cnt AS gold_total
FROM silver_count s
CROSS JOIN gold_sum g
WHERE s.cnt != g.cnt
