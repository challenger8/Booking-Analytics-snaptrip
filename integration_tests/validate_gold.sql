-- Integration test: Validate Gold layer output on Trino/Iceberg

-- Test 1: Counts add up
SELECT
    CASE
        WHEN COUNT(*) = 0
        THEN 'PASS: Counts are consistent'
        ELSE 'FAIL: Count mismatch detected'
    END AS test_result
FROM lakehouse.gold.gold_daily_city_kpis
WHERE total_bookings != confirmed_bookings + cancelled_bookings + pending_bookings;

-- Test 2: Cancellation rate bounds
SELECT
    CASE
        WHEN COUNT(*) = 0
        THEN 'PASS: Cancellation rates in bounds'
        ELSE 'FAIL: Cancellation rate out of bounds'
    END AS test_result
FROM lakehouse.gold.gold_daily_city_kpis
WHERE cancellation_rate < 0 OR cancellation_rate > 1;

-- Test 3: Non-negative revenue
SELECT
    CASE
        WHEN COUNT(*) = 0
        THEN 'PASS: Revenue non-negative'
        ELSE 'FAIL: Negative revenue found'
    END AS test_result
FROM lakehouse.gold.gold_daily_city_kpis
WHERE total_confirmed_revenue < 0;
