-- Integration test: Validate Silver layer output on Trino/Iceberg

-- Test 1: Row count
SELECT 
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT booking_id) 
        THEN 'PASS: One row per booking' 
        ELSE 'FAIL: Duplicate booking_ids found' 
    END AS test_result
FROM lakehouse.silver.silver_bookings;

-- Test 2: Valid statuses
SELECT
    CASE
        WHEN COUNT(*) = 0
        THEN 'PASS: All statuses valid'
        ELSE 'FAIL: Invalid status values found'
    END AS test_result
FROM lakehouse.silver.silver_bookings
WHERE booking_status NOT IN ('created', 'confirmed', 'cancelled');

-- Test 3: No NULL keys
SELECT
    CASE
        WHEN COUNT(*) = 0
        THEN 'PASS: No NULL booking_ids'
        ELSE 'FAIL: NULL booking_ids found'
    END AS test_result
FROM lakehouse.silver.silver_bookings
WHERE booking_id IS NULL;
