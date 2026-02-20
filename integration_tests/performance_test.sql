-- Performance baseline test
-- This query should complete within 120 seconds on test data
-- Simulates the full Silver + Gold pipeline query pattern

SELECT 
    g.booking_date,
    g.city,
    g.total_bookings,
    g.cancellation_rate,
    g.total_confirmed_revenue
FROM lakehouse.gold.gold_daily_city_kpis g
WHERE g.booking_date >= DATE '2024-01-01'
ORDER BY g.booking_date, g.city;
