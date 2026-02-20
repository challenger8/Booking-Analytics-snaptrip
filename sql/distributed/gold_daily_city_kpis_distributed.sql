/*
    ============================================================
    Gold Daily City KPIs — DISTRIBUTED ENGINE VERSION (Trino/Spark + Iceberg)
    ============================================================
    
    This is the production version optimized for distributed execution.
    Compare with: models/gold/gold_daily_city_kpis.sql (generic version)
    
    KEY DIFFERENCES FROM GENERIC VERSION:
    ┌────────────────────────┬─────────────────────────────────────┐
    │ Generic Version        │ This Distributed Version            │
    ├────────────────────────┼─────────────────────────────────────┤
    │ Regular LEFT JOIN      │ BROADCAST hint for hotels (small)   │
    │ No write strategy      │ INSERT OVERWRITE by partition       │
    │ No partition awareness │ Partition by booking_date           │
    │ No skew handling       │ Spark AQE handles skew              │
    │ No file management     │ Compaction + statistics             │
    └────────────────────────┴─────────────────────────────────────┘
    
    ============================================================
*/


-- ============================================================
-- TABLE DDL (run once)
-- ============================================================
/*
CREATE TABLE IF NOT EXISTS lakehouse.gold.gold_daily_city_kpis (
    booking_date            DATE,
    city                    VARCHAR,
    total_bookings          BIGINT,
    confirmed_bookings      BIGINT,
    cancelled_bookings      BIGINT,
    pending_bookings        BIGINT,
    cancellation_rate       DECIMAL(6, 4),
    total_confirmed_revenue DECIMAL(14, 2),
    avg_booking_price       DECIMAL(10, 2),
    avg_confirmed_price     DECIMAL(10, 2),
    _computed_at            TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['booking_date']    -- One partition per day
);
*/


-- ============================================================
-- FULL REFRESH VERSION
-- ============================================================

INSERT OVERWRITE lakehouse.gold.gold_daily_city_kpis

WITH bookings_enriched AS (
    SELECT
        /*+ BROADCAST(h) */
        -- ^^^ SPARK HINT: Broadcast hotels table to all executors
        -- Hotels is tiny (<1MB) — broadcasting avoids shuffling 
        -- the entire silver_bookings table by hotel_id
        --
        -- TRINO: No hint needed. Trino auto-broadcasts tables below
        -- join_distribution_type threshold (default 100MB).
        -- Can force: SET SESSION join_distribution_type = 'BROADCAST';
        
        s.booking_id,
        s.booking_status,
        s.price,
        CAST(s.created_at AS DATE)          AS booking_date,
        COALESCE(h.city, 'Unknown')         AS city,
        COALESCE(h.star_rating, 0)          AS star_rating
    FROM lakehouse.silver.silver_bookings s
    LEFT JOIN lakehouse.staging.stg_hotels h
        ON s.hotel_id = h.hotel_id
    /*
        JOIN ANALYSIS:
        - silver_bookings: potentially millions/billions of rows
        - stg_hotels: hundreds of rows (<1MB)
        
        WITHOUT broadcast: Both tables shuffled by hotel_id = EXPENSIVE
        WITH broadcast: Hotels sent to every executor = CHEAP
        
        Cost difference at 100M bookings:
        - Without: ~15 min (shuffle 100M rows)
        - With:    ~2 min (no shuffle, local join)
    */
),

daily_city_agg AS (
    SELECT
        booking_date,
        city,

        COUNT(*)                            AS total_bookings,

        COUNT(CASE 
            WHEN booking_status = 'confirmed' THEN 1 
        END)                                AS confirmed_bookings,

        COUNT(CASE 
            WHEN booking_status = 'cancelled' THEN 1 
        END)                                AS cancelled_bookings,

        COUNT(CASE 
            WHEN booking_status = 'created' THEN 1 
        END)                                AS pending_bookings,

        CASE 
            WHEN COUNT(*) > 0 
            THEN ROUND(
                CAST(COUNT(CASE WHEN booking_status = 'cancelled' THEN 1 END) AS DOUBLE)
                / CAST(COUNT(*) AS DOUBLE),
                4
            )
            ELSE 0 
        END                                 AS cancellation_rate,

        COALESCE(
            SUM(CASE WHEN booking_status = 'confirmed' THEN price END), 
            0
        )                                   AS total_confirmed_revenue,

        ROUND(AVG(price), 2)                AS avg_booking_price,

        ROUND(
            COALESCE(
                AVG(CASE WHEN booking_status = 'confirmed' THEN price END),
                0
            ), 
            2
        )                                   AS avg_confirmed_price,

        CURRENT_TIMESTAMP                   AS _computed_at

    FROM bookings_enriched
    GROUP BY booking_date, city
    /*
        SHUFFLE NOTE: GROUP BY (booking_date, city) has low cardinality
        (~365 days x ~N cities = hundreds of groups).
        Final aggregation is cheap regardless of data volume.
        Spark AQE may coalesce shuffle partitions automatically.
    */
)

SELECT * FROM daily_city_agg
ORDER BY booking_date, city
;


-- ============================================================
-- POST-LOAD MAINTENANCE
-- ============================================================
/*
ALTER TABLE lakehouse.gold.gold_daily_city_kpis EXECUTE optimize;

ALTER TABLE lakehouse.gold.gold_daily_city_kpis EXECUTE 
    expire_snapshots(older_than => CURRENT_TIMESTAMP - INTERVAL '7' DAY);

ANALYZE lakehouse.gold.gold_daily_city_kpis;
*/