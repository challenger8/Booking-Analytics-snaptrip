/*
    ============================================================
    TABLE DEFINITIONS - Apache Iceberg on Trino/Spark
    ============================================================
    
    These DDLs define the target tables for production deployment.
    
    Why Iceberg?
    - Schema evolution without data rewrite
    - Hidden partitioning (partition transforms)
    - Time travel for auditing and rollback
    - MERGE INTO for incremental upserts
    - Partition pruning via metadata (no full scan)
    ============================================================
*/

-- ============================================================
-- STAGING TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS lakehouse.staging.stg_bookings (
    booking_id      VARCHAR,
    user_id         VARCHAR,
    hotel_id        VARCHAR,
    status          VARCHAR,
    price           DECIMAL(10, 2),
    created_at      TIMESTAMP(6),
    updated_at      TIMESTAMP(6),
    _ingested_at    TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(created_at)'],
    sorted_by = ARRAY['booking_id']
);


CREATE TABLE IF NOT EXISTS lakehouse.staging.stg_booking_events (
    booking_id      VARCHAR,
    event_type      VARCHAR,
    event_ts        TIMESTAMP(6),
    _ingested_at    TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(event_ts)'],
    sorted_by = ARRAY['booking_id']
);


CREATE TABLE IF NOT EXISTS lakehouse.staging.stg_hotels (
    hotel_id        VARCHAR,
    city            VARCHAR,
    star_rating     INTEGER
)
WITH (
    format = 'PARQUET'
    -- No partitioning: small reference table, full scan is cheap
);


-- ============================================================
-- SILVER TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS lakehouse.silver.silver_bookings (
    booking_id          VARCHAR,
    user_id             VARCHAR,
    hotel_id            VARCHAR,
    booking_status      VARCHAR,
    price               DECIMAL(10, 2),
    created_at          TIMESTAMP(6),
    confirmed_at        TIMESTAMP(6),
    cancelled_at        TIMESTAMP(6),
    last_updated_at     TIMESTAMP(6),
    _raw_snapshot_status VARCHAR,
    _raw_event_status   VARCHAR,
    _had_conflict       BOOLEAN,
    _loaded_at          TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(created_at)'],
    sorted_by = ARRAY['booking_id']
);


-- ============================================================
-- GOLD TABLE
-- ============================================================

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
    partitioning = ARRAY['booking_date']
);


-- ============================================================
-- INCREMENTAL: MERGE INTO for Silver layer (production)
-- ============================================================
/*
MERGE INTO lakehouse.silver.silver_bookings AS target
USING (
    -- <same CTE logic as silver_bookings.sql, filtered by high-water mark>
    -- WHERE snapshot_updated_at > ${high_water_mark}
    --    OR event_updated_at   > ${high_water_mark}
) AS source
ON target.booking_id = source.booking_id

WHEN MATCHED AND source.last_updated_at > target.last_updated_at THEN
    UPDATE SET
        booking_status      = source.booking_status,
        price               = source.price,
        confirmed_at        = source.confirmed_at,
        cancelled_at        = source.cancelled_at,
        last_updated_at     = source.last_updated_at,
        _raw_snapshot_status = source._raw_snapshot_status,
        _raw_event_status   = source._raw_event_status,
        _had_conflict       = source._had_conflict,
        _loaded_at          = CURRENT_TIMESTAMP

WHEN NOT MATCHED THEN
    INSERT (
        booking_id, user_id, hotel_id, booking_status, price,
        created_at, confirmed_at, cancelled_at, last_updated_at,
        _raw_snapshot_status, _raw_event_status, _had_conflict, _loaded_at
    )
    VALUES (
        source.booking_id, source.user_id, source.hotel_id, 
        source.booking_status, source.price,
        source.created_at, source.confirmed_at, source.cancelled_at, 
        source.last_updated_at,
        source._raw_snapshot_status, source._raw_event_status, 
        source._had_conflict, CURRENT_TIMESTAMP
    );
*/


-- ============================================================
-- TABLE MAINTENANCE (schedule via Airflow/cron)
-- ============================================================
/*
-- Compact small files (run daily)
ALTER TABLE lakehouse.silver.silver_bookings EXECUTE optimize;
ALTER TABLE lakehouse.gold.gold_daily_city_kpis EXECUTE optimize;

-- Expire old snapshots (keep 7 days for time-travel)
ALTER TABLE lakehouse.silver.silver_bookings EXECUTE 
    expire_snapshots(older_than => CURRENT_TIMESTAMP - INTERVAL '7' DAY);
ALTER TABLE lakehouse.gold.gold_daily_city_kpis EXECUTE 
    expire_snapshots(older_than => CURRENT_TIMESTAMP - INTERVAL '7' DAY);

-- Remove orphan files
ALTER TABLE lakehouse.silver.silver_bookings EXECUTE 
    remove_orphan_files(older_than => CURRENT_TIMESTAMP - INTERVAL '7' DAY);

-- Update table statistics for query optimizer
ANALYZE lakehouse.silver.silver_bookings;
ANALYZE lakehouse.gold.gold_daily_city_kpis;
*/
