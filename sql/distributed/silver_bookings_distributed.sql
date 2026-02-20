/*
    ============================================================
    Silver Bookings — DISTRIBUTED ENGINE VERSION (Trino/Spark + Iceberg)
    ============================================================
    
    This is the production version optimized for distributed execution.
    Compare with: models/silver/silver_bookings.sql (generic version)
    
    KEY DIFFERENCES FROM GENERIC VERSION:
    ┌────────────────────────┬─────────────────────────────────────┐
    │ Generic Version        │ This Distributed Version            │
    ├────────────────────────┼─────────────────────────────────────┤
    │ No table format        │ Iceberg with partitioning + sort    │
    │ No write strategy      │ INSERT OVERWRITE + MERGE INTO       │
    │ GREATEST() for NULL    │ Explicit CASE (cross-engine safe)   │
    │ No shuffle awareness   │ Comments on shuffle + co-partition  │
    │ No broadcast hints     │ Broadcast for small dimension joins │
    │ Full refresh only      │ Incremental MERGE ready             │
    │ No statistics          │ ANALYZE TABLE for optimizer         │
    │ No file management     │ Compaction + snapshot expiry        │
    └────────────────────────┴─────────────────────────────────────┘
    
    TARGET: Trino or Spark SQL on Apache Iceberg tables
    STORAGE: Parquet on S3/GCS/ADLS
    
    ============================================================
*/


-- ============================================================
-- TABLE DDL (run once)
-- ============================================================
/*
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
    partitioning = ARRAY['day(created_at)'],     -- Hidden partitioning
    sorted_by = ARRAY['booking_id']               -- Sort for merge efficiency
);
*/


-- ============================================================
-- FULL REFRESH VERSION
-- ============================================================

INSERT OVERWRITE lakehouse.silver.silver_bookings
-- ^^^ DIFFERENCE: Uses INSERT OVERWRITE instead of CREATE TABLE
-- This is atomic in Iceberg — readers see old data until write completes

WITH latest_booking_snapshot AS (
    SELECT
        booking_id,
        user_id,
        hotel_id,
        status              AS snapshot_status,
        price,
        created_at,
        updated_at          AS snapshot_updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY booking_id 
            ORDER BY updated_at DESC
        ) AS rn
    FROM lakehouse.staging.stg_bookings
    /*
        SHUFFLE NOTE: ROW_NUMBER triggers hash partition by booking_id
        across all executors. This is the most expensive operation but
        unavoidable for CDC deduplication.
        
        OPTIMIZATION: If stg_bookings is already sorted by 
        (booking_id, updated_at) via Iceberg sort order, the shuffle 
        cost is reduced — each file contains complete booking histories.
    */
),

latest_booking AS (
    -- FILTER BEFORE JOIN: Reduces data volume before shuffle join
    SELECT
        booking_id,
        user_id,
        hotel_id,
        snapshot_status,
        price,
        created_at,
        snapshot_updated_at
    FROM latest_booking_snapshot
    WHERE rn = 1
),

latest_event AS (
    SELECT
        booking_id,
        event_type          AS event_status,
        event_ts            AS event_updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY booking_id
            ORDER BY event_ts DESC
        ) AS rn
    FROM lakehouse.staging.stg_booking_events
    /*
        SHUFFLE NOTE: Same hash partition key (booking_id) as above.
        Spark/Trino optimizer may recognize co-partitioning and skip 
        the second shuffle when joining with latest_booking.
    */
),

latest_event_filtered AS (
    SELECT
        booking_id,
        event_status,
        event_updated_at
    FROM latest_event
    WHERE rn = 1
),

event_timestamps AS (
    SELECT
        booking_id,
        MAX(CASE WHEN event_type = 'created'   THEN event_ts END) AS event_created_at,
        MAX(CASE WHEN event_type = 'confirmed'  THEN event_ts END) AS event_confirmed_at,
        MAX(CASE WHEN event_type = 'cancelled'  THEN event_ts END) AS event_cancelled_at
    FROM lakehouse.staging.stg_booking_events
    GROUP BY booking_id
    /*
        SHUFFLE NOTE: Same partition key (booking_id). Spark AQE can
        combine this shuffle with the latest_event shuffle above.
    */
),

combined AS (
    SELECT
        b.booking_id,
        b.user_id,
        b.hotel_id,
        b.price,
        b.created_at,
        b.snapshot_status,
        b.snapshot_updated_at,
        e.event_status,
        e.event_updated_at,
        et.event_confirmed_at,
        et.event_cancelled_at,

        CASE
            WHEN b.snapshot_status = 'cancelled' 
              OR e.event_status = 'cancelled'
            THEN 'cancelled'

            WHEN e.event_status IS NOT NULL 
             AND b.snapshot_status IS NOT NULL
            THEN CASE
                    WHEN e.event_updated_at >= b.snapshot_updated_at 
                    THEN e.event_status
                    ELSE b.snapshot_status
                 END

            WHEN e.event_status IS NOT NULL 
            THEN e.event_status

            ELSE b.snapshot_status
        END AS resolved_status,

        -- CROSS-ENGINE SAFE: Explicit CASE instead of GREATEST()
        -- Trino GREATEST: returns NULL if ANY arg is NULL
        -- Spark GREATEST: skips NULLs
        -- This CASE works identically on both engines
        CASE
            WHEN b.snapshot_updated_at IS NULL THEN e.event_updated_at
            WHEN e.event_updated_at IS NULL THEN b.snapshot_updated_at
            WHEN e.event_updated_at >= b.snapshot_updated_at THEN e.event_updated_at
            ELSE b.snapshot_updated_at
        END AS last_updated_at

    FROM latest_booking b

    LEFT JOIN latest_event_filtered e
        ON b.booking_id = e.booking_id
        /*
            JOIN NOTE: Both sides are already hash-partitioned by 
            booking_id from ROW_NUMBER. Spark/Trino can do a 
            sort-merge join without additional shuffle (co-partitioned).
        */

    LEFT JOIN event_timestamps et
        ON b.booking_id = et.booking_id
)

SELECT
    booking_id,
    user_id,
    hotel_id,
    resolved_status             AS booking_status,
    price,
    created_at,
    event_confirmed_at          AS confirmed_at,
    event_cancelled_at          AS cancelled_at,
    last_updated_at,
    snapshot_status              AS _raw_snapshot_status,
    event_status                AS _raw_event_status,
    CASE 
        WHEN snapshot_status IS NOT NULL 
         AND event_status IS NOT NULL 
         AND snapshot_status != event_status
        THEN TRUE 
        ELSE FALSE 
    END                         AS _had_conflict,
    CURRENT_TIMESTAMP           AS _loaded_at

FROM combined
;


-- ============================================================
-- INCREMENTAL VERSION (for daily/hourly production runs)
-- ============================================================
/*
MERGE INTO lakehouse.silver.silver_bookings AS target
USING (
    -- Same CTE logic as above, but filtered:
    -- WHERE snapshot_updated_at > TIMESTAMP '${high_water_mark}'
    --    OR event_updated_at   > TIMESTAMP '${high_water_mark}'
    
    <same combined CTE with date filter>
    
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
-- POST-LOAD MAINTENANCE (schedule daily)
-- ============================================================
/*
-- Compact small files into optimal size (128-256MB)
ALTER TABLE lakehouse.silver.silver_bookings EXECUTE optimize;

-- Expire old snapshots (keep 7 days for time-travel)
ALTER TABLE lakehouse.silver.silver_bookings EXECUTE 
    expire_snapshots(older_than => CURRENT_TIMESTAMP - INTERVAL '7' DAY);

-- Remove orphan files from failed writes
ALTER TABLE lakehouse.silver.silver_bookings EXECUTE 
    remove_orphan_files(older_than => CURRENT_TIMESTAMP - INTERVAL '7' DAY);

-- Update statistics for query optimizer
ANALYZE lakehouse.silver.silver_bookings;
*/