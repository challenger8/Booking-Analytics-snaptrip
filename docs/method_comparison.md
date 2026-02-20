# Method Comparison: Generic SQL vs Distributed-Aware SQL

## File Locations

| Layer | Generic (runs on DuckDB) | Distributed (runs on Trino/Spark) |
|-------|--------------------------|-----------------------------------|
| Silver | `models/silver/silver_bookings.sql` | `models/silver/distributed/silver_bookings_distributed.sql` |
| Gold | `models/gold/gold_daily_city_kpis.sql` | `models/gold/distributed/gold_daily_city_kpis_distributed.sql` |

## Side-by-Side Differences

### 1. Table Format & Write Strategy

```sql
-- GENERIC:
-- dbt handles materialization
-- {{ config(materialized='table') }}

-- DISTRIBUTED:
-- Explicit Iceberg DDL with partitioning + sort order
CREATE TABLE ... WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(created_at)'],
    sorted_by = ARRAY['booking_id']
);
-- Atomic INSERT OVERWRITE (readers see old data until write completes)
INSERT OVERWRITE lakehouse.silver.silver_bookings ...

###2. Join Strategy
SQL

-- GENERIC:
LEFT JOIN stg_hotels h ON s.hotel_id = h.hotel_id
-- Engine decides join strategy. May shuffle both tables.

-- DISTRIBUTED:
SELECT /*+ BROADCAST(h) */ ...
LEFT JOIN stg_hotels h ON s.hotel_id = h.hotel_id
-- Hotels (<1MB) broadcast to all executors.
-- Silver table (millions of rows) stays in place. No shuffle.

### 3.  NULL Handling

-- GENERIC (may break on some engines):
GREATEST(
    COALESCE(b.snapshot_updated_at, e.event_updated_at),
    COALESCE(e.event_updated_at, b.snapshot_updated_at)
)

-- DISTRIBUTED (cross-engine safe):
CASE
    WHEN b.snapshot_updated_at IS NULL THEN e.event_updated_at
    WHEN e.event_updated_at IS NULL THEN b.snapshot_updated_at
    WHEN e.event_updated_at >= b.snapshot_updated_at THEN e.event_updated_at
    ELSE b.snapshot_updated_at
END
-- Works identically on Trino, Spark, DuckDB, Postgres

### 4. Incremental Updates
-- GENERIC:
-- Full refresh only. Reprocess everything every run.

-- DISTRIBUTED:
MERGE INTO silver_bookings AS target
USING (... filtered by high_water_mark ...) AS source
ON target.booking_id = source.booking_id
WHEN MATCHED AND source.last_updated_at > target.last_updated_at THEN UPDATE ...
WHEN NOT MATCHED THEN INSERT ...
-- Only processes changed bookings. 95%+ cost reduction on daily runs.

### 5. Incremental Updates

-- GENERIC:
-- Not applicable. Engine manages storage.

-- DISTRIBUTED:
-- Compact small files (prevents "small file problem")
ALTER TABLE silver_bookings EXECUTE optimize;

-- Expire old snapshots (free storage, keep 7 days for time-travel)
ALTER TABLE silver_bookings EXECUTE 
    expire_snapshots(older_than => CURRENT_TIMESTAMP - INTERVAL '7' DAY);

-- Update statistics for query optimizer
ANALYZE silver_bookings;