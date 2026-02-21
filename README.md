# Booking Analytics — Bronze → Silver → Gold Pipeline

## Overview

A data engineering solution for a travel-tech company's lakehouse platform. 
Implements a Bronze → Silver → Gold data flow to transform raw CDC booking 
data into reliable daily analytics.

**Tech Stack**: SQL (Trino/Spark compatible), dbt, Apache Iceberg, DuckDB (for testing)

## Architecture
Source Systems (Operational DBs)
│
▼
CDC Pipeline (Debezium → Kafka)
│
▼
┌──────────────────────────────────┐
│ BRONZE (Raw) │
│ bookings_raw (CDC snapshots) │
│ booking_events_raw (event log) │
│ hotels_raw (reference) │
└───────┬──────────────────────────┘
│
▼
┌──────────────────────────────────┐
│ STAGING (Light Cleaning) │
│ stg_bookings │
│ stg_booking_events │
│ stg_hotels │
│ • Type casting │
│ • Exact duplicate removal │
│ • No business logic │
└───────┬──────────────────────────┘
│
▼
┌──────────────────────────────────┐
│ SILVER (Curated) │
│ silver_bookings │
│ • 1 row per booking_id │
│ • Conflict resolution applied │
│ • Terminal state logic │
│ • Audit columns │
└───────┬──────────────────────────┘
│
▼
┌──────────────────────────────────┐
│ GOLD (Analytics) │
│ gold_daily_city_kpis │
│ • 1 row per day × city │
│ • Revenue, counts, rates │
│ • Broadcast join with hotels │
└──────────────────────────────────┘




## Quick Start

```bash
# Install dependencies
pip install dbt-core dbt-duckdb

# Run the full pipeline
dbt seed --profiles-dir ./ci --target ci     # Load test data
dbt run --profiles-dir ./ci --target ci      # Build all models
dbt test --profiles-dir ./ci --target ci     # Run data quality tests

# Inspect output
python3 inspect_output.py

# Compare generic vs distributed methods
python3 compare_methods.py
```
## Project Structure


booking-analytics/
├── models/                          ← dbt models (Generic SQL, runs on DuckDB)
│   ├── staging/
│   │   ├── stg_bookings.sql         ← Light cleaning of CDC snapshots
│   │   ├── stg_booking_events.sql   ← Light cleaning of event log
│   │   └── stg_hotels.sql           ← Light cleaning of reference data
│   ├── silver/
│   │   └── silver_bookings.sql      ← Part 1: Latest state per booking
│   └── gold/
│       └── gold_daily_city_kpis.sql ← Part 2: Daily KPIs by city
│
├── sql/                             ← Production SQL (Trino/Spark style)
│   ├── setup/
│   │   └── create_tables.sql        ← Iceberg DDLs + MERGE INTO + maintenance
│   └── distributed/
│       ├── silver_bookings_distributed.sql    ← Trino/Spark optimized Silver
│       └── gold_daily_city_kpis_distributed.sql ← Trino/Spark optimized Gold
│
├── data/                            ← Seed data with edge cases
│   ├── bookings_raw.csv
│   ├── booking_events_raw.csv
│   └── hotels_raw.csv
│
├── tests/                           ← Data quality tests (7 tests)
│   ├── assert_one_row_per_booking.sql
│   ├── assert_valid_status.sql
│   ├── assert_counts_add_up.sql
│   ├── assert_revenue_non_negative.sql
│   ├── assert_cancellation_rate_bounds.sql
│   ├── assert_silver_gold_consistency.sql
│   └── assert_no_future_dates.sql
│
├── docs/                            ← Documentation
│   ├── data_flow_diagram.md
│   ├── edge_cases.md
│   └── method_comparison.md         ← Generic vs Distributed comparison
│
├── .github/workflows/               ← CI/CD pipelines
│   ├── ci.yml
│   └── cd.yml
│
├── schema.yml                       ← Column descriptions + schema tests
├── dbt_project.yml                  ← dbt configuration
├── ci/profiles.yml                  ← dbt profiles (DuckDB for CI, Trino for prod)
├── inspect_output.py                ← Verify pipeline output
├── compare_methods.py               ← Compare generic vs distributed results
├── run_distributed.py               ← Run distributed version + timing
└── README.md


Two SQL Approaches
This project provides both a generic and distributed-aware version:

Approach	Location	Engine	Purpose
Generic SQL	models/	DuckDB (dbt)	Development, CI testing, proves correctness
Distributed SQL	sql/distributed/	Trino/Spark	Production deployment with optimizations
Both produce identical results — verified by compare_methods.py.

Key differences in the distributed version:

BROADCAST join hints for small dimension tables (hotels)
Explicit CASE instead of GREATEST() for cross-engine NULL safety
INSERT OVERWRITE for atomic writes
MERGE INTO for incremental upserts
Iceberg partitioning (day(created_at)) and sort order
Shuffle awareness comments on every join and window function
See docs/method_comparison.md for detailed comparison.

## Assumptions


cancelled is a terminal state. Once a booking is cancelled (by either source), it cannot be reverted. This is a standard business rule in travel — cancellations require a new booking, not status reversion.
created_at is immutable. The creation timestamp never changes across CDC snapshots — it reflects when the booking was originally placed.
price does not change after creation. If it did, we would need to track price history. The current design takes the price from the latest snapshot.
Events are the authority for status; snapshots are the authority for dimensional attributes. Events capture precise state transitions. Snapshots carry the full record context (price, user, hotel).
The Gold layer uses created_at date for reporting. This means we measure "demand on the day the booking was created" — not when it was confirmed or cancelled. This is standard in travel analytics for measuring booking pace.
Hotels reference data is static (no SCD tracking needed for this exercise).

## Design Decisions

1. Conflict Resolution Strategy
Two data sources may disagree on booking status:

bookings_raw (CDC snapshots): has dimensional attributes (price, user, hotel)
booking_events_raw (event log): has precise state transition timestamps
Resolution rules:

Priority 1: TERMINAL STATE GUARD
   If EITHER source shows 'cancelled' → booking is cancelled
   (cancelled is irreversible)

Priority 2: TIMESTAMP COMPARISON
   If both sources exist and neither is cancelled →
   the source with the later timestamp wins

Priority 3: FALLBACK
   If only one source has data → use that source
Why these rules?

Events are append-only and great for status tracking, but they don't carry dimensional attributes like price, user_id, or hotel_id
In real CDC pipelines, event streams can have gaps — a booking might exist in the CDC table but its events might be missing
The terminal state guard prevents a late-arriving "confirmed" event from resurrecting a cancelled booking
2. Date Dimension Choice
Gold layer uses created_at (booking creation date) as the reporting date.

Why: created_at is immutable. Using updated_at or event_ts would shift bookings between days when they get confirmed/cancelled, breaking dashboard stability.

Trade-off acknowledged: A booking created on Jan 10 and cancelled on Jan 11 will count as both a total booking AND a cancellation on Jan 10. This is the correct semantic — Jan 10's demand included a booking that ultimately cancelled.

3. Audit Columns
Silver layer includes _had_conflict and _raw_*_status columns. These let us monitor the health of the reconciliation. If _had_conflict suddenly spikes, it signals a pipeline issue worth investigating.

Metric Definitions
Metric	Definition
total_bookings	COUNT of all bookings created on this day in this city
confirmed_bookings	COUNT where final status = 'confirmed'
cancelled_bookings	COUNT where final status = 'cancelled'
pending_bookings	COUNT where final status = 'created' (not yet confirmed/cancelled)
cancellation_rate	cancelled / total (0 if total = 0)
total_confirmed_revenue	SUM(price) for confirmed bookings only
avg_booking_price	AVG(price) across ALL bookings (demand signal)
avg_confirmed_price	AVG(price) for confirmed bookings only
Double counting prevention: Silver layer guarantees exactly one row per booking_id. Each booking maps to exactly one day (created_at date) and one city (via hotel join). No double counting is possible by construction.

Edge Cases Handled
Scenario	How It's Handled
Booking with multiple CDC snapshots	Latest updated_at wins for dimensional attributes
Late-arriving event with older timestamp	Timestamp comparison prevents overwrite
Booking cancelled then "confirmed" event arrives late	Terminal state guard keeps it cancelled
Duplicate events (same booking + type + timestamp)	Deduplicated in staging layer
Hotel not found in reference data	City defaults to 'Unknown'
Booking never confirmed or cancelled	Status stays 'created' (counted as pending)
GREATEST() NULL behavior across engines	Explicit CASE statement for cross-engine safety
See docs/edge_cases.md for full details.

## Known Limitations

1. **No SCD2 on Silver.** Only latest state stored. Cannot answer "what was the status of booking B001 at 2pm yesterday?" For this use case, current state is sufficient.

2. **No incremental processing in CI.** All models run full-refresh. In production, the distributed version uses `MERGE INTO` for incremental upserts.

3. **Price changes not tracked.** If a booking's price changes between CDC snapshots, we take the latest. A production system might want to capture price history.

4. **Timezone handling.** All timestamps assumed UTC. Production would need explicit timezone management.

5. **No data freshness SLA monitoring.** In production, we'd add checks like "if no new events in 2 hours, raise an alert."

## What-If Scenarios

### "What if the schema of bookings_raw changes?"
Iceberg schema evolution handles new columns as NULLs. dbt `schema.yml` contracts detect breaking changes early.

### "What if we need to reprocess 6 months of data?"
Run `dbt run --full-refresh`. Idempotent by design — `INSERT OVERWRITE` produces the same result regardless of how many times it runs.

### "What if a new status (e.g., 'checked_in') is added?"
1. Update staging `accepted_values` test
2. Update Silver conflict resolution rules (is it terminal?)
3. Add a new counter to Gold
4. Tests catch if unexpected values appear

### "What if event volume grows to 1B/day?"
- Switch to incremental `MERGE INTO` with high-water mark
- Iceberg partition pruning skips 90%+ of files
- Add bloom filter indexes on booking_id
- Consider streaming Silver updates with Kafka + Flink/Spark Structured Streaming

## Distributed Engine & Lakehouse Considerations

### Table Format: Apache Iceberg
- **Hidden partitioning**: `day(created_at)` transform — no physical partition column needed
- **Sort order**: Tables sorted by `booking_id` for efficient MERGE operations
- **Schema evolution**: `ALTER 
