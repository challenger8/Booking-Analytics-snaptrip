# Data Flow Diagram

## High-Level Pipeline
Source Systems (Operational DBs)
│
│ CDC (Debezium)
▼
┌───────────────────┐
│ Message Queue │
│ (Apache Kafka) │
└────────┬──────────┘
│
│ Consumers write to object storage
▼
┌──────────────────────────────────────────────────────────┐
│ BRONZE (Raw Data) │
│ │
│ ┌─────────────────┐ ┌──────────────────┐ ┌────────┐ │
│ │ bookings_raw │ │booking_events_raw│ │hotels_ │ │
│ │ │ │ │ │raw │ │
│ │ CDC snapshots │ │ Append-only │ │Reference│ │
│ │ Multiple rows │ │ event log │ │ data │ │
│ │ per booking │ │ Can arrive late │ │ │ │
│ │ │ │ │ │ │ │
│ │ booking_id │ │ booking_id │ │hotel_id│ │
│ │ user_id │ │ event_type │ │city │ │
│ │ hotel_id │ │ event_ts │ │star_ │ │
│ │ status │ │ │ │rating │ │
│ │ price │ │ │ │ │ │
│ │ created_at │ │ │ │ │ │
│ │ updated_at │ │ │ │ │ │
│ └────────┬────────┘ └────────┬─────────┘ └───┬────┘ │
│ │ │ │ │
└───────────┼────────────────────┼─────────────────┼───────┘
│ │ │
▼ ▼ ▼
┌──────────────────────────────────────────────────────────┐
│ STAGING (Light Cleaning) │
│ │
│ What happens here: │
│ • CAST types explicitly (VARCHAR, TIMESTAMP, DECIMAL) │
│ • Remove exact duplicate rows │
│ • NO business logic applied │
│ │
│ ┌─────────────────┐ ┌──────────────────┐ ┌────────┐ │
│ │ stg_bookings │ │stg_booking_events│ │stg_ │ │
│ │ │ │ │ │hotels │ │
│ │ Dedup by │ │ Dedup by │ │Dedup by│ │
│ │ booking_id + │ │ booking_id + │ │hotel_id│ │
│ │ updated_at │ │ event_type + │ │ │ │
│ │ │ │ event_ts │ │ │ │
│ └────────┬────────┘ └────────┬─────────┘ └───┬────┘ │
│ │ │ │ │
└───────────┼────────────────────┼─────────────────┼───────┘
│ │ │
▼ ▼ │
┌──────────────────────────────────────────────────┼───────┐
│ SILVER (Curated State) │ │
│ │ │
│ ┌───────────────────────────────────────────┐ │ │
│ │ silver_bookings │ │ │
│ │ │ │ │
│ │ GRAIN: One row per booking_id │ │ │
│ │ │ │ │
│ │ INPUTS: │ │ │
│ │ ┌─────────────┐ ┌──────────────────┐ │ │ │
│ │ │stg_bookings │ │stg_booking_events│ │ │ │
│ │ │ │ │ │ │ │ │
│ │ │ Latest CDC │ │ Latest event │ │ │ │
│ │ │ snapshot │ │ per booking │ │ │ │
│ │ │ (ROW_NUMBER │ │ (ROW_NUMBER │ │ │ │
│ │ │ by updated │ │ by event_ts │ │ │ │
│ │ │ _at DESC) │ │ DESC) │ │ │ │
│ │ └──────┬──────┘ └────────┬─────────┘ │ │ │
│ │ │ │ │ │ │
│ │ ▼ ▼ │ │ │
│ │ ┌─────────────────────────────────────┐ │ │ │
│ │ │ CONFLICT RESOLUTION │ │ │ │
│ │ │ │ │ │ │
│ │ │ 1. If EITHER says cancelled │ │ │ │
│ │ │ → cancelled (terminal state) │ │ │ │
│ │ │ │ │ │ │
│ │ │ 2. If both exist, neither cancel │ │ │ │
│ │ │ → latest timestamp wins │ │ │ │
│ │ │ │ │ │ │
│ │ │ 3. If only one source exists │ │ │ │
│ │ │ → use that source │ │ │ │
│ │ └─────────────────────────────────────┘ │ │ │
│ │ │ │ │
│ │ OUTPUT COLUMNS: │ │ │
│ │ • booking_id (from bookings) │ │ │
│ │ • user_id (from bookings) │ │ │
│ │ • hotel_id (from bookings) │ │ │
│ │ • booking_status (resolved) │ │ │
│ │ • price (from bookings) │ │ │
│ │ • created_at (from bookings) │ │ │
│ │ • confirmed_at (from events) │ │ │
│ │ • cancelled_at (from events) │ │ │
│ │ • last_updated_at (MAX both sources) │ │ │
│ │ • _raw_snapshot_status (audit) │ │ │
│ │ • _raw_event_status (audit) │ │ │
│ │ • _had_conflict (audit) │ │ │
│ │ │ │ │
│ └──────────────────────┬─────────────────────┘ │ │
│ │ │ │
└─────────────────────────┼──────────────────────────┼───────┘
│ │
▼ ▼
┌──────────────────────────────────────────────────────────┐
│ GOLD (Analytics Mart) │
│ │
│ ┌────────────────────────────────────────────────────┐ │
│ │ gold_daily_city_kpis │ │
│ │ │ │
│ │ GRAIN: One row per booking_date × city │ │
│ │ │ │
│ │ INPUTS: │ │
│ │ ┌────────────────┐ ┌─────────────┐ │ │
│ │ │silver_bookings │ │ stg_hotels │ │ │
│ │ │ │ │ (BROADCAST) │ │ │
│ │ │ booking_id │ │ hotel_id │ │ │
│ │ │ booking_status │ │ city ───────┼──┐ │ │
│ │ │ price │ │ │ │ │ │
│ │ │ created_at ────┼─── booking_date │ │ │ │
│ │ │ hotel_id ──────┼─── JOIN ──────────┘ │ │ │
│ │ └────────────────┘ └─────────────┘ │ │ │
│ │ │ │ │
│ │ DATE CHOICE: CAST(created_at AS DATE) │ │ │
│ │ (immutable, stable for dashboards) │ │ │
│ │ │ │ │
│ │ GROUP BY booking_date, city │ │ │
│ │ ▼ │ │
│ │ OUTPUT METRICS: │ │
│ │ ┌───────────────────────┬─────────────────────┐ │ │
│ │ │ Metric │ Logic │ │ │
│ │ ├───────────────────────┼─────────────────────┤ │ │
│ │ │ total_bookings │ COUNT(*) │ │ │
│ │ │ confirmed_bookings │ COUNT(confirmed) │ │ │
│ │ │ cancelled_bookings │ COUNT(cancelled) │ │ │
│ │ │ pending_bookings │ COUNT(created) │ │ │
│ │ │ cancellation_rate │ cancelled / total │ │ │
│ │ │ total_confirmed_rev │ SUM(price) confirmed│ │ │
│ │ │ avg_booking_price │ AVG(price) all │ │ │
│ │ │ avg_confirmed_price │ AVG(price) confirmed│ │ │
│ │ └───────────────────────┴─────────────────────┘ │ │
│ │ │ │
│ └──────────────────────────────────────────────────────┘ │
│ │
└────────────────────────────────────────────────────────────┘
│
▼
┌──────────────────────────────────────────────────────────┐
│ CONSUMERS │
│ │
│ • BI Dashboards (daily booking trends) │
│ • Revenue reporting (confirmed revenue by city) │
│ • Operations alerts (cancellation rate spikes) │
│ • Executive KPIs (booking pace, market performance) │
│ │
└──────────────────────────────────────────────────────────┘




## Data Quality Tests Flow
┌──────────────────────────────────────────────────────────┐
│ DATA QUALITY LAYER │
│ │
│ SILVER TESTS: │
│ ┌────────────────────────────────────────────────┐ │
│ │ assert_one_row_per_booking → unique booking│ │
│ │ assert_valid_status → enum check │ │
│ │ assert_no_future_dates → date sanity │ │
│ └────────────────────────────────────────────────┘ │
│ │
│ GOLD TESTS: │
│ ┌────────────────────────────────────────────────┐ │
│ │ assert_counts_add_up → math check │ │
│ │ assert_cancellation_rate_bounds→ 0 to 1 │ │
│ │ assert_revenue_non_negative → no negatives │ │
│ └────────────────────────────────────────────────┘ │
│ │
│ CROSS-LAYER TESTS: │
│ ┌────────────────────────────────────────────────┐ │
│ │ assert_silver_gold_consistency → counts match │ │
│ └────────────────────────────────────────────────┘ │
│ │
│ Total: 7 tests, all passing │
│ │
└──────────────────────────────────────────────────────────┘




## Example Data Flow (Booking b2)
BRONZE:
bookings_raw:
b2, u2, h2, created, 200.0, 2025-01-02 09:00, 2025-01-02 09:00
b2, u2, h2, confirmed, 200.0, 2025-01-02 09:00, 2025-01-02 10:00
b2, u2, h2, cancelled, 200.0, 2025-01-02 09:00, 2025-01-02 14:00

booking_events_raw:
b2, created, 2025-01-02 09:00
b2, confirmed, 2025-01-02 10:00
b2, cancelled, 2025-01-02 14:00



     │
     ▼
STAGING:
stg_bookings: 3 rows (deduped, typed)
stg_events: 3 rows (deduped, typed)



     │
     ▼
SILVER:
latest_booking_snapshot:
b2, cancelled, updated_at=14:00 (ROW_NUMBER picks latest)

latest_event:
b2, cancelled, event_ts=14:00 (ROW_NUMBER picks latest)

conflict_resolution:
snapshot=cancelled, event=cancelled
→ Rule 1: EITHER is cancelled → cancelled ✅

silver_bookings:
b2, cancelled, 200.0, created_at=2025-01-02



     │
     ▼
GOLD:
booking_date = 2025-01-02 (from created_at)
city = Shiraz (from hotels via hotel_id=h2)

gold_daily_city_kpis:
2025-01-02, Shiraz:
total_bookings = 1
cancelled_bookings = 1
cancellation_rate = 1.0
total_confirmed_revenue = 0
