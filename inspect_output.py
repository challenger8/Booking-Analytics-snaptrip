#!/usr/bin/env python3
"""
Quick inspection of pipeline output.
Run after: dbt seed + dbt run

Usage: python3 inspect_output.py
"""

import duckdb

con = duckdb.connect("booking_analytics.duckdb", read_only=True)

print("=" * 70)
print("  PIPELINE OUTPUT INSPECTION")
print("=" * 70)

# ─────────────────────────────────────────
# SILVER: Check booking states
# ─────────────────────────────────────────
print("\n SILVER — Booking States:")
print("-" * 70)

result = con.execute("""
    SELECT 
        booking_id,
        booking_status,
        price,
        CAST(created_at AS DATE) AS created_date,
        _raw_snapshot_status,
        _raw_event_status,
        _had_conflict
    FROM silver_bookings
    ORDER BY booking_id
""").fetchdf()
print(result.to_string(index=False))

print(f"\nTotal bookings: {len(result)}")

# Status distribution
print("\n SILVER — Status Distribution:")
print("-" * 70)
result = con.execute("""
    SELECT 
        booking_status,
        COUNT(*) AS count,
        ROUND(SUM(price), 2) AS total_price
    FROM silver_bookings
    GROUP BY booking_status
    ORDER BY booking_status
""").fetchdf()
print(result.to_string(index=False))

# Conflicts
print("\n SILVER — Conflict Detection:")
print("-" * 70)
result = con.execute("""
    SELECT 
        booking_id,
        _raw_snapshot_status,
        _raw_event_status,
        booking_status AS resolved_to
    FROM silver_bookings
    WHERE _had_conflict = TRUE
""").fetchdf()
if len(result) == 0:
    print("No conflicts detected (both sources always agreed)")
else:
    print(result.to_string(index=False))

# ─────────────────────────────────────────
# GOLD: Check daily KPIs
# ─────────────────────────────────────────
print("\n GOLD — Daily City KPIs:")
print("-" * 70)
result = con.execute("""
    SELECT 
        booking_date,
        city,
        total_bookings,
        confirmed_bookings,
        cancelled_bookings,
        pending_bookings,
        ROUND(cancellation_rate, 4) AS cancel_rate,
        total_confirmed_revenue AS revenue,
        avg_booking_price AS avg_price
    FROM gold_daily_city_kpis
    ORDER BY booking_date, city
""").fetchdf()
print(result.to_string(index=False))

# Totals
print("\n GOLD — Overall Totals:")
print("-" * 70)
result = con.execute("""
    SELECT 
        SUM(total_bookings) AS total_bookings,
        SUM(confirmed_bookings) AS confirmed,
        SUM(cancelled_bookings) AS cancelled,
        SUM(pending_bookings) AS pending,
        ROUND(SUM(total_confirmed_revenue), 2) AS total_revenue
    FROM gold_daily_city_kpis
""").fetchdf()
print(result.to_string(index=False))

# ─────────────────────────────────────────
# EDGE CASE VERIFICATION (auto-detect booking IDs)
# ─────────────────────────────────────────
print("\n EDGE CASE VERIFICATION:")
print("-" * 70)

# Get all bookings and their statuses
all_bookings = con.execute("""
    SELECT booking_id, booking_status, price
    FROM silver_bookings
    ORDER BY booking_id
""").fetchall()

all_passed = True

for booking_id, status, price in all_bookings:
    # Basic checks per booking
    checks_passed = True
    
    # Check 1: Status is valid
    if status not in ('created', 'confirmed', 'cancelled'):
        print(f"  [X] FAIL  {booking_id}: invalid status '{status}'")
        checks_passed = False
        all_passed = False
    
    # Check 2: Price is positive
    if price is None or price <= 0:
        print(f"  [X] FAIL  {booking_id}: invalid price {price}")
        checks_passed = False
        all_passed = False
    
    if checks_passed:
        print(f"  [+] PASS  {booking_id}: status={status}, price={price}")

# ─────────────────────────────────────────
# STATUS TRANSITION VERIFICATION
# ─────────────────────────────────────────
print("\n STATUS TRANSITION VERIFICATION:")
print("-" * 70)

# Check that cancelled bookings actually had a cancellation event or snapshot
cancelled_bookings = con.execute("""
    SELECT 
        s.booking_id,
        s.booking_status,
        s._raw_snapshot_status,
        s._raw_event_status
    FROM silver_bookings s
    WHERE s.booking_status = 'cancelled'
""").fetchall()

for booking_id, status, snap_status, event_status in cancelled_bookings:
    if snap_status == 'cancelled' or event_status == 'cancelled':
        print(f"  [+] PASS  {booking_id}: cancelled confirmed by source data")
    else:
        print(f"  [X] FAIL  {booking_id}: cancelled but no source confirms it")
        all_passed = False

# Check that confirmed bookings are not also cancelled
confirmed_bookings = con.execute("""
    SELECT 
        s.booking_id,
        s._raw_snapshot_status,
        s._raw_event_status
    FROM silver_bookings s
    WHERE s.booking_status = 'confirmed'
""").fetchall()

for booking_id, snap_status, event_status in confirmed_bookings:
    if snap_status == 'cancelled' or event_status == 'cancelled':
        print(f"  [X] FAIL  {booking_id}: confirmed but a source shows cancelled")
        all_passed = False
    else:
        print(f"  [+] PASS  {booking_id}: confirmed with no cancellation conflict")

# ─────────────────────────────────────────
# CROSS-LAYER CONSISTENCY
# ─────────────────────────────────────────
print("\n CROSS-LAYER CONSISTENCY:")
print("-" * 70)

silver_count = con.execute("SELECT COUNT(*) FROM silver_bookings").fetchone()[0]
gold_sum = con.execute("SELECT SUM(total_bookings) FROM gold_daily_city_kpis").fetchone()[0]

if silver_count == gold_sum:
    print(f"  [+] PASS  Silver count ({silver_count}) = Gold sum ({int(gold_sum)})")
else:
    print(f"  [X] FAIL  Silver count ({silver_count}) != Gold sum ({gold_sum})")
    all_passed = False

silver_revenue = con.execute("""
    SELECT ROUND(SUM(price), 2) 
    FROM silver_bookings 
    WHERE booking_status = 'confirmed'
""").fetchone()[0]

gold_revenue = con.execute("""
    SELECT ROUND(SUM(total_confirmed_revenue), 2) 
    FROM gold_daily_city_kpis
""").fetchone()[0]

if silver_revenue == gold_revenue:
    print(f"  [+] PASS  Silver revenue ({silver_revenue}) = Gold revenue ({gold_revenue})")
else:
    print(f"  [X] FAIL  Silver revenue ({silver_revenue}) != Gold revenue ({gold_revenue})")
    all_passed = False

# Check counts add up in Gold
count_check = con.execute("""
    SELECT COUNT(*)
    FROM gold_daily_city_kpis
    WHERE total_bookings != confirmed_bookings + cancelled_bookings + pending_bookings
""").fetchone()[0]

if count_check == 0:
    print(f"  [+] PASS  Gold: confirmed + cancelled + pending = total (all rows)")
else:
    print(f"  [X] FAIL  Gold: {count_check} rows where counts don't add up")
    all_passed = False

# ─────────────────────────────────────────
# FINAL RESULT
# ─────────────────────────────────────────
print(f"\n{'=' * 70}")
if all_passed:
    print("  ALL CHECKS PASSED — Pipeline is correct!")
else:
    print("  SOME CHECKS FAILED — Review Silver logic!")
print(f"{'=' * 70}")

con.close()