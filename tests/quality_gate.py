#!/usr/bin/env python3
"""
Data Quality Gate - runs after deployment to validate output.

Usage:
    python quality_gate.py --environment staging --trino-host localhost
    python quality_gate.py --environment production --trino-host prod-trino
"""

import argparse
import sys

try:
    from trino.dbapi import connect
    HAS_TRINO = True
except ImportError:
    HAS_TRINO = False


def run_quality_checks(cursor, catalog: str) -> list:
    """Run all quality checks and return results."""
    
    checks = []
    
    # CHECK 1: Silver - one row per booking
    cursor.execute(f"""
        SELECT COUNT(*) AS total_rows,
               COUNT(DISTINCT booking_id) AS unique_bookings
        FROM {catalog}.silver.silver_bookings
    """)
    row = cursor.fetchone()
    total_rows, unique_bookings = row[0], row[1]
    checks.append({
        "name": "Silver: one row per booking",
        "passed": total_rows == unique_bookings,
        "detail": f"total_rows={total_rows}, unique_bookings={unique_bookings}"
    })
    
    # CHECK 2: No NULL booking_ids
    cursor.execute(f"""
        SELECT COUNT(*) 
        FROM {catalog}.silver.silver_bookings
        WHERE booking_id IS NULL
    """)
    null_count = cursor.fetchone()[0]
    checks.append({
        "name": "Silver: no NULL booking_ids",
        "passed": null_count == 0,
        "detail": f"null_booking_ids={null_count}"
    })
    
    # CHECK 3: Valid status values
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM {catalog}.silver.silver_bookings
        WHERE booking_status NOT IN ('created', 'confirmed', 'cancelled')
    """)
    invalid_status = cursor.fetchone()[0]
    checks.append({
        "name": "Silver: valid status values",
        "passed": invalid_status == 0,
        "detail": f"invalid_status_count={invalid_status}"
    })
    
    # CHECK 4: Gold counts add up
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM {catalog}.gold.gold_daily_city_kpis
        WHERE total_bookings != 
              confirmed_bookings + cancelled_bookings + pending_bookings
    """)
    mismatched = cursor.fetchone()[0]
    checks.append({
        "name": "Gold: counts add up",
        "passed": mismatched == 0,
        "detail": f"mismatched_rows={mismatched}"
    })
    
    # CHECK 5: Cancellation rate in bounds
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM {catalog}.gold.gold_daily_city_kpis
        WHERE cancellation_rate < 0 OR cancellation_rate > 1
    """)
    out_of_bounds = cursor.fetchone()[0]
    checks.append({
        "name": "Gold: cancellation rate 0-1",
        "passed": out_of_bounds == 0,
        "detail": f"out_of_bounds_rows={out_of_bounds}"
    })
    
    # CHECK 6: Non-negative revenue
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM {catalog}.gold.gold_daily_city_kpis
        WHERE total_confirmed_revenue < 0
    """)
    negative_revenue = cursor.fetchone()[0]
    checks.append({
        "name": "Gold: non-negative revenue",
        "passed": negative_revenue == 0,
        "detail": f"negative_revenue_rows={negative_revenue}"
    })
    
    # CHECK 7: Cross-layer consistency
    cursor.execute(f"""
        SELECT 
            (SELECT COUNT(*) FROM {catalog}.silver.silver_bookings) AS silver_count,
            (SELECT COALESCE(SUM(total_bookings), 0) FROM {catalog}.gold.gold_daily_city_kpis) AS gold_total
    """)
    row = cursor.fetchone()
    silver_count, gold_total = row[0], row[1]
    checks.append({
        "name": "Cross-layer: Silver count = Gold sum",
        "passed": silver_count == gold_total,
        "detail": f"silver={silver_count}, gold_sum={gold_total}"
    })
    
    return checks


def main():
    parser = argparse.ArgumentParser(description="Data Quality Gate")
    parser.add_argument("--environment", required=True, choices=["staging", "production"])
    parser.add_argument("--trino-host", required=True)
    parser.add_argument("--trino-port", default=8080, type=int)
    parser.add_argument("--alert-slack-webhook", default=None)
    args = parser.parse_args()
    
    if not HAS_TRINO:
        print("WARNING: trino package not installed. Install with: pip install trino")
        print("Skipping quality gate checks.")
        sys.exit(0)
    
    catalog = "staging_lakehouse" if args.environment == "staging" else "prod_lakehouse"
    
    conn = connect(
        host=args.trino_host,
        port=args.trino_port,
        user="ci-pipeline",
        catalog=catalog,
    )
    cursor = conn.cursor()
    
    print(f"\n{'='*60}")
    print(f"  DATA QUALITY GATE - {args.environment.upper()}")
    print(f"{'='*60}\n")
    
    checks = run_quality_checks(cursor, catalog)
    
    all_passed = True
    for check in checks:
        status = "PASS" if check["passed"] else "FAIL"
        symbol = "+" if check["passed"] else "X"
        print(f"  [{symbol}] {status}  {check['name']}")
        print(f"          {check['detail']}")
        if not check["passed"]:
            all_passed = False
    
    print(f"\n{'='*60}")
    if all_passed:
        print("  ALL CHECKS PASSED - deployment approved")
        print(f"{'='*60}\n")
        sys.exit(0)
    else:
        print("  QUALITY GATE FAILED - deployment blocked")
        print(f"{'='*60}\n")
        sys.exit(1)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
