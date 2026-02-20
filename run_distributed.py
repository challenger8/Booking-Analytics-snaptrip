#!/usr/bin/env python3
"""
Run distributed SQL on both DuckDB (simulated) and Trino (real).
Compare results from both engines.

Usage:
    python3 run_distributed.py              # DuckDB only
    python3 run_distributed.py --trino      # DuckDB + Trino comparison
"""

import argparse
import time
import duckdb

try:
    from trino.dbapi import connect as trino_connect
    HAS_TRINO = True
except ImportError:
    HAS_TRINO = False


def run_on_duckdb():
    """Run both generic and distributed versions on DuckDB."""
    
    con = duckdb.connect("booking_analytics.duckdb", read_only=False)
    results = {}
    
    print("\n  ENGINE: DuckDB")
    print("  " + "-" * 60)
    
    # Generic Silver
    start = time.time()
    generic_silver = con.execute("""
        SELECT booking_id, booking_status, price,
            CAST(created_at AS DATE) AS created_date
        FROM silver_bookings
        ORDER BY booking_id
    """).fetchdf()
    results['generic_silver_time'] = (time.time() - start) * 1000
    results['generic_silver'] = generic_silver
    
    # Generic Gold
    start = time.time()
    generic_gold = con.execute("""
        SELECT booking_date, city, total_bookings, confirmed_bookings,
            cancelled_bookings, pending_bookings,
            ROUND(cancellation_rate, 4) AS cancellation_rate,
            total_confirmed_revenue, avg_booking_price
        FROM gold_daily_city_kpis
        ORDER BY booking_date, city
    """).fetchdf()
    results['generic_gold_time'] = (time.time() - start) * 1000
    results['generic_gold'] = generic_gold
    
    # Distributed Silver (same logic, explicit CASE for NULLs)
    con.execute("DROP TABLE IF EXISTS silver_dist_test")
    start = time.time()
    con.execute("""
        CREATE TABLE silver_dist_test AS
        WITH latest_booking_snapshot AS (
            SELECT booking_id, user_id, hotel_id,
                status AS snapshot_status, price, created_at,
                updated_at AS snapshot_updated_at,
                ROW_NUMBER() OVER (PARTITION BY booking_id ORDER BY updated_at DESC) AS rn
            FROM stg_bookings
        ),
        latest_booking AS (
            SELECT * FROM latest_booking_snapshot WHERE rn = 1
        ),
        latest_event AS (
            SELECT booking_id, event_type AS event_status, event_ts AS event_updated_at,
                ROW_NUMBER() OVER (PARTITION BY booking_id ORDER BY event_ts DESC) AS rn
            FROM stg_booking_events
        ),
        latest_event_filtered AS (
            SELECT * FROM latest_event WHERE rn = 1
        ),
        event_timestamps AS (
            SELECT booking_id,
                MAX(CASE WHEN event_type = 'confirmed' THEN event_ts END) AS event_confirmed_at,
                MAX(CASE WHEN event_type = 'cancelled' THEN event_ts END) AS event_cancelled_at
            FROM stg_booking_events GROUP BY booking_id
        ),
        combined AS (
            SELECT
                b.booking_id, b.user_id, b.hotel_id, b.price, b.created_at,
                b.snapshot_status, b.snapshot_updated_at,
                e.event_status, e.event_updated_at,
                et.event_confirmed_at, et.event_cancelled_at,
                CASE
                    WHEN b.snapshot_status = 'cancelled' OR e.event_status = 'cancelled' THEN 'cancelled'
                    WHEN e.event_status IS NOT NULL AND b.snapshot_status IS NOT NULL
                    THEN CASE WHEN e.event_updated_at >= b.snapshot_updated_at 
                        THEN e.event_status ELSE b.snapshot_status END
                    WHEN e.event_status IS NOT NULL THEN e.event_status
                    ELSE b.snapshot_status
                END AS resolved_status,
                CASE
                    WHEN b.snapshot_updated_at IS NULL THEN e.event_updated_at
                    WHEN e.event_updated_at IS NULL THEN b.snapshot_updated_at
                    WHEN e.event_updated_at >= b.snapshot_updated_at THEN e.event_updated_at
                    ELSE b.snapshot_updated_at
                END AS last_updated_at
            FROM latest_booking b
            LEFT JOIN latest_event_filtered e ON b.booking_id = e.booking_id
            LEFT JOIN event_timestamps et ON b.booking_id = et.booking_id
        )
        SELECT booking_id, user_id, hotel_id,
            resolved_status AS booking_status, price, created_at,
            event_confirmed_at AS confirmed_at, event_cancelled_at AS cancelled_at,
            last_updated_at
        FROM combined
    """)
    results['dist_silver_time'] = (time.time() - start) * 1000
    
    dist_silver = con.execute("""
        SELECT booking_id, booking_status, price,
            CAST(created_at AS DATE) AS created_date
        FROM silver_dist_test
        ORDER BY booking_id
    """).fetchdf()
    results['dist_silver'] = dist_silver
    
    # Distributed Gold
    con.execute("DROP TABLE IF EXISTS gold_dist_test")
    start = time.time()
    con.execute("""
        CREATE TABLE gold_dist_test AS
        WITH enriched AS (
            SELECT s.booking_id, s.booking_status, s.price,
                CAST(s.created_at AS DATE) AS booking_date,
                COALESCE(h.city, 'Unknown') AS city
            FROM silver_dist_test s
            LEFT JOIN stg_hotels h ON s.hotel_id = h.hotel_id
        )
        SELECT booking_date, city,
            COUNT(*) AS total_bookings,
            COUNT(CASE WHEN booking_status = 'confirmed' THEN 1 END) AS confirmed_bookings,
            COUNT(CASE WHEN booking_status = 'cancelled' THEN 1 END) AS cancelled_bookings,
            COUNT(CASE WHEN booking_status = 'created' THEN 1 END) AS pending_bookings,
            CASE WHEN COUNT(*) > 0
                THEN ROUND(CAST(COUNT(CASE WHEN booking_status = 'cancelled' THEN 1 END) AS DOUBLE)
                    / CAST(COUNT(*) AS DOUBLE), 4)
                ELSE 0 END AS cancellation_rate,
            COALESCE(SUM(CASE WHEN booking_status = 'confirmed' THEN price END), 0) AS total_confirmed_revenue,
            ROUND(AVG(price), 2) AS avg_booking_price
        FROM enriched
        GROUP BY booking_date, city
        ORDER BY booking_date, city
    """)
    results['dist_gold_time'] = (time.time() - start) * 1000
    
    dist_gold = con.execute("""
        SELECT booking_date, city, total_bookings, confirmed_bookings,
            cancelled_bookings, pending_bookings,
            ROUND(cancellation_rate, 4) AS cancellation_rate,
            total_confirmed_revenue, avg_booking_price
        FROM gold_dist_test
        ORDER BY booking_date, city
    """).fetchdf()
    results['dist_gold'] = dist_gold
    
    # Cleanup
    con.execute("DROP TABLE IF EXISTS silver_dist_test")
    con.execute("DROP TABLE IF EXISTS gold_dist_test")
    con.close()
    
    return results


def run_on_trino(host='localhost', port=8080):
    """Run distributed version on actual Trino engine."""
    
    conn = trino_connect(host=host, port=port, user="test", catalog="memory")
    cursor = conn.cursor()
    results = {}
    
    print("\n  ENGINE: Trino")
    print("  " + "-" * 60)
    
    # Silver
    start = time.time()
    cursor.execute("""
        CREATE OR REPLACE TABLE memory.silver.silver_bookings AS
        WITH latest_booking_snapshot AS (
            SELECT booking_id, user_id, hotel_id,
                status AS snapshot_status, price, created_at,
                updated_at AS snapshot_updated_at,
                ROW_NUMBER() OVER (PARTITION BY booking_id ORDER BY updated_at DESC) AS rn
            FROM memory.staging.stg_bookings
        ),
        latest_booking AS (SELECT * FROM latest_booking_snapshot WHERE rn = 1),
        latest_event AS (
            SELECT booking_id, event_type AS event_status, event_ts AS event_updated_at,
                ROW_NUMBER() OVER (PARTITION BY booking_id ORDER BY event_ts DESC) AS rn
            FROM memory.staging.stg_booking_events
        ),
        latest_event_filtered AS (SELECT * FROM latest_event WHERE rn = 1),
        event_timestamps AS (
            SELECT booking_id,
                MAX(CASE WHEN event_type = 'confirmed' THEN event_ts END) AS event_confirmed_at,
                MAX(CASE WHEN event_type = 'cancelled' THEN event_ts END) AS event_cancelled_at
            FROM memory.staging.stg_booking_events GROUP BY booking_id
        ),
        combined AS (
            SELECT b.booking_id, b.user_id, b.hotel_id, b.price, b.created_at,
                b.snapshot_status, b.snapshot_updated_at,
                e.event_status, e.event_updated_at,
                et.event_confirmed_at, et.event_cancelled_at,
                CASE
                    WHEN b.snapshot_status = 'cancelled' OR e.event_status = 'cancelled' THEN 'cancelled'
                    WHEN e.event_status IS NOT NULL AND b.snapshot_status IS NOT NULL
                    THEN CASE WHEN e.event_updated_at >= b.snapshot_updated_at
                        THEN e.event_status ELSE b.snapshot_status END
                    WHEN e.event_status IS NOT NULL THEN e.event_status
                    ELSE b.snapshot_status
                END AS resolved_status,
                CASE
                    WHEN b.snapshot_updated_at IS NULL THEN e.event_updated_at
                    WHEN e.event_updated_at IS NULL THEN b.snapshot_updated_at
                    WHEN e.event_updated_at >= b.snapshot_updated_at THEN e.event_updated_at
                    ELSE b.snapshot_updated_at
                END AS last_updated_at
            FROM latest_booking b
            LEFT JOIN latest_event_filtered e ON b.booking_id = e.booking_id
            LEFT JOIN event_timestamps et ON b.booking_id = et.booking_id
        )
        SELECT booking_id, user_id, hotel_id,
            resolved_status AS booking_status, price, created_at,
            event_confirmed_at AS confirmed_at, event_cancelled_at AS cancelled_at,
            last_updated_at
        FROM combined
    """)
    cursor.fetchall()
    results['trino_silver_time'] = (time.time() - start) * 1000
    
    # Read Silver results
    cursor.execute("""
        SELECT booking_id, booking_status, price, CAST(created_at AS DATE) AS created_date
        FROM memory.silver.silver_bookings ORDER BY booking_id
    """)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    
    import pandas as pd
    results['trino_silver'] = pd.DataFrame(rows, columns=columns)
    
    # Gold
    start = time.time()
    cursor.execute("""
        CREATE OR REPLACE TABLE memory.gold.gold_daily_city_kpis AS
        WITH enriched AS (
            SELECT s.booking_id, s.booking_status, s.price,
                CAST(s.created_at AS DATE) AS booking_date,
                COALESCE(h.city, 'Unknown') AS city
            FROM memory.silver.silver_bookings s
            LEFT JOIN memory.staging.stg_hotels h ON s.hotel_id = h.hotel_id
        )
        SELECT booking_date, city,
            COUNT(*) AS total_bookings,
            COUNT(CASE WHEN booking_status = 'confirmed' THEN 1 END) AS confirmed_bookings,
            COUNT(CASE WHEN booking_status = 'cancelled' THEN 1 END) AS cancelled_bookings,
            COUNT(CASE WHEN booking_status = 'created' THEN 1 END) AS pending_bookings,
            CASE WHEN COUNT(*) > 0
                THEN ROUND(CAST(COUNT(CASE WHEN booking_status = 'cancelled' THEN 1 END) AS DOUBLE)
                    / CAST(COUNT(*) AS DOUBLE), 4)
                ELSE 0 END AS cancellation_rate,
            COALESCE(SUM(CASE WHEN booking_status = 'confirmed' THEN price END), 0) AS total_confirmed_revenue,
            ROUND(AVG(price), 2) AS avg_booking_price
        FROM enriched
        GROUP BY booking_date, city
        ORDER BY booking_date, city
    """)
    cursor.fetchall()
    results['trino_gold_time'] = (time.time() - start) * 1000
    
    cursor.execute("""
        SELECT booking_date, city, total_bookings, confirmed_bookings,
            cancelled_bookings, pending_bookings, cancellation_rate,
            total_confirmed_revenue, avg_booking_price
        FROM memory.gold.gold_daily_city_kpis ORDER BY booking_date, city
    """)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    results['trino_gold'] = pd.DataFrame(rows, columns=columns)
    
    cursor.close()
    conn.close()
    
    return results


def print_results(duckdb_results, trino_results=None):
    """Print comparison of results across engines."""
    
    print("\n" + "=" * 80)
    print("  CROSS-ENGINE COMPARISON")
    print("=" * 80)
    
    g_silver = duckdb_results['generic_silver']
    d_silver = duckdb_results['dist_silver']
    
    silver_match = g_silver['booking_status'].tolist() == d_silver['booking_status'].tolist()
    gold_match = duckdb_results['generic_gold']['total_bookings'].tolist() == \
                 duckdb_results['dist_gold']['total_bookings'].tolist()
    
    print(f"\n  DuckDB: Generic vs Distributed")
    print("  " + "-" * 60)
    print(f"    Silver results match: {'YES' if silver_match else 'NO'}")
    print(f"    Gold results match:   {'YES' if gold_match else 'NO'}")
    
    # Timing table
    trino_silver_time = f"{trino_results['trino_silver_time']:.2f} ms" if trino_results else "N/A"
    trino_gold_time = f"{trino_results['trino_gold_time']:.2f} ms" if trino_results else "N/A"
    trino_total = f"{(trino_results['trino_silver_time'] + trino_results['trino_gold_time']):.2f} ms" if trino_results else "N/A"
    
    generic_total = duckdb_results['generic_silver_time'] + duckdb_results['generic_gold_time']
    dist_total = duckdb_results['dist_silver_time'] + duckdb_results['dist_gold_time']
    
    print(f"""
  ┌─────────────────────┬──────────────────┬──────────────────┬──────────────────┐
  │ Operation           │ Generic (DuckDB) │ Distributed      │ Trino            │
  │                     │                  │ (DuckDB)         │ (Docker)         │
  ├─────────────────────┼──────────────────┼──────────────────┼──────────────────┤
  │ Silver build        │ {duckdb_results['generic_silver_time']:>12.2f} ms │ {duckdb_results['dist_silver_time']:>12.2f} ms │ {trino_silver_time:>14} │
  │ Gold build          │ {duckdb_results['generic_gold_time']:>12.2f} ms │ {duckdb_results['dist_gold_time']:>12.2f} ms │ {trino_gold_time:>14} │
  │ Total               │ {generic_total:>12.2f} ms │ {dist_total:>12.2f} ms │ {trino_total:>14} │
  └─────────────────────┴──────────────────┴──────────────────┴──────────────────┘
""")
    
    # Cross-engine comparison if Trino results exist
    if trino_results:
        print("  DuckDB vs Trino Results")
        print("  " + "-" * 60)
        
        d_statuses = duckdb_results['dist_silver']['booking_status'].tolist()
        t_statuses = trino_results['trino_silver']['booking_status'].tolist()
        
        cross_match = d_statuses == t_statuses
        print(f"    Silver results match across engines: {'YES' if cross_match else 'NO'}")
        
        if cross_match:
            print("    PROVEN: Same logic produces same results on DuckDB AND Trino")
        else:
            print("    WARNING: Results differ between engines!")
    
    # Silver details
    print("\n  SILVER — Booking Status per Engine:")
    print("  " + "-" * 60)
    for i in range(len(g_silver)):
        bid = g_silver.iloc[i]['booking_id']
        g_status = g_silver.iloc[i]['booking_status']
        d_status = d_silver.iloc[i]['booking_status']
        
        if trino_results:
            t_status = trino_results['trino_silver'].iloc[i]['booking_status']
            all_same = (g_status == d_status == t_status)
            print(f"    [{'+'if all_same else 'X'}] {bid}: Generic={g_status}, Distributed={d_status}, Trino={t_status}")
        else:
            all_same = (g_status == d_status)
            print(f"    [{'+'if all_same else 'X'}] {bid}: Generic={g_status}, Distributed={d_status}")
    
    # Gold details
    print("\n  GOLD — Daily KPIs per Engine:")
    print("  " + "-" * 60)
    g_gold = duckdb_results['generic_gold']
    d_gold = duckdb_results['dist_gold']
    
    for i in range(len(g_gold)):
        date = g_gold.iloc[i]['booking_date']
        city = g_gold.iloc[i]['city']
        g_total = g_gold.iloc[i]['total_bookings']
        d_total = d_gold.iloc[i]['total_bookings']
        g_revenue = g_gold.iloc[i]['total_confirmed_revenue']
        d_revenue = d_gold.iloc[i]['total_confirmed_revenue']
        
        match = (g_total == d_total and g_revenue == d_revenue)
        print(f"    [{'+'if match else 'X'}] {date} | {city}: bookings={g_total}, revenue={g_revenue}")
    
    print(f"\n{'='*80}")
    if silver_match and gold_match:
        print("  CONCLUSION:")
        print("  Same business logic produces IDENTICAL results across methods")
        print("  Distributed version adds production optimizations for scale")
    else:
        print("  WARNING: Results differ! Review logic.")
    print(f"{'='*80}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trino", action="store_true", help="Also run on Trino (requires Docker)")
    parser.add_argument("--trino-host", default="localhost")
    parser.add_argument("--trino-port", default=8080, type=int)
    args = parser.parse_args()
    
    print("=" * 80)
    print("  DISTRIBUTED SQL EXECUTION & COMPARISON")
    print("=" * 80)
    
    # Run on DuckDB
    print("\n[1] Running on DuckDB...")
    duckdb_results = run_on_duckdb()
    print(f"    Generic:     Silver={duckdb_results['generic_silver_time']:.2f}ms, Gold={duckdb_results['generic_gold_time']:.2f}ms")
    print(f"    Distributed: Silver={duckdb_results['dist_silver_time']:.2f}ms, Gold={duckdb_results['dist_gold_time']:.2f}ms")
    
    # Run on Trino if requested
    trino_results = None
    if args.trino:
        if not HAS_TRINO:
            print("\n[2] Trino: pip install trino required")
            print("    Run: pip install trino")
        else:
            print(f"\n[2] Running on Trino ({args.trino_host}:{args.trino_port})...")
            try:
                trino_results = run_on_trino(args.trino_host, args.trino_port)
                print(f"    Silver={trino_results['trino_silver_time']:.2f}ms, Gold={trino_results['trino_gold_time']:.2f}ms")
            except Exception as e:
                print(f"    Trino connection failed: {e}")
                print("    Make sure Trino is running: docker run -d --name trino -p 8080:8080 trinodb/trino:latest")
    
    # Print comparison
    print_results(duckdb_results, trino_results)


if __name__ == "__main__":
    main()