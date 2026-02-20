/*
    Gold Layer - Daily Booking KPIs by City
    
    =============================================================
    DESIGN DECISIONS
    =============================================================
    
    1. GRAIN: One row per day x city
    
    2. DATE DIMENSION CHOICE:
       We use created_at (cast to DATE) as the reporting date.
       
       Rationale: 
       - created_at is immutable and represents when demand occurred
       - Using event_ts or updated_at would shift a booking's date 
         when it gets confirmed/cancelled, breaking dashboard stability
       - Standard practice in travel analytics: "When was booking made?"
       
       Trade-off: A booking created Jan 10 and cancelled Jan 11 
       counts as both a total booking AND cancellation on Jan 10.
    
    3. METRIC DEFINITIONS:
       - total_bookings:           COUNT of all bookings created on this day
       - confirmed_bookings:       COUNT where final status = 'confirmed'
       - cancelled_bookings:       COUNT where final status = 'cancelled'
       - pending_bookings:         COUNT where final status = 'created'
       - cancellation_rate:        cancelled / total (0 if total = 0)
       - total_confirmed_revenue:  SUM(price) for confirmed only
       - avg_booking_price:        AVG(price) across ALL bookings
       - avg_confirmed_price:      AVG(price) for confirmed only
       
    4. DOUBLE COUNTING PREVENTION:
       - Silver guarantees exactly one row per booking_id
       - Each booking assigned to exactly one day (created_at date)
       - Each booking assigned to exactly one city (via hotel join)
    
    5. DISTRIBUTED CONSIDERATIONS:
       - Hotels table is small (reference data) -> BROADCAST join candidate
         Trino: automatic broadcast below threshold
         Spark: use /*+ BROADCAST(h) */ hint
       - GROUP BY (booking_date, city) has low cardinality -> cheap shuffle
    
    =============================================================
*/

WITH bookings_enriched AS (
    SELECT
        s.booking_id,
        s.booking_status,
        s.price,
        CAST(s.created_at AS DATE)          AS booking_date,
        COALESCE(h.city, 'Unknown')         AS city,
        COALESCE(h.star_rating, 0)          AS star_rating
    FROM {{ ref('silver_bookings') }} s
    LEFT JOIN {{ ref('stg_hotels') }} h
        ON s.hotel_id = h.hotel_id
    /*
        BROADCAST JOIN NOTE:
        hotels is a small dimension table (< 1MB).
        In production:
        - Trino auto-broadcasts tables below join_distribution threshold
        - Spark: SELECT /*+ BROADCAST(h) */ ...
        This eliminates shuffling the large bookings table by hotel_id
    */
),

daily_city_agg AS (
    SELECT
        booking_date,
        city,
        
        -- Volume metrics
        COUNT(*)                                                    AS total_bookings,
        
        COUNT(CASE 
            WHEN booking_status = 'confirmed' THEN 1 
        END)                                                        AS confirmed_bookings,
        
        COUNT(CASE 
            WHEN booking_status = 'cancelled' THEN 1 
        END)                                                        AS cancelled_bookings,
        
        COUNT(CASE 
            WHEN booking_status = 'created' THEN 1 
        END)                                                        AS pending_bookings,
        
        -- Rate metric with safe division
        CASE 
            WHEN COUNT(*) > 0 
            THEN ROUND(
                CAST(COUNT(CASE WHEN booking_status = 'cancelled' THEN 1 END) AS DECIMAL(10,4))
                / CAST(COUNT(*) AS DECIMAL(10,4)),
                4
            )
            ELSE CAST(0 AS DECIMAL(6,4))
        END                                                         AS cancellation_rate,
        
        -- Revenue: confirmed only
        COALESCE(
            SUM(CASE WHEN booking_status = 'confirmed' THEN price END), 
            CAST(0 AS DECIMAL(14,2))
        )                                                           AS total_confirmed_revenue,
        
        -- Average prices
        ROUND(AVG(price), 2)                                        AS avg_booking_price,
        
        ROUND(
            COALESCE(
                AVG(CASE WHEN booking_status = 'confirmed' THEN price END),
                CAST(0 AS DECIMAL(10,2))
            ), 
            2
        )                                                           AS avg_confirmed_price,
        
        CURRENT_TIMESTAMP                                           AS _computed_at
        
    FROM bookings_enriched
    GROUP BY booking_date, city
)

SELECT
    booking_date,
    city,
    total_bookings,
    confirmed_bookings,
    cancelled_bookings,
    pending_bookings,
    cancellation_rate,
    total_confirmed_revenue,
    avg_booking_price,
    avg_confirmed_price,
    _computed_at
FROM daily_city_agg
ORDER BY booking_date, city
