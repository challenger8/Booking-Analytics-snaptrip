/*
    Silver Bookings - Latest correct state per booking
    
    =============================================================
    DESIGN DECISIONS & ASSUMPTIONS
    =============================================================
    
    1. GRAIN: One row per booking_id
    
    2. DEDUPLICATION STRATEGY:
       - bookings_raw: take the row with MAX(updated_at) per booking_id
         to get the latest CDC snapshot.
       - booking_events_raw: take the event with MAX(event_ts) per booking_id
         to determine the latest known status from the event stream.
    
    3. CONFLICT RESOLUTION (the core design choice):
       - Dimensional attributes (user_id, hotel_id, price): sourced from 
         bookings_raw because events don't carry these attributes.
       - Status: We use a COALESCE approach comparing both sources.
         The source with the most recent timestamp wins for status.
         
         Rationale: The event stream is append-only and captures the 
         precise moment of each state change. CDC snapshots may arrive 
         batched or delayed. When both sources disagree, the one with 
         the later timestamp is more likely to reflect reality.
    
    4. LATE-ARRIVING EVENTS:
       - A late-arriving event with an older event_ts than the latest 
         known update will NOT override the current state - we compare 
         timestamps, not arrival order.
    
    5. STATUS HIERARCHY (terminal state guard):
       - "cancelled" is treated as a terminal state. Once a booking 
         reaches cancelled (by either source), we preserve it regardless 
         of late events that might show an earlier non-terminal state.
    
    6. TIMESTAMPS:
       - created_at: from bookings_raw (immutable, set once)
       - last_updated_at: MAX across both sources for auditability
    
    7. DISTRIBUTED ENGINE CONSIDERATIONS:
       - ROW_NUMBER triggers a shuffle by booking_id (unavoidable for CDC dedup)
       - Both dedup CTEs share the same partition key (booking_id), enabling
         co-partitioned joins downstream (Spark/Trino can skip second shuffle)
       - Explicit CASE instead of GREATEST() for cross-engine NULL safety
         (Trino GREATEST returns NULL if ANY arg is NULL; Spark skips NULLs)
    
    =============================================================
*/

WITH latest_booking_snapshot AS (
    -- Get the latest CDC snapshot per booking from bookings_raw
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
    FROM {{ ref('stg_bookings') }}
),

latest_booking AS (
    -- Filter BEFORE joining to minimize data volume in shuffle
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
    -- Get the latest event per booking from the event stream
    SELECT
        booking_id,
        event_type          AS event_status,
        event_ts            AS event_updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY booking_id
            ORDER BY event_ts DESC
        ) AS rn
    FROM {{ ref('stg_booking_events') }}
),

latest_event_filtered AS (
    SELECT
        booking_id,
        event_status,
        event_updated_at
    FROM latest_event
    WHERE rn = 1
),

-- Extract specific event timestamps for enrichment
event_timestamps AS (
    SELECT
        booking_id,
        MAX(CASE WHEN event_type = 'created'   THEN event_ts END) AS event_created_at,
        MAX(CASE WHEN event_type = 'confirmed'  THEN event_ts END) AS event_confirmed_at,
        MAX(CASE WHEN event_type = 'cancelled'  THEN event_ts END) AS event_cancelled_at
    FROM {{ ref('stg_booking_events') }}
    GROUP BY booking_id
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

        /*
            RESOLVED STATUS LOGIC
            
            Priority 1: If EITHER source shows 'cancelled', the booking 
                        is cancelled (terminal state - cannot be undone)
            Priority 2: If sources disagree and neither is cancelled,
                        pick the one with the later timestamp
            Priority 3: If only one source exists, use that
        */
        CASE
            -- Terminal state: cancelled in either source
            WHEN b.snapshot_status = 'cancelled' 
              OR e.event_status = 'cancelled'
            THEN 'cancelled'
            
            -- Both sources exist, neither cancelled -> latest timestamp wins
            WHEN e.event_status IS NOT NULL 
             AND b.snapshot_status IS NOT NULL
            THEN CASE
                    WHEN e.event_updated_at >= b.snapshot_updated_at 
                    THEN e.event_status
                    ELSE b.snapshot_status
                 END
            
            -- Only one source exists
            WHEN e.event_status IS NOT NULL 
            THEN e.event_status
            
            ELSE b.snapshot_status
        END AS resolved_status,
        
        /*
            Cross-engine safe MAX timestamp
            GREATEST() handles NULLs differently:
            - Trino: returns NULL if ANY argument is NULL
            - Spark: returns the non-NULL value
            Using explicit CASE for safety
        */
        CASE
            WHEN b.snapshot_updated_at IS NULL THEN e.event_updated_at
            WHEN e.event_updated_at IS NULL THEN b.snapshot_updated_at
            WHEN e.event_updated_at >= b.snapshot_updated_at THEN e.event_updated_at
            ELSE b.snapshot_updated_at
        END AS last_updated_at
        
    FROM latest_booking b
    LEFT JOIN latest_event_filtered e
        ON b.booking_id = e.booking_id
    LEFT JOIN event_timestamps et
        ON b.booking_id = et.booking_id
)

SELECT
    booking_id,
    user_id,
    hotel_id,
    resolved_status                     AS booking_status,
    price,
    created_at,
    
    -- Enriched timestamps from event stream
    event_confirmed_at                  AS confirmed_at,
    event_cancelled_at                  AS cancelled_at,
    
    last_updated_at,
    
    -- Audit columns for data quality monitoring
    snapshot_status                      AS _raw_snapshot_status,
    event_status                        AS _raw_event_status,
    CASE 
        WHEN snapshot_status IS NOT NULL 
         AND event_status IS NOT NULL 
         AND snapshot_status != event_status
        THEN TRUE 
        ELSE FALSE 
    END                                 AS _had_conflict,
    
    CURRENT_TIMESTAMP                   AS _loaded_at

FROM combined
