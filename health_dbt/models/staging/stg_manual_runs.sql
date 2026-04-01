with source as (
    select * from {{ source('raw', 'manual_runs') }}
),

enriched as (
    select
        id,
        date,
        distance_miles,
        duration_seconds,
        round(duration_seconds / 60.0, 2)                           as duration_minutes,
        -- pace in decimal minutes per mile (e.g. 8.5 = 8:30/mile)
        round((duration_seconds / 60.0) / nullif(distance_miles, 0), 2) as pace_min_per_mile,
        notes,
        logged_at
    from source
)

select * from enriched
