with source as (
    select * from {{ source('raw', 'manual_runs') }}
)

select
    id,
    date,
    distance_miles,
    duration_seconds,
    round(duration_seconds / 60.0, 2)                              as duration_minutes,
    round((duration_seconds / 60.0) / nullif(distance_miles, 0), 2) as pace_min_per_mile,
    notes,
    logged_at
from source
