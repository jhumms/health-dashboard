with source as (
    select * from {{ source('raw', 'daily_strength_session') }}
),

renamed as (
    select
        id,
        (raw_data->>'name')                                         as workout_name,
        -- endDate is a Unix timestamp in milliseconds
        to_timestamp((raw_data->>'endDate')::bigint / 1000)::date  as date,
        to_timestamp((raw_data->>'startDate')::bigint / 1000)       as started_at,
        to_timestamp((raw_data->>'endDate')::bigint / 1000)         as ended_at,
        (
            to_timestamp((raw_data->>'endDate')::bigint / 1000)
            - to_timestamp((raw_data->>'startDate')::bigint / 1000)
        )                                                           as duration,
        (raw_data->>'isComplete')::boolean                          as is_complete,
        jsonb_array_length(
            raw_data->'workoutSessionExercises'
        )                                                           as exercise_count,
        raw_data->'workoutSessionExercises'                         as exercises_json,
        pulled_at
    from source
)

select * from renamed
