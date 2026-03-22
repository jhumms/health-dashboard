with source as (
    select * from {{ source('raw', 'oura_daily_activity') }}
),

renamed as (
    select
        id,
        (raw_data->>'day')::date                                as date,
        (raw_data->>'score')::integer                           as activity_score,
        (raw_data->>'steps')::integer                           as steps,
        (raw_data->>'active_calories')::integer                 as active_calories,
        (raw_data->>'total_calories')::integer                  as total_calories,
        (raw_data->>'target_calories')::integer                 as target_calories,
        (raw_data->>'equivalent_walking_distance')::integer     as equivalent_walking_distance_m,
        (raw_data->>'high_activity_time')::integer              as high_activity_time_s,
        (raw_data->>'medium_activity_time')::integer            as medium_activity_time_s,
        (raw_data->>'low_activity_time')::integer               as low_activity_time_s,
        (raw_data->>'sedentary_time')::integer                  as sedentary_time_s,
        (raw_data->>'resting_time')::integer                    as resting_time_s,
        (raw_data->>'non_wear_time')::integer                   as non_wear_time_s,
        (raw_data->>'average_met_minutes')::numeric             as average_met_minutes,
        (raw_data->>'inactivity_alerts')::integer               as inactivity_alerts,
        pulled_at
    from source
)

select * from renamed
