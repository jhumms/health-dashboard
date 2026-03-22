with source as (
    select * from {{ source('raw', 'oura_daily_readiness') }}
),

renamed as (
    select
        id,
        (raw_data->>'day')::date                                        as date,
        (raw_data->>'score')::integer                                   as readiness_score,
        (raw_data->>'temperature_deviation')::numeric                   as temperature_deviation,
        (raw_data->>'temperature_trend_deviation')::numeric             as temperature_trend_deviation,
        (raw_data->'contributors'->>'activity_balance')::integer        as activity_balance_score,
        (raw_data->'contributors'->>'body_temperature')::integer        as body_temperature_score,
        (raw_data->'contributors'->>'hrv_balance')::integer             as hrv_balance_score,
        (raw_data->'contributors'->>'previous_day_activity')::integer   as previous_day_activity_score,
        (raw_data->'contributors'->>'previous_night')::integer          as previous_night_score,
        (raw_data->'contributors'->>'recovery_index')::integer          as recovery_index_score,
        (raw_data->'contributors'->>'resting_heart_rate')::integer      as resting_heart_rate_score,
        (raw_data->'contributors'->>'sleep_balance')::integer           as sleep_balance_score,
        pulled_at
    from source
)

select * from renamed
