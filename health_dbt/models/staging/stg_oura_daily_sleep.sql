with source as (
    select * from {{ source('raw', 'oura_daily_sleep') }}
),

renamed as (
    select
        id,
        (raw_data->>'day')::date                                    as date,
        (raw_data->>'score')::integer                               as sleep_score,
        (raw_data->'contributors'->>'deep_sleep')::integer          as deep_sleep_score,
        (raw_data->'contributors'->>'efficiency')::integer          as efficiency_score,
        (raw_data->'contributors'->>'latency')::integer             as latency_score,
        (raw_data->'contributors'->>'rem_sleep')::integer           as rem_sleep_score,
        (raw_data->'contributors'->>'restfulness')::integer         as restfulness_score,
        (raw_data->'contributors'->>'timing')::integer              as timing_score,
        (raw_data->'contributors'->>'total_sleep')::integer         as total_sleep_score,
        pulled_at
    from source
)

select * from renamed
