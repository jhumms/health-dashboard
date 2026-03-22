with source as (
    select * from {{ source('raw', 'oura_sleep') }}
),

renamed as (
    select
        (raw_data->>'day')::date                            as date,
        (raw_data->>'lowest_heart_rate')::integer           as resting_heart_rate,
        (raw_data->>'average_hrv')::integer                 as average_hrv
    from source
    where raw_data->>'type' = 'long_sleep'
)

select * from renamed
