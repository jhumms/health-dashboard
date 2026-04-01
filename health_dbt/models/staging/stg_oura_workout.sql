with source as (
    select * from {{ source('raw', 'oura_workout') }}
),

renamed as (
    select
        id,
        (raw_data->>'day')::date                                        as date,
        raw_data->>'activity'                                           as activity,
        raw_data->>'intensity'                                          as intensity,
        raw_data->>'source'                                             as source,
        (raw_data->>'calories')::numeric                                as calories,
        (raw_data->>'distance')::numeric                                as distance_m,
        (raw_data->>'start_datetime')::timestamptz                      as started_at,
        (raw_data->>'end_datetime')::timestamptz                        as ended_at,
        round(
            extract(epoch from
                (raw_data->>'end_datetime')::timestamptz
                - (raw_data->>'start_datetime')::timestamptz
            ) / 60.0
        , 1)                                                            as duration_minutes,
        pulled_at
    from source
)

select * from renamed
