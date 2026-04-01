with source as (
    select * from {{ source('raw', 'weather_daily') }}
),

renamed as (
    select
        (raw_data->>'date')::date                           as date,
        (raw_data->>'city')                                 as city,
        (raw_data->>'latitude')::numeric                    as latitude,
        (raw_data->>'longitude')::numeric                   as longitude,

        -- Daily summary
        (raw_data->>'temp_max_c')::numeric                  as temp_max_c,
        (raw_data->>'temp_min_c')::numeric                  as temp_min_c,
        round(((raw_data->>'temp_max_c')::numeric * 9/5 + 32)::numeric, 1) as temp_max_f,
        round(((raw_data->>'temp_min_c')::numeric * 9/5 + 32)::numeric, 1) as temp_min_f,
        (raw_data->>'precip_sum_mm')::numeric               as precip_sum_mm,
        (raw_data->>'precip_prob_max')::integer             as precip_prob_max,
        (raw_data->>'weathercode')::integer                 as weathercode,
        (raw_data->>'weather_desc')                         as weather_desc,
        (raw_data->>'sunrise')                              as sunrise,
        (raw_data->>'sunset')                               as sunset,

        -- Time-of-day breakdowns (key for AI recommendations)
        (raw_data->>'morning_temp_c')::numeric              as morning_temp_c,
        (raw_data->>'afternoon_temp_c')::numeric            as afternoon_temp_c,
        (raw_data->>'evening_temp_c')::numeric              as evening_temp_c,
        round(((raw_data->>'morning_temp_c')::numeric * 9/5 + 32)::numeric, 1)   as morning_temp_f,
        round(((raw_data->>'afternoon_temp_c')::numeric * 9/5 + 32)::numeric, 1) as afternoon_temp_f,
        round(((raw_data->>'evening_temp_c')::numeric * 9/5 + 32)::numeric, 1)   as evening_temp_f,
        (raw_data->>'morning_precip_prob')::numeric         as morning_precip_prob,
        (raw_data->>'afternoon_precip_prob')::numeric       as afternoon_precip_prob,
        (raw_data->>'evening_precip_prob')::numeric         as evening_precip_prob,

        -- Derived flags for AI prompt building
        (raw_data->>'precip_prob_max')::integer > 50        as likely_rain,
        (raw_data->>'morning_precip_prob')::numeric < 20
            and (raw_data->>'evening_precip_prob')::numeric > 40
                                                            as better_in_morning,
        round(((raw_data->>'temp_max_c')::numeric * 9/5 + 32)::numeric, 1) > 82  as hot_day,
        round(((raw_data->>'temp_max_c')::numeric * 9/5 + 32)::numeric, 1) < 41 as cold_day,

        pulled_at
    from source
)

select * from renamed
