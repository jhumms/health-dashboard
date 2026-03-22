{{
    config(
        materialized='table',
        indexes=[{'columns': ['date'], 'unique': True}]
    )
}}

-- Spine: all dates that appear in any source
with spine as (
    select date from {{ ref('stg_oura_daily_sleep') }}
    union
    select date from {{ ref('stg_oura_daily_readiness') }}
    union
    select date from {{ ref('stg_oura_daily_activity') }}
    union
    select date from {{ ref('stg_garmin_daily_steps') }}
    union
    select date from {{ ref('stg_daylio_logs') }}
    union
    select date from {{ ref('stg_daily_strength_session') }}
),

sleep as (
    select * from {{ ref('stg_oura_daily_sleep') }}
),

readiness as (
    select * from {{ ref('stg_oura_daily_readiness') }}
),

activity as (
    select * from {{ ref('stg_oura_daily_activity') }}
),

steps as (
    select * from {{ ref('stg_garmin_daily_steps') }}
),

mood as (
    select * from {{ ref('stg_daylio_logs') }}
),

weather as (
    select * from {{ ref('stg_weather') }}
),

-- Aggregate strength sessions by date (can have multiple per day)
strength as (
    select
        date,
        count(*)                        as workout_count,
        sum(exercise_count)             as total_exercises,
        sum(extract(epoch from duration) / 60)  as total_workout_minutes,
        string_agg(workout_name, ', ')  as workout_names
    from {{ ref('stg_daily_strength_session') }}
    where is_complete = true
    group by date
)

select
    spine.date,

    -- Sleep
    sleep.sleep_score,
    sleep.deep_sleep_score,
    sleep.rem_sleep_score,
    sleep.restfulness_score,

    -- Readiness
    readiness.readiness_score,
    readiness.hrv_balance_score,
    readiness.recovery_index_score,
    readiness.temperature_deviation,

    -- Activity (Oura)
    activity.activity_score,
    activity.steps                              as oura_steps,
    activity.active_calories,
    activity.total_calories,
    activity.high_activity_time_s,
    activity.medium_activity_time_s,
    activity.sedentary_time_s,

    -- Steps (Garmin)
    steps.steps                                 as garmin_steps,

    -- Preferred steps: Oura first, fall back to Garmin if Oura missing or zero
    coalesce(
        nullif(activity.steps, 0),
        nullif(steps.steps, 0)
    )                                           as preferred_steps,

    -- Mood (Daylio)
    mood.mood,
    mood.mood_score,
    mood.activities_raw                         as daylio_activities,
    mood.note_title,

    -- Strength training
    strength.workout_count,
    strength.total_exercises,
    strength.total_workout_minutes,
    strength.workout_names,

    -- Weather
    weather.city                                as weather_city,
    weather.temp_max_f,
    weather.temp_min_f,
    weather.temp_max_c,
    weather.temp_min_c,
    weather.precip_sum_mm,
    weather.precip_prob_max,
    weather.weather_desc,
    weather.sunrise,
    weather.sunset,
    weather.morning_temp_c,
    weather.afternoon_temp_c,
    weather.evening_temp_c,
    weather.morning_precip_prob,
    weather.afternoon_precip_prob,
    weather.evening_precip_prob,
    weather.likely_rain,
    weather.better_in_morning,
    weather.hot_day,
    weather.cold_day,

    -- Convenience flags
    (sleep.sleep_score is not null)             as has_oura_data,
    (steps.steps is not null)                   as has_garmin_steps,
    (mood.mood is not null)                     as has_mood_log,
    (strength.workout_count is not null)        as has_workout,
    (weather.date is not null)                  as has_weather

from spine
left join sleep     on spine.date = sleep.date
left join readiness on spine.date = readiness.date
left join activity  on spine.date = activity.date
left join steps     on spine.date = steps.date
left join mood      on spine.date = mood.date
left join strength  on spine.date = strength.date
left join weather   on spine.date = weather.date

order by spine.date desc
