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
    select date from {{ ref('stg_daylio_logs') }}
    union
    select date from {{ ref('stg_daily_strength_session') }}
    union
    select date from {{ ref('stg_manual_runs') }}
    union
    select date from {{ ref('stg_oura_workout') }}
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

manual_runs as (
    select
        date,
        round(sum(distance_miles), 2)                                   as distance_miles,
        round(sum(duration_seconds) / 60.0, 2)                          as duration_minutes,
        round(
            (sum(duration_seconds) / 60.0) / nullif(sum(distance_miles), 0),
            2
        )                                                               as pace_min_per_mile,
        count(*)                                                        as run_count,
        string_agg(notes, '; ' order by logged_at) filter (where notes is not null) as notes
    from {{ ref('stg_manual_runs') }}
    group by date
),

mood as (
    select * from {{ ref('stg_daylio_logs') }}
),

weather as (
    select * from {{ ref('stg_weather') }}
),

sleep_sessions as (
    select * from {{ ref('stg_oura_sleep') }}
),

-- Aggregate strength sessions by date (can have multiple per day)
strength as (
    select
        date,
        count(*)                                    as workout_count,
        sum(exercise_count)                         as total_exercises,
        sum(extract(epoch from duration) / 60)      as total_workout_minutes,
        string_agg(workout_name, ', ')              as workout_names
    from {{ ref('stg_daily_strength_session') }}
    where is_complete = true
    group by date
),

-- Aggregate all Oura-tracked workouts by date
oura_workouts as (
    select
        date,
        count(*)                                    as oura_workout_count,
        round(sum(duration_minutes), 0)             as oura_workout_minutes,
        round(sum(calories), 0)                     as oura_workout_calories,
        string_agg(activity, ', ' order by activity) as oura_workout_types
    from {{ ref('stg_oura_workout') }}
    group by date
),

-- Oura-tracked runs specifically
oura_runs as (
    select
        date,
        count(*)                                    as oura_run_count,
        round(sum(duration_minutes), 1)             as oura_run_minutes,
        round(sum(distance_m) / 1609.34, 2)         as oura_run_distance_miles,
        round(sum(calories), 0)                     as oura_run_calories
    from {{ ref('stg_oura_workout') }}
    where activity = 'running'
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
    readiness.resting_heart_rate_score,

    -- Heart rate & HRV from full sleep session
    sleep_sessions.resting_heart_rate,
    sleep_sessions.average_hrv,

    -- Activity (Oura)
    activity.activity_score,
    activity.steps                                  as oura_steps,
    activity.active_calories,
    activity.total_calories,
    activity.high_activity_time_s,
    activity.medium_activity_time_s,
    activity.sedentary_time_s,
    nullif(activity.steps, 0)                       as preferred_steps,

    -- Oura workouts (all types)
    oura_workouts.oura_workout_count,
    oura_workouts.oura_workout_minutes,
    oura_workouts.oura_workout_calories,
    oura_workouts.oura_workout_types,

    -- Runs: manual entry is primary; Oura is supplementary fallback only
    coalesce(oura_runs.oura_run_count, 0)           as oura_run_count,
    oura_runs.oura_run_minutes,
    oura_runs.oura_run_distance_miles,
    coalesce(
        manual_runs.distance_miles,
        oura_runs.oura_run_distance_miles
    )                                               as run_distance_miles,
    coalesce(
        manual_runs.duration_minutes,
        oura_runs.oura_run_minutes
    )                                               as run_duration_minutes,
    manual_runs.pace_min_per_mile                   as run_pace_min_per_mile,
    manual_runs.notes                               as run_notes,

    -- Mood (Daylio)
    mood.mood,
    mood.mood_score,
    mood.activities_raw                             as daylio_activities,
    mood.note_title,

    -- Strength training
    strength.workout_count,
    strength.total_exercises,
    strength.total_workout_minutes,
    strength.workout_names,

    -- Weather
    weather.city                                    as weather_city,
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
    weather.morning_temp_f,
    weather.afternoon_temp_f,
    weather.evening_temp_f,
    weather.morning_precip_prob,
    weather.afternoon_precip_prob,
    weather.evening_precip_prob,
    weather.likely_rain,
    weather.better_in_morning,
    weather.hot_day,
    weather.cold_day,

    -- Convenience flags
    (sleep.sleep_score is not null)                 as has_oura_data,
    (mood.mood is not null)                         as has_mood_log,
    (strength.workout_count is not null)            as has_workout,
    (oura_runs.date is not null or manual_runs.date is not null) as has_run,
    (weather.date is not null)                      as has_weather

from spine
left join sleep          on spine.date = sleep.date
left join readiness      on spine.date = readiness.date
left join activity       on spine.date = activity.date
left join oura_workouts  on spine.date = oura_workouts.date
left join oura_runs      on spine.date = oura_runs.date
left join manual_runs    on spine.date = manual_runs.date
left join mood           on spine.date = mood.date
left join strength       on spine.date = strength.date
left join sleep_sessions on spine.date = sleep_sessions.date
left join weather        on spine.date = weather.date

order by spine.date desc
