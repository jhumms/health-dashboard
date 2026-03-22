with source as (
    select * from {{ source('raw', 'daylio_logs') }}
),

renamed as (
    select
        full_date::date                                 as date,
        (raw_data->>'mood')                             as mood,
        (raw_data->>'activities')                       as activities_raw,
        (raw_data->>'note_title')                       as note_title,
        (raw_data->>'note')                             as note,
        (raw_data->>'time')                             as time_of_day,
        -- Map mood text to numeric score (Daylio default scale)
        case raw_data->>'mood'
            when 'rad'      then 5
            when 'good'     then 4
            when 'meh'      then 3
            when 'bad'      then 2
            when 'awful'    then 1
            else null
        end                                             as mood_score,
        pulled_at
    from source
)

select * from renamed
