with source as (
    select * from {{ source('raw', 'garmin_daily_steps') }}
),

normalized as (
    select
        -- Normalize the one known bad date format (01/01/2023 → 2023-01-01)
        case
            when date ~ '^\d{2}/\d{2}/\d{4}$'
                then to_date(date, 'MM/DD/YYYY')
            else date::date
        end                     as date,
        steps,
        pulled_at
    from source
),

-- Garmin logs multiple sessions per day; sum them into one daily total
aggregated as (
    select
        date,
        sum(steps)              as steps,
        max(pulled_at)          as pulled_at
    from normalized
    group by date
)

select * from aggregated
