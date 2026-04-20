-- models/staging/stg_mixpanel_funnels.sql
-- Cleans Mixpanel funnel step conversion data.

with source as (
    select * from {{ source('raw', 'mixpanel_funnel_conversions') }}
),

renamed as (
    select
        cast(date as date)                              as date,
        cast(funnel_id as string)                       as funnel_id,
        funnel_name,
        cast(step_index as int64)                       as step_index,
        step_label,
        coalesce(cast(count as int64), 0)               as entered_count,
        coalesce(cast(conversion_rate as float64), 0.0) as step_conversion_rate,
        coalesce(cast(avg_time_to_convert_sec as float64), 0.0)
                                                        as avg_time_to_convert_sec,
        coalesce(cast(goal_count as int64), 0)          as goal_completions,
        coalesce(cast(goal_conversion_rate as float64), 0.0)
                                                        as overall_funnel_conversion_rate,
        -- Derived
        safe_divide(1.0, nullif(cast(avg_time_to_convert_sec as float64), 0))
                                                        as time_to_convert_inverse,
        _extracted_at

    from source
    where date is not null
      and funnel_id is not null
)

select * from renamed
