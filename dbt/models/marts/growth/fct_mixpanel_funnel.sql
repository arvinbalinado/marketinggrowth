-- models/marts/growth/fct_mixpanel_funnel.sql
-- Mixpanel funnel conversion rates by step, date, and funnel definition.
-- Complements HubSpot funnel data (fct_funnel_conversion) for product-led funnels.

{{
  config(
    materialized = 'table',
    partition_by = {'field': 'date', 'data_type': 'date', 'granularity': 'day'}
  )
}}

with funnel_steps as (
    select * from {{ ref('stg_mixpanel_funnels') }}
),

-- Attach entry count at step 0 per funnel/date for relative drop-off
entry_counts as (
    select
        date,
        funnel_id,
        funnel_name,
        entered_count as top_of_funnel_count
    from funnel_steps
    where step_index = 0
),

enriched as (
    select
        fs.date,
        date_trunc(fs.date, week)  as week_start,
        date_trunc(fs.date, month) as month_start,
        fs.funnel_id,
        fs.funnel_name,
        fs.step_index,
        fs.step_label,
        fs.entered_count,
        fs.step_conversion_rate,
        fs.avg_time_to_convert_sec,
        -- Minutes / hours convenience columns
        safe_divide(fs.avg_time_to_convert_sec, 60)   as avg_time_to_convert_min,
        safe_divide(fs.avg_time_to_convert_sec, 3600)  as avg_time_to_convert_hrs,
        fs.goal_completions,
        fs.overall_funnel_conversion_rate,

        -- Drop-off rate relative to top of funnel
        safe_divide(
            ec.top_of_funnel_count - fs.entered_count,
            nullif(ec.top_of_funnel_count, 0)
        ) as cumulative_drop_off_rate,

        ec.top_of_funnel_count

    from funnel_steps fs
    left join entry_counts ec
        on fs.date = ec.date
       and fs.funnel_id = ec.funnel_id
)

select * from enriched
