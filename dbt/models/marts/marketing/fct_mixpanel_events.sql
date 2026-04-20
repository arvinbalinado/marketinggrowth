-- models/marts/marketing/fct_mixpanel_events.sql
-- Daily event volume and unique user counts per event × channel × device.
-- Feeds campaign performance, product analytics, and growth dashboards.

{{
  config(
    materialized = 'table',
    partition_by = {
      'field': 'event_date',
      'data_type': 'date',
      'granularity': 'day'
    },
    cluster_by = ['event_name', 'channel_normalized']
  )
}}

with events as (
    select * from {{ ref('stg_mixpanel_events') }}
),

daily_summary as (
    select
        event_date,
        date_trunc(event_date, week)  as week_start,
        date_trunc(event_date, month) as month_start,
        event_name,
        channel_normalized,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        country_code,
        operating_system,
        device_type,

        -- Volume
        count(*)                        as event_count,
        count(distinct distinct_id)     as unique_users,

        -- Engagement proxy: events per user
        safe_divide(
            count(*),
            nullif(count(distinct distinct_id), 0)
        )                               as events_per_user

    from events
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
)

select * from daily_summary
