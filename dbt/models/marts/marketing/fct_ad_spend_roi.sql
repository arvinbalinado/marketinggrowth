-- models/marts/marketing/fct_ad_spend_roi.sql
-- Daily ROAS and ROI summary per channel and campaign.
-- Primary input for marketing ROI dashboards.

{{
  config(
    materialized = 'table',
    partition_by = {'field': 'date', 'data_type': 'date', 'granularity': 'day'}
  )
}}

with campaign_perf as (
    select * from {{ ref('fct_campaign_performance') }}
),

daily_channel as (
    select
        date,
        week_start,
        month_start,
        channel_source,
        campaign_id,
        campaign_name,

        -- Spend & revenue
        sum(cost_usd)             as total_spend,
        sum(conversion_value_usd) as total_conversion_value,
        sum(conversions)          as total_conversions,
        sum(impressions)          as total_impressions,
        sum(clicks)               as total_clicks,

        -- Weighted averages
        safe_divide(sum(clicks),         nullif(sum(impressions), 0))          as ctr,
        safe_divide(sum(cost_usd),        nullif(sum(clicks), 0))              as cpc,
        safe_divide(sum(cost_usd),        nullif(sum(impressions), 0)) * 1000  as cpm,
        safe_divide(sum(conversions),     nullif(sum(clicks), 0))              as cvr,
        safe_divide(sum(cost_usd),        nullif(sum(conversions), 0))         as cpa,
        safe_divide(sum(conversion_value_usd), nullif(sum(cost_usd), 0))       as roas

    from campaign_perf
    group by 1, 2, 3, 4, 5, 6
),

with_roi as (
    select
        *,
        -- ROI = (revenue - cost) / cost
        safe_divide(
            total_conversion_value - total_spend,
            nullif(total_spend, 0)
        ) as roi,

        -- Performance tiers
        case
            when roas >= 4.0 then 'high_performer'
            when roas >= 2.0 then 'mid_performer'
            when roas >= 1.0 then 'break_even'
            else 'underperformer'
        end as performance_tier

    from daily_channel
)

select * from with_roi
