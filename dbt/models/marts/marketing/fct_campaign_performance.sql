-- models/marts/marketing/fct_campaign_performance.sql
-- Unified campaign performance fact table combining all paid channels.
-- Partitioned by date; clustered by channel_source, campaign_id.

{{
  config(
    materialized = 'table',
    partition_by = {
      'field': 'date',
      'data_type': 'date',
      'granularity': 'day'
    },
    cluster_by = ['channel_source', 'campaign_id']
  )
}}

with google_ads as (
    select
        date,
        campaign_id,
        campaign_name,
        ad_group_id,
        ad_group_name,
        channel_type              as campaign_type,
        device,
        null                      as objective,
        impressions,
        clicks,
        cost_usd,
        conversions,
        conversion_value_usd,
        video_views,
        ctr,
        cpc,
        cpm,
        cvr,
        cpa,
        roas,
        channel_source
    from {{ ref('stg_google_ads') }}
),

meta_ads as (
    select
        date,
        campaign_id,
        campaign_name,
        adset_id                  as ad_group_id,
        ad_set_name               as ad_group_name,
        null                      as campaign_type,
        null                      as device,
        objective,
        impressions,
        clicks,
        cost_usd,
        purchases                 as conversions,
        null                      as conversion_value_usd,
        null                      as video_views,
        ctr,
        cpc,
        cpm,
        null                      as cvr,
        null                      as cpa,
        null                      as roas,
        channel_source
    from {{ ref('stg_meta_ads') }}
),

unioned as (
    select * from google_ads
    union all
    select * from meta_ads
),

with_week_month as (
    select
        *,
        date_trunc(date, week)  as week_start,
        date_trunc(date, month) as month_start,
        extract(year from date) as year,
        extract(month from date) as month,
        extract(week from date)  as week_of_year
    from unioned
)

select * from with_week_month
