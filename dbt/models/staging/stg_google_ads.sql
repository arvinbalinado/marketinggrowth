-- models/staging/stg_google_ads.sql
-- Cleans and standardises raw Google Ads campaign data.
-- One row per campaign/ad_group/date/device combination.

with source as (
    select * from {{ source('raw', 'google_ads_campaigns') }}
),

renamed as (
    select
        -- Keys
        cast(campaign_id   as string) as campaign_id,
        cast(ad_group_id   as string) as ad_group_id,
        cast(date          as date)   as date,
        lower(device)                 as device,

        -- Dimensions
        campaign_name,
        lower(campaign_status)        as campaign_status,
        lower(channel_type)           as channel_type,
        ad_group_name,
        lower(ad_group_status)        as ad_group_status,

        -- Metrics (nullsafe casts)
        coalesce(cast(impressions as int64), 0)          as impressions,
        coalesce(cast(clicks as int64), 0)               as clicks,
        coalesce(cast(cost as float64), 0.0)             as cost_usd,
        coalesce(cast(conversions as float64), 0.0)      as conversions,
        coalesce(cast(conversion_value as float64), 0.0) as conversion_value_usd,
        coalesce(cast(video_views as int64), 0)          as video_views,

        -- Derived KPIs (safe division)
        safe_divide(cast(clicks as float64), nullif(cast(impressions as float64), 0))        as ctr,
        safe_divide(cast(cost as float64), nullif(cast(clicks as float64), 0))               as cpc,
        safe_divide(cast(cost as float64), nullif(cast(impressions as float64), 0)) * 1000   as cpm,
        safe_divide(cast(conversions as float64), nullif(cast(clicks as float64), 0))        as cvr,
        safe_divide(cast(cost as float64), nullif(cast(conversions as float64), 0))          as cpa,
        safe_divide(cast(conversion_value as float64), nullif(cast(cost as float64), 0))     as roas,

        -- Metadata
        _extracted_at,
        'google_ads' as channel_source

    from source
    where date is not null
      and campaign_id is not null
)

select * from renamed
