-- models/staging/stg_meta_ads.sql
-- Cleans and standardises raw Meta (Facebook/Instagram) Ads Insights data.

with source as (
    select * from {{ source('raw', 'meta_ads_insights') }}
),

renamed as (
    select
        -- Keys
        cast(campaign_id as string) as campaign_id,
        cast(adset_id    as string) as adset_id,
        cast(ad_id       as string) as ad_id,
        cast(date_start  as date)   as date,

        -- Dimensions
        campaign_name,
        adset_name                   as ad_set_name,
        ad_name,
        lower(objective)             as objective,
        lower(buying_type)           as buying_type,

        -- Core metrics
        coalesce(cast(impressions as int64), 0)    as impressions,
        coalesce(cast(clicks      as int64), 0)    as clicks,
        coalesce(cast(spend       as float64), 0.0) as cost_usd,
        coalesce(cast(reach       as int64), 0)    as reach,
        coalesce(cast(frequency   as float64), 0.0) as frequency,

        -- Action funnels (common action types — extend as needed)
        coalesce(cast(action_link_click         as float64), 0.0) as link_clicks,
        coalesce(cast(action_purchase           as float64), 0.0) as purchases,
        coalesce(cast(action_lead               as float64), 0.0) as leads,
        coalesce(cast(action_add_to_cart        as float64), 0.0) as add_to_cart,
        coalesce(cast(action_view_content       as float64), 0.0) as view_content,
        coalesce(cast(action_initiate_checkout  as float64), 0.0) as initiate_checkout,

        -- Derived KPIs
        safe_divide(cast(clicks as float64),    nullif(cast(impressions as float64), 0)) as ctr,
        safe_divide(cast(spend  as float64),    nullif(cast(clicks      as float64), 0)) as cpc,
        safe_divide(cast(spend  as float64),    nullif(cast(impressions as float64), 0)) * 1000 as cpm,

        -- Metadata
        _extracted_at,
        'meta_ads' as channel_source

    from source
    where date is not null
      and ad_id is not null
)

select * from renamed
