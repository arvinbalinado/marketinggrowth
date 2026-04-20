-- models/staging/stg_ga4_sessions.sql
-- Cleans and standardises Google Analytics 4 session-level data.

with source as (
    select * from {{ source('raw', 'ga4_sessions') }}
),

renamed as (
    select
        cast(date as date)             as date,
        sessionSource                  as session_source,
        sessionMedium                  as session_medium,
        sessionCampaignName            as campaign_name,
        lower(deviceCategory)          as device_category,
        country,
        landingPage                    as landing_page,
        sessionDefaultChannelGroup     as default_channel_group,

        -- Traffic metrics
        coalesce(cast(sessions             as int64),   0) as sessions,
        coalesce(cast(activeUsers          as int64),   0) as active_users,
        coalesce(cast(newUsers             as int64),   0) as new_users,
        coalesce(cast(screenPageViews      as int64),   0) as page_views,
        coalesce(cast(engagedSessions      as int64),   0) as engaged_sessions,
        coalesce(cast(conversions          as int64),   0) as conversions,
        coalesce(cast(totalRevenue         as float64), 0.0) as revenue_usd,

        -- Engagement rates
        coalesce(cast(bounceRate           as float64), 0.0) as bounce_rate,
        coalesce(cast(engagementRate       as float64), 0.0) as engagement_rate,
        coalesce(cast(averageSessionDuration as float64), 0.0) as avg_session_duration_sec,

        -- Derived
        safe_divide(cast(sessions as float64), nullif(cast(active_users as float64), 0)) as sessions_per_user,
        safe_divide(cast(conversions as float64), nullif(cast(sessions as float64), 0))  as conversion_rate,

        -- Channel normalisation
        case
            when lower(sessionDefaultChannelGroup) like '%paid search%'  then 'paid_search'
            when lower(sessionDefaultChannelGroup) like '%paid social%'   then 'paid_social'
            when lower(sessionDefaultChannelGroup) like '%organic%'       then 'organic_search'
            when lower(sessionDefaultChannelGroup) like '%email%'         then 'email'
            when lower(sessionDefaultChannelGroup) like '%direct%'        then 'direct'
            when lower(sessionDefaultChannelGroup) like '%referral%'      then 'referral'
            when lower(sessionDefaultChannelGroup) like '%display%'       then 'display'
            else 'other'
        end as channel_normalized,

        _extracted_at,
        'google_analytics_4' as channel_source

    from source
    where date is not null
)

select * from renamed
