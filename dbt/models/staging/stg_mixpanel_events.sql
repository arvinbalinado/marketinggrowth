-- models/staging/stg_mixpanel_events.sql
-- Cleans and standardises raw Mixpanel event-level data.
-- One row per unique event instance (distinct_id + insert_id + time).

with source as (
    select * from {{ source('raw', 'mixpanel_events') }}
),

renamed as (
    select
        -- Identity
        coalesce(insert_id, {{ dbt_utils.generate_surrogate_key(['distinct_id', 'event', 'time']) }})
                                    as event_id,
        event                       as event_name,
        distinct_id,

        -- Timestamps
        cast(time as timestamp)     as event_timestamp,
        cast(event_date as date)    as event_date,
        extract(hour from cast(time as timestamp)) as event_hour,

        -- Geography
        upper(country_code)         as country_code,
        city,
        region                      as region_name,

        -- Device / platform
        lower(os)                   as operating_system,
        lower(browser)              as browser,
        browser_version,
        lower(device)               as device_type,
        cast(screen_width  as int64) as screen_width,
        cast(screen_height as int64) as screen_height,

        -- UTM / traffic attribution
        lower(nullif(utm_source,   '')) as utm_source,
        lower(nullif(utm_medium,   '')) as utm_medium,
        lower(nullif(utm_campaign, '')) as utm_campaign,
        lower(nullif(utm_content,  '')) as utm_content,
        lower(nullif(utm_term,     '')) as utm_term,

        -- Channel normalisation (mirrors GA4 logic)
        case
            when lower(utm_medium) in ('cpc', 'ppc', 'paidsearch') then 'paid_search'
            when lower(utm_source) in ('google', 'bing', 'yahoo')
             and lower(utm_medium) in ('cpc', 'ppc')              then 'paid_search'
            when lower(utm_medium) in ('paid_social', 'social_paid',
                                       'cpm', 'cpv')              then 'paid_social'
            when lower(utm_source) in ('facebook', 'instagram',
                                       'linkedin', 'twitter',
                                       'tiktok', 'meta')          then 'paid_social'
            when lower(utm_medium) = 'email'                      then 'email'
            when lower(utm_medium) = 'organic'                    then 'organic_search'
            when lower(utm_medium) = 'referral'                   then 'referral'
            when lower(utm_medium) = 'affiliate'                  then 'affiliate'
            when lower(utm_medium) = 'display'                    then 'display'
            when utm_source is null and utm_medium is null        then 'direct'
            else 'other'
        end as channel_normalized,

        -- Page / referrer
        current_url,
        initial_referrer,
        referrer,
        lib                         as sdk_library,

        -- Custom event properties (raw JSON — parse in mart layer)
        custom_properties,

        -- Metadata
        _extracted_at

    from source
    where event_name is not null
      and distinct_id is not null
      and time is not null
)

select * from renamed
