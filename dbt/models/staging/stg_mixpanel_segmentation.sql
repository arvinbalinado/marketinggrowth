-- models/staging/stg_mixpanel_segmentation.sql
-- Cleans aggregated Mixpanel segmentation data.
-- One row per event + segment_value + date.

with source as (
    select * from {{ source('raw', 'mixpanel_segmentation') }}
),

renamed as (
    select
        cast(date as date)                      as date,
        event                                   as event_name,
        segment_property,
        lower(nullif(segment_value, ''))        as segment_value,
        coalesce(cast(event_count as int64), 0) as event_count,

        -- Derived channel for UTM segment properties
        case
            when segment_property = 'utm_source' then
                case
                    when lower(segment_value) in ('google', 'bing')   then 'paid_search'
                    when lower(segment_value) in ('facebook', 'instagram',
                                                  'linkedin', 'twitter',
                                                  'tiktok', 'meta')   then 'paid_social'
                    when lower(segment_value) = 'email'               then 'email'
                    else lower(segment_value)
                end
            else null
        end as channel_normalized,

        _extracted_at

    from source
    where date is not null
      and event is not null
)

select * from renamed
