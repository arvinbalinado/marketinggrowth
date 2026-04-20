-- models/marts/marketing/fct_mixpanel_campaign_attribution.sql
-- Joins Mixpanel UTM event data with paid campaign spend to produce a
-- unified, event-level attribution view across all paid channels.
-- Designed to sit alongside fct_attribution (GA4-based) for cross-validation.

{{
  config(
    materialized = 'table',
    partition_by = {'field': 'event_date', 'data_type': 'date', 'granularity': 'day'}
  )
}}

with mixpanel_events as (
    select
        event_date,
        week_start,
        month_start,
        utm_campaign,
        utm_source,
        utm_medium,
        channel_normalized,
        event_name,
        country_code,
        device_type,
        event_count,
        unique_users
    from {{ ref('fct_mixpanel_events') }}
    where utm_campaign is not null
),

-- Key conversion events to surface in attribution
conversions as (
    select
        event_date,
        utm_campaign,
        utm_source,
        utm_medium,
        channel_normalized,
        sum(case when event_name = 'Order Completed'       then event_count else 0 end) as orders,
        sum(case when event_name = 'Lead Form Submitted'   then event_count else 0 end) as leads,
        sum(case when event_name = 'Demo Requested'        then event_count else 0 end) as demo_requests,
        sum(case when event_name = 'Signed Up'             then event_count else 0 end) as signups,
        sum(case when event_name = 'Checkout Started'      then event_count else 0 end) as checkout_starts,
        sum(case when event_name = 'Add to Cart'           then event_count else 0 end) as add_to_carts,
        sum(case when event_name = 'Page Viewed'           then event_count else 0 end) as page_views,
        sum(unique_users)                                                                as total_unique_users,
        sum(event_count)                                                                 as total_events
    from mixpanel_events
    group by 1, 2, 3, 4, 5
),

-- Join to ad spend for ROAS / CPA calculations
with_spend as (
    select
        c.*,
        s.total_spend,

        -- Mixpanel-attributed CPA
        safe_divide(s.total_spend, nullif(c.orders, 0))      as cpa_orders,
        safe_divide(s.total_spend, nullif(c.leads, 0))       as cpa_leads,
        safe_divide(s.total_spend, nullif(c.signups, 0))     as cpa_signups,

        -- Funnel conversion rates from Mixpanel events
        safe_divide(c.checkout_starts, nullif(c.add_to_carts, 0)) as cart_to_checkout_rate,
        safe_divide(c.orders,          nullif(c.checkout_starts, 0)) as checkout_to_order_rate,
        safe_divide(c.orders,          nullif(c.page_views, 0)) as page_to_order_rate

    from conversions c
    left join {{ ref('fct_ad_spend_roi') }} s
        on c.event_date   = s.date
       and c.utm_campaign = s.campaign_name
       and c.channel_normalized = s.channel_source
)

select * from with_spend
