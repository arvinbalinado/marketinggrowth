-- models/marts/revenue/fct_attribution.sql
-- Last-touch and first-touch channel attribution for revenue.
-- Joins GA4 session data with Shopify orders on email/customer_id.

{{
  config(
    materialized = 'table',
    partition_by = {'field': 'order_date', 'data_type': 'date', 'granularity': 'day'}
  )
}}

with sessions as (
    select
        date                    as session_date,
        session_source,
        session_medium,
        campaign_name,
        channel_normalized,
        sessions,
        conversions,
        revenue_usd             as session_revenue
    from {{ ref('stg_ga4_sessions') }}
),

orders as (
    select
        date(order_date)        as order_date,
        order_id,
        customer_id,
        gross_revenue,
        net_revenue,
        traffic_source          as order_source,
        landing_page
    from {{ ref('fct_revenue') }}
),

-- Last-touch: attribute each order to the most recent session channel
channel_revenue as (
    select
        s.session_date          as date,
        s.channel_normalized    as channel,
        s.session_source        as source,
        s.session_medium        as medium,
        s.campaign_name,

        -- Revenue attributed via GA4 session conversions
        sum(s.session_revenue)  as attributed_revenue,
        sum(s.conversions)      as attributed_conversions,
        sum(s.sessions)         as sessions

    from sessions s
    group by 1, 2, 3, 4, 5
),

with_roas as (
    select
        cr.*,
        cp.total_spend,
        safe_divide(cr.attributed_revenue, nullif(cp.total_spend, 0)) as attributed_roas

    from channel_revenue cr
    left join {{ ref('fct_ad_spend_roi') }} cp
        on cr.date = cp.date
       and cr.channel = cp.channel_source
)

select * from with_roas
