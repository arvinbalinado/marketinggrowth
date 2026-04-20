-- models/marts/growth/fct_funnel_conversion.sql
-- Marketing funnel conversion rates by stage, channel, and date.
-- Supports funnel visualisations and MQL→SQL reporting.

{{
  config(
    materialized = 'table',
    partition_by = {'field': 'week_start', 'data_type': 'date', 'granularity': 'day'}
  )
}}

with contacts as (
    select
        contact_id,
        email,
        lifecycle_stage,
        lead_status,
        original_source,
        created_at,
        date(created_at)              as created_date,
        date_trunc(date(created_at), week)  as week_start,
        date_trunc(date(created_at), month) as month_start,
        is_mql,
        is_sql,
        is_opportunity,
        is_customer

    from {{ ref('stg_hubspot_contacts') }}
    where created_at is not null
),

weekly_funnel as (
    select
        week_start,
        original_source             as channel,

        -- Stage volumes
        count(distinct contact_id)  as total_leads,
        countif(is_mql)             as mqls,
        countif(is_sql)             as sqls,
        countif(is_opportunity)     as opportunities,
        countif(is_customer)        as customers,

        -- Conversion rates
        safe_divide(countif(is_mql),         count(distinct contact_id)) as lead_to_mql_rate,
        safe_divide(countif(is_sql),         countif(is_mql))            as mql_to_sql_rate,
        safe_divide(countif(is_opportunity), countif(is_sql))            as sql_to_opp_rate,
        safe_divide(countif(is_customer),    countif(is_opportunity))    as opp_to_customer_rate,

        -- Overall funnel conversion
        safe_divide(countif(is_customer),    count(distinct contact_id)) as overall_conversion_rate

    from contacts
    group by 1, 2
)

select * from weekly_funnel
