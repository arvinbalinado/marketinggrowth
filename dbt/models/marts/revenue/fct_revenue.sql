-- models/marts/revenue/fct_revenue.sql
-- Daily revenue summary from Shopify orders.
-- Supports revenue dashboards, MoM growth, and LTV tracking.

{{
  config(
    materialized = 'table',
    partition_by = {'field': 'order_date', 'data_type': 'date', 'granularity': 'day'}
  )
}}

with orders as (
    select * from {{ ref('stg_shopify_orders') }}
    where not is_cancelled
      and is_paid
),

daily_revenue as (
    select
        date(order_date)              as order_date,
        date_trunc(date(order_date), week)  as week_start,
        date_trunc(date(order_date), month) as month_start,
        extract(year from order_date) as year,
        currency,
        source_name                   as traffic_source,
        order_type,

        count(distinct order_id)      as order_count,
        count(distinct customer_id)   as unique_customers,
        sum(gross_revenue)            as gross_revenue,
        sum(net_revenue)              as net_revenue,
        sum(discount_amount)          as total_discounts,
        sum(tax_amount)               as total_taxes,
        sum(item_count)               as total_items,

        avg(gross_revenue)            as avg_order_value,
        avg(item_count)               as avg_items_per_order,
        safe_divide(sum(discount_amount), nullif(sum(gross_revenue), 0)) as discount_rate

    from orders
    group by 1, 2, 3, 4, 5, 6, 7
),

with_mom_growth as (
    select
        *,
        -- Month-over-month revenue growth
        lag(gross_revenue, 1) over (
            partition by extract(month from order_date)
            order by month_start
        ) as prev_month_revenue,

        safe_divide(
            gross_revenue - lag(gross_revenue, 1) over (
                partition by extract(month from order_date)
                order by month_start
            ),
            nullif(lag(gross_revenue, 1) over (
                partition by extract(month from order_date)
                order by month_start
            ), 0)
        ) as revenue_mom_growth_rate

    from daily_revenue
)

select * from with_mom_growth
