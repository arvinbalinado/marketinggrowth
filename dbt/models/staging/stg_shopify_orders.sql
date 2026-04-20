-- models/staging/stg_shopify_orders.sql
-- Cleans and enriches Shopify order records for revenue analysis.

with source as (
    select *
    from {{ source('raw', 'shopify_raw') }}
    where object_type = 'order'
),

renamed as (
    select
        cast(order_id as string)         as order_id,
        cast(created_at as timestamp)    as order_date,
        cast(updated_at as timestamp)    as updated_at,
        lower(email)                     as customer_email,
        cast(customer_id as string)      as customer_id,
        lower(financial_status)          as financial_status,
        lower(fulfillment_status)        as fulfillment_status,
        cast(total_price as float64)     as gross_revenue,
        cast(subtotal_price as float64)  as subtotal_revenue,
        cast(total_tax as float64)       as tax_amount,
        cast(total_discounts as float64) as discount_amount,
        cast(total_price as float64)
            - cast(total_tax as float64)        as net_revenue,
        currency,
        item_count,
        discount_codes,
        source_name,
        lower(referring_site)            as referring_site,
        lower(landing_site)              as landing_page,
        cancelled_at is not null         as is_cancelled,
        financial_status = 'paid'        as is_paid,

        -- Revenue categories
        case
            when cast(total_discounts as float64) > 0 then 'discounted'
            else 'full_price'
        end as order_type,

        _extracted_at

    from source
    where order_id is not null
      and financial_status != 'refunded'
)

select * from renamed
