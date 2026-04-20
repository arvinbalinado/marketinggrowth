-- models/staging/stg_hubspot_contacts.sql
-- Flattens and cleans HubSpot contact records.

with source as (
    select *
    from {{ source('raw', 'hubspot_objects') }}
    where object_type = 'contact'
),

renamed as (
    select
        id                           as contact_id,
        lower(email)                 as email,
        firstname                    as first_name,
        lastname                     as last_name,
        lower(lifecyclestage)        as lifecycle_stage,
        lower(hs_lead_status)        as lead_status,
        hubspot_owner_id             as owner_id,
        lower(hs_analytics_source)   as original_source,
        hs_analytics_source_data_1   as original_source_detail,
        cast(createdate as timestamp)        as created_at,
        cast(lastmodifieddate as timestamp)  as updated_at,
        num_conversion_events,
        recent_conversion_event_name,

        -- Stage flags
        lifecycle_stage in ('marketingqualifiedlead', 'mql') as is_mql,
        lifecycle_stage in ('salesqualifiedlead', 'sql')     as is_sql,
        lifecycle_stage = 'opportunity'                       as is_opportunity,
        lifecycle_stage = 'customer'                          as is_customer,

        _extracted_at

    from source
    where id is not null
)

select * from renamed
