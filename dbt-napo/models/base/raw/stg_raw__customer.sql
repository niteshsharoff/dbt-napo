with raw as (
select 
    version_id
    ,timestamp_millis(created_date) as created_date
    ,cast(timestamp_millis(date_of_birth) as date) as date_of_birth
    ,phone_number
    ,street_address
    ,address_locality
    ,address_region
    ,postal_code
    ,mandate_id
    ,user_id
    ,mandate_inactive_event_id
    ,firebase_user_id
    ,uuid
    ,referral_code
    ,referred
    ,is_vet
    ,customer_id
    ,timestamp_millis(change_at) as change_at
    ,change_reason
    ,change_user_id
    ,timestamp_millis(effective_at) as effective_at
    ,cast(run_date as date) as run_date
    ,row_number() over(partition by customer_id order by version_id desc) as row_no 
from {{source('raw','customer')}}
)
select * except (row_no)
    ,extract(YEAR from date_of_birth) as year_of_birth
from raw
where row_no = 1



