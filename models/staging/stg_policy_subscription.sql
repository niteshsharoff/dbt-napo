with grouped_policy_data as (
select 
    s.*
    ,row_number() over(partition by policy order by created_date desc) row_no
from  {{ref('raw_subscription')}} s
),
subscription_table as (
    select * except (row_no)
    from grouped_policy_data 
    where row_no = 1
),
grouped_data as (
    select
         p.pk
        ,p.quote_id
        ,p.reference_number
        ,coalesce(cast(s.active as string),'not set') as subscription_active
        ,p.created_date
        ,s.created_date as subscription_created_date
        ,s.modified_date as subscription_modified_date
        ,p.start_date
        ,p.end_date
        ,p.cancel_date
        ,p.cancel_reason
        ,p.payment_plan_type
        ,p.monthly_price
        ,p.annual_price
        ,p.customer
        ,p.pet
        ,p.product
        ,p.quote_source_reference
        ,p.quote_source
        ,p.voucher_code
    FROM {{ref('raw_policy')}} p
    left join subscription_table s
    on p.pk = s.policy
)
select * from grouped_data
