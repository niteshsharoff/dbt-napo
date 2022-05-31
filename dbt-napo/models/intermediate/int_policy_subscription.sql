with grouped_policy_data as (
select 
    s.*
    ,row_number() over(partition by policy order by created_date desc) row_no
from  {{ref('raw_subscription')}} s
),
subscription_table as ( --latest subscription table
    select * except (row_no)
    from grouped_policy_data 
    where row_no = 1
),
active_policy_existed as (
  select policy
    ,countif(active=true) as active_policy_existed  
  from  {{ref('raw_subscription')}}
  group by policy
),
grouped_data as (
    select
         p.pk
        ,p.quote_id
        ,p.reference_number
        ,coalesce(cast(s.active as string),'not set') as subscription_active
        ,coalesce(if(c.active_policy_existed=1,true,false),false) as active_policy_existed
        ,p.annual_payment_id
        ,p.created_date as created_date
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
    left join active_policy_existed c
    on c.policy = p.pk
)
select * from grouped_data