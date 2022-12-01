with grouped_policy_data as (
select 
    s.*
    ,row_number() over(partition by policy_id order by created_date desc) row_no
from  {{ref('stg_postgres__subscription')}} s
),
subscription_table as ( --latest subscription table
    select * except (row_no)
    from grouped_policy_data 
    where row_no = 1
),
active_policy_existed as (
  select policy_id
    ,countif(active=true) as active_subscription_existed  
  from  {{ref('stg_postgres__subscription')}}
  group by policy_id
),
grouped_data as (
    select
         p.pk
        ,p.policy_id
        ,p.quote_id
        ,p.reference_number
        ,s.active as is_subscription_active
--        ,coalesce(cast(s.active as string),'not set') as subscription_active
--        ,coalesce(if(c.active_subscription_existed=1,true,false),false) as active_subscription_existed
        ,p.active_subscription_setup
        ,p.annual_payment_id
        ,p.effective_at as policy_effective_date
        ,p.created_date as created_date
        ,s.created_date as last_subscription_created_date
        ,s.modified_date as last_subscription_modified_date
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
    FROM {{ref('stg_postgres__policy')}} p
    left join subscription_table s --only has last subscription
    on p.policy_id = s.policy_id
    left join active_policy_existed c
    on c.policy_id = p.policy_id
),
policy_data_with_payment_data as (
select a.*
      ,first_value(b.charge_date) over(partition by policy_id order by charge_date asc rows between unbounded preceding and unbounded following) as first_payment_charge_date
      ,last_value(b.charge_date) over(partition by policy_id order by charge_date asc rows between unbounded preceding and unbounded following) as last_payment_charge_date
from grouped_data a
left join {{ref('int_payments')}} b
using (policy_id)
--where b.status !='failed'
)
select distinct * 
from policy_data_with_payment_data
--where is_subscription_active is not null
