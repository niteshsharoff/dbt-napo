with payments as (
select *
from {{ref('stg_policy_payments')}}
),
subscriptions as (
  select distinct 
         subscription_id
        ,policy_id 
from {{ref('stg_subscription')}}
),
payments_monthly as (
select a.*
      ,b.policy_id
from payments a
left join subscriptions b
using (subscription_id)
),
payments_monthly_annual as (
select a.* except(policy_id)
      ,coalesce(a.policy_id,b.policy_id) as policy_id
from {{ref('stg_policy')}} b
right join payments_monthly a
on b.annual_payment_id = a.payment_id
)
select * 
from payments_monthly_annual
--where policy_id is null

