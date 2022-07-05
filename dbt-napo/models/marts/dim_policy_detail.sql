{{config(materialized='table')}}


with policies as (
    select p.*
         , b.* except (pk)
         , b.pk as breed_pk
    from {{ref('int_policy_customer')}} p
    left join {{ref('int_pet_breed')}} b
    on p.pet = b.pk
),
features as (
select *
       ,DATE_DIFF(policy_start_date,cast(created_date as date),DAY) as days_policy_start
       ,if(DATE_DIFF(policy_end_date,current_date(),DAY)<0,true,false) as is_policy_expired 

from policies
)
select *
from features
--where payment_plan_type = 'monthly'
--and first_charge_date_monthly is null

--Problem to fix - many don't have a payment plan?!