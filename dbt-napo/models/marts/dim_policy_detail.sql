{{
    config(
      re_data_monitored=true)}}

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
from policies
)
select * from features