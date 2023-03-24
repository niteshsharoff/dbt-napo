with policies as (
    select p.* except (pk)
         , b.* except (pk)
         , b.pk as breed_pk
    from {{ref('int_policy_customer')}} p
    left join {{ref('int_pet_breed')}} b
    on p.pet = b.pet_id
),
features as (
select *
       ,DATE_DIFF(current_date(),pet_date_of_birth,MONTH) as pet_current_age_months
       ,DATE_DIFF(policy_start_date,cast(created_date as date),DAY) as days_policy_start
       ,if(DATE_DIFF(policy_end_date,current_date(),DAY)<0,true,false) as is_policy_expired 
       

from policies
)
select *
from features