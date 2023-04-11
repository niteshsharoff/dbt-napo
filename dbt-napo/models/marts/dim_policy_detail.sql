with policies as (
    select p.* except (version_id)
         , b.* except (pk,pet_id)
         , b.pk as breed_pk
    from {{ref('int_policy_customer')}} p
    left join {{ref('int_pet_breed')}} b
    on p.pet_id = b.pet_id
),
features as (
select *
       ,DATE_DIFF(current_date(),pet_date_of_birth,MONTH) as pet_current_age_months
       ,DATE_DIFF(cast(policy_start_date as date),cast(created_date as date),DAY) as days_policy_start
       ,if(DATE_DIFF(cast(policy_end_date as date),current_date(),DAY)<0,true,false) as is_policy_expired 
       

from policies
)
select *
from features