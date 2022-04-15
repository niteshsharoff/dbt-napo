{{
    config(
      re_data_monitored=false)}}

with policies as (
    select p.*
         , b.* except (pk)
         , b.pk as breed_pk
    from {{ref('int_policy_customer')}} p
    left join {{ref('int_pet_breed')}} b
    on p.pet = b.pk
)
select * 
from policies