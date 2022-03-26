with policies as (
    select p.*, b.* except (pk)
    from {{ref('stg_policy_subscription')}} p
    left join {{ref('stg_pet_breed')}} b
    on p.pet = b.pk
)
select * 
from policies