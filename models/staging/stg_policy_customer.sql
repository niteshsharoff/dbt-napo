select p.*
      ,c.year_of_birth
      ,(extract(YEAR from current_date())-cast(c.year_of_birth as numeric)) as customer_age
      ,c.user as customer_id
from {{ref('stg_policy_subscription')}} p
left join {{ref('raw_customer')}} c
on p.customer = c.pk
