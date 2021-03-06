select 
       p.pk
      ,p.quote_id
      ,p.reference_number
      ,p.subscription_active
      ,p.created_date
      ,p.subscription_created_date
      ,p.subscription_modified_date
      ,p.start_date as policy_start_date
      ,p.end_date as policy_end_date
      ,p.cancel_date as policy_cancel_date
      ,p.cancel_reason as policy_cancel_reason
      ,p.payment_plan_type
      ,p.monthly_price
      ,p.annual_price
      ,p.customer
      ,p.pet
      ,p.product
      ,p.quote_source_reference
      ,p.quote_source
      ,p.voucher_code
      ,c.year_of_birth
      ,(extract(YEAR from current_date())-cast(c.year_of_birth as numeric)) as customer_age
      ,c.user
from {{ref('int_policy_subscription')}} p
left join {{ref('raw_customer')}} c
on p.customer = c.pk



