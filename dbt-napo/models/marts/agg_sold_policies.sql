{{config(materialized='table')}}

select format_date('%Y',(date_trunc(date(first_payment_charge_date),YEAR))) as purchase_year
      ,format_date('%m',date_trunc(date(first_payment_charge_date),MONTH)) as purchase_month
      ,count(policy_id) as total_policies
from  {{ref('dim_policy_detail')}}
where (annual_payment_id is not null and annual_payment_id !='')
or is_subscription_active is not null
group by 1,2
order by 1,2
