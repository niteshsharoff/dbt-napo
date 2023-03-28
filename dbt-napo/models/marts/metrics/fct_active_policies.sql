{{
    config(materialized='incremental',
        unique_key='date')
}}

select 
       date(current_date()) as date
      ,format_date('%V',current_date()) as week
      ,format_date('%A',current_date()) as weekday
      ,count(*) as active_policies
from {{ref('dim_policy_detail')}}
where is_policy_expired is false
and (is_subscription_active is not null or annual_payment_id is not null)
and policy_cancel_date is null
--and policy_start_date = '2023-03-12'