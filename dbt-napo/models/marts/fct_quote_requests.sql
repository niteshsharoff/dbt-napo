{{config(materialized='view')}}

with stg_quote as (
  select * except(customer)
         ,customer.* 
  from {{ref('stg_raw__quote_request')}}
)
select * 
from stg_quote