{{config(materialized='table')}}

with stg_quote as (
  select *
  from {{ref('stg_raw__quote_request')}}
)
select * 
from stg_quote