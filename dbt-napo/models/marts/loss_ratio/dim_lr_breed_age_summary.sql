{{config(materialized='table')}}
select *
from {{source('loss_ratio','breed_age_summary')}}