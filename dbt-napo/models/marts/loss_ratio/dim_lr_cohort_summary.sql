{{config(materialized='table')}}
select *
from {{source('loss_ratio','cohort_summary')}}