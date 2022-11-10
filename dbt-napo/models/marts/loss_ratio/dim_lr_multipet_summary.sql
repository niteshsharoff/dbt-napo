{{config(materialized='table')}}
select *
from {{source('loss_ratio','multipet_summary')}}