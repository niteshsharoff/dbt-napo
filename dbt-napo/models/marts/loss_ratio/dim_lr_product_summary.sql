{{config(materialized='table')}}
select *
from {{source('loss_ratio','product_summary')}}