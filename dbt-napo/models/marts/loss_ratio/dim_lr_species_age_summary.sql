{{config(materialized='table')}}
select *
from {{source('loss_ratio','species_age_summary')}}