{{config(materialized='table')}}
select *
from {{source('loss_ratio','all_book_summary')}}