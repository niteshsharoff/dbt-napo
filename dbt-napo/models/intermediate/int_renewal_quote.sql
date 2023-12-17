{{ config(
    materialized="table",
    tags=['daily','growth']

)}}
select *
from {{ ref("int_quote") }}
where quote_id in (select distinct quote_id from {{ ref("stg_raw__renewal") }})
