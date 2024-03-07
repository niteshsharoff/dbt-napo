{{ config(materialized="view") }}

with
    stg_quote as (
        select * except (customer), customer.*
        from {{ ref("stg_raw__quote_request", v=1) }}
    )
select *
from stg_quote
