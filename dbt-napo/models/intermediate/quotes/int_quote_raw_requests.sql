{{ config(materialized="table", tags=["daily", "growth"]) }}

with
    quote_requests as (
        select *
        from {{ ref("int_quote_ctm_raw_requests") }}
        union distinct
        select *
        from {{ ref("int_quote_msm_raw_requests") }}
        union distinct
        select *
        from {{ ref("int_quote_gocompare_raw_requests") }}
        union distinct
        select *
        from {{ ref("int_quote_quotezone_raw_requests") }}
        union distinct
        select *
        from {{ ref("int_quote_confused_raw_requests") }}
        union distinct
        select *
        from {{ ref("int_quote_direct_raw_requests") }}
    )
select *
from quote_requests
