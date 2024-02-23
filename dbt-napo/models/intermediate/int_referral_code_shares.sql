{{ config(schema="marts", tags=["daily", "growth"]) }}

select
    date(timestamp_millis(created_at)) as created_date,
    count(distinct email) as unique_referrals_created
from {{ ref("stg_raw__activatedreferral") }}
group by 1
order by 1 desc
