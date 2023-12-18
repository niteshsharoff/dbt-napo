{{
    config(
        materialized="table",
        partition_by={"field": "date", "data_type": "date", "granularity": "day"},
        tags=["daily", "growth"],
    )
}}
select
    timeperiod as date,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(spend) as cost_gbp,
    sum(conversions) as conversions,
    max(currencycode) as currency,
    sum(revenue) as conversion_value
from {{ ref("stg_src_airbyte__bing_account_performance_report_daily") }}
where accountnumber = '{{var(' bing_account_id ')}}'
group by 1
