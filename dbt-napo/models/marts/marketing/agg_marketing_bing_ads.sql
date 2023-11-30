{{
    config(
        materialized='table',
        partition_by={
            'field':'date',
            'data_type':'date',
            'granularity':'day'
        }
    )
}}
SELECT 
     timeperiod as date
    ,sum(impressions) as impressions
    ,sum(clicks) as clicks
    ,sum(spend) as cost_gbp
    ,sum(conversions) as conversions
    ,max(currencycode) as currency
    ,sum(revenue) as conversion_value
FROM {{ref('stg_src_airbyte__bing_account_performance_report_daily')}} 
where accountnumber = 'F149C4W5'
group by 1