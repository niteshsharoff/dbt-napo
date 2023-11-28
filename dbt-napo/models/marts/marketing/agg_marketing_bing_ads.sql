SELECT 
     timeperiod as date
    ,sum(impressions) as impressions
    ,sum(clicks) as clicks
    ,sum(spend) as cost
    ,sum(conversions) as conversions
    ,max(currencycode) as currency
    ,sum(revenue) as conversion_value
FROM {{ref('stg_src_airbyte__bing_account_performance_report_daily')}} 
where accountnumber = 'F149C4W5'
group by 1
order by 1 desc