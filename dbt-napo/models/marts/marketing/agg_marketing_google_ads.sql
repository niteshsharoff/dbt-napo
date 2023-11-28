{{
    config(
        materialized='table',
        partition_by={
            'field':'date',
            'data_type':'date',
            'granularity':'day'
        },
        cluster_by = ['weekday','month','week']
    )
}}

SELECT 
   segments_date as date
  ,segments_day_of_week as weekday
  ,segments_month as month
  ,segments_week as week
  ,sum(metrics_impressions) as impressions
  ,sum(metrics_clicks) as clicks 
  ,sum(metrics_conversions) as conversions
  ,safe_divide(sum(metrics_clicks),sum(metrics_impressions)) as ctr
  ,sum(metrics_cost_micros)/1000000 as cost
FROM {{ ref('stg_src_google_ads__ads_AdGroupStats_4788955894') }}
group by 1,2,3,4
