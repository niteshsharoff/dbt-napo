{{
    config(
        materialized='table',
        partition_by={
            'field':'date',
            'data_type':'date',
            'granularity':'day'
        },
        cluster_by = ['napo_campaign_type'],
        schema='marts'
    )
}}

SELECT 
   segments_date as date
    ,case 
        when lower(campaign_name) like '%leadgen%' then 'leadgen'
        when lower(campaign_name) not like any ('%leadgen%','%standalone%') then 'growth'
        else 'other'
    end as napo_campaign_type
    ,case
    when lower(segments_ad_network_type) like '%youtube%' then true
    else false
    end as is_youtube_campaign
  ,sum(metrics_impressions) as impressions
  ,sum(metrics_clicks) as clicks 
  ,sum(metrics_conversions) as conversions
  ,safe_divide(sum(metrics_clicks),sum(metrics_impressions)) as ctr
  ,sum(metrics_cost_micros)/1000000 as cost_gbp
FROM {{ ref('stg_src_airbyte__google_ads_campaign') }}
group by 1,2,3