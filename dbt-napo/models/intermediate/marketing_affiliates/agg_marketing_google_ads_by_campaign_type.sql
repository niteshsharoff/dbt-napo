{{
    config(
        materialized='table',
        partition_by={
            'field':'date',
            'data_type':'date',
            'granularity':'day'
        },
        cluster_by = ['napo_campaign_type']
    )
}}

with campaign as (
SELECT 
   segments_date as date
    ,case 
        when lower(campaign_name) like '%leadgen%' then 'leadgen'
        when lower(campaign_name) not like any ('%leadgen%','%standalone%') then 'growth'
        when lower(campaign_name) like '%standalone%' then 'pa_standalone'
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
)
    select 
         a.date
        ,a.napo_campaign_type
        ,a.is_youtube_campaign
        ,sum(impressions) as impressions
        ,sum(clicks) as clicks
        ,sum(conversions) as conversions_all
        ,sum(cost_gbp) as cost_gbp
        ,sum(view_quote_conversions) as view_quote_conversions
        ,sum(lead_conversions) as lead_conversions
        ,sum(academy_registration_conversions) as academy_registration_conversions
        ,sum(purchase_conversions) as purchase_conversions
        ,sum(all_conversions_value) as all_conversions_value
    from campaign a 
    left join {{ref('int_gads_campaign_conversions')}} b
    on a.date = b.date
    and a.napo_campaign_type = b.napo_campaign_type
    and a.is_youtube_campaign = b.is_youtube_campaign
    group by 1,2,3
