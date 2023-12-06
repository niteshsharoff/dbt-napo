{{
    config(
        materialized='table',
        partition_by={
            'field':'date',
            'data_type':'date',
            'granularity':'day'
        },
        schema='marts'
    )
}}
select 
      TimePeriod as date
      ,case
        when lower(trim(CampaignName)) not like any ('%leadgen%','%standalone%') then 'growth'
        when  lower(trim(CampaignName)) like '%leadgen%' then 'leadgen'
        else 'other'
      end as napo_campaign_type
      ,sum(Impressions) as impressions
      ,sum(Clicks) as clicks
      ,sum(AllConversions) as conversions
      ,sum(Revenue) as conv_value
      ,sum(Spend) as cost_gbp
from {{ref('stg_src_airbyte__bing_campaign_performance_report_daily')}}
group by 1,2