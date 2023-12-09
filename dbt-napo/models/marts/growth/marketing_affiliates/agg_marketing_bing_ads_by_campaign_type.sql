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

with prep as (
select 
      TimePeriod as date
      ,case
        when lower(trim(CampaignName)) not like any ('%leadgen%','%standalone%') then 'growth'
        when  lower(trim(CampaignName)) like '%leadgen%' then 'leadgen'
        else 'other'
      end as napo_campaign_type
      ,sum(Impressions) as impressions
      ,sum(Clicks) as clicks
      ,sum(AllConversions) as all_conversions
      ,sum(Revenue) as all_conv_value
      ,sum(Spend) as cost_gbp
from {{ref('stg_src_airbyte__bing_campaign_performance_report_daily')}}
group by 1,2
)

select a.date
      ,a.napo_campaign_type
      ,sum(a.impressions) as impressions
      ,sum(a.clicks) as clicks
      ,sum(a.all_conversions) as all_conversions
      ,sum(a.all_conv_value) as all_conv_value
      ,sum(a.cost_gbp) as cost_gbp
      ,sum(b.quote_view_conversions_qualified) as view_quote_conversions
      ,sum(b.lead_conversions_qualified) as lead_conversions
      ,sum(b.purchase_conversions_qualified) as purchase_conversions
      ,sum(b.purchase_conversion_revenue) as purchase_conv_revenue
from prep a
left join {{ref('int_bing_goals_and_funnels_daily')}} b
on a.date = b.date
and a.napo_campaign_type = b.napo_campaign_type
group by 1,2