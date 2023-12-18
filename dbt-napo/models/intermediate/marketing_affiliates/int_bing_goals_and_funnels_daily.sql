{{config(
    tags=['daily','growth']
)}}

with raw as (
select
    cast(TimePeriod as date) as TimePeriod
    ,CampaignId
    ,CampaignName
    ,Goal
   ,AllConversionsQualified
   ,AllConversions
   ,AllRevenue
   ,row_number() over(partition by concat(TimePeriod,CampaignId,coalesce(Goal,'-'))) as row_no
from {{ref('stg_src_airbyte__bing_goals_and_funnels_report_request')}}
qualify row_no = 1
)
select 
    TimePeriod as date
    ,CampaignId
    ,sum(
      case
        when Goal='generate_lead' then cast(AllConversions as numeric)
        else null
      end) as lead_conversions
    ,sum(
      case
        when Goal='view_quote' then cast(AllConversions as numeric)
        else null
      end) as view_quote_conversions
    ,sum(
      case
        when Goal='purchase' then cast(AllConversions as numeric)
        else null
      end) as purchase_conversions
    ,sum(
      case
        when Goal='generate_lead' then cast(AllConversionsQualified as numeric)
        else null
      end) as lead_conversions_qualified
    ,sum(
      case
        when Goal='view_quote' then cast(AllConversionsQualified as numeric)
        else null
      end) as view_quote_conversions_qualified
    ,sum(
      case
        when Goal='purchase' then cast(AllConversionsQualified as numeric)
        else null
      end) as purchase_conversions_qualified
    ,sum(
      case
        when Goal='purchase' then cast(AllRevenue as numeric)
        else null
      end) as purchase_conversion_revenue

from raw
group by 1,2
