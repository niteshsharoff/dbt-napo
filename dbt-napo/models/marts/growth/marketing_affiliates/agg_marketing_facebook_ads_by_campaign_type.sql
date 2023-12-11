{{config(
    materialized='table',
    partition_by={
        'field':'date',
        'data_type':'date',
        'granularity':'day'
    },
    schema='marts'
)}}

-- In documentation, make it clear that only the purchase_insurance, Lead and view_quote are considered

with data as (
select
       date_start as date
    ,case 
        when lower(campaign_name) like '%leadgen%' then 'leadgen'
        when lower(campaign_name) not like any ('%leadgen%','%standalone%') then 'growth'
        else 'other'
    end as napo_campaign_type
      ,sum(reach) as reach --TO DO resolve this to match Asights report
      ,sum(impressions) as impressions
      ,sum(clicks) as clicks
      --TO DO --,sum(cast((select json_extract_scalar(c,'$.value') from unnest(json_extract_array(outbound_clicks,'$')) c where json_extract_scalar(c,'$.action_type')='outbound_click') as numeric)) as outbound_clicks
      ,sum(unique_clicks) as unique_clicks
      ,sum(spend) as cost_gbp
      ,max(account_currency) as currency
      ,sum(cast((select json_extract_scalar(c,'$.value') from unnest(json_extract_array(conversions,'$')) c where json_extract_scalar(c,'$.action_type')='offsite_conversion.fb_pixel_custom.purchase_insurance') as numeric)) as conversions
      ,sum(cast((select json_extract_scalar(c,'$.value') from unnest(json_extract_array(conversion_values,'$')) c where json_extract_scalar(c,'$.action_type')='offsite_conversion.fb_pixel_custom.purchase_insurance') as numeric)) as conversion_value
      ,sum(cast((select json_extract_scalar(c,'$.value') from unnest(json_extract_array(conversions,'$')) c where json_extract_scalar(c,'$.action_type')='offsite_conversion.fb_pixel_custom.start_quote') as numeric)) as start_quote_conversions
      ,sum(cast((select json_extract_scalar(c,'$.value') from unnest(json_extract_array(conversions,'$')) c where json_extract_scalar(c,'$.action_type')='offsite_conversion.fb_pixel_custom.view_quote') as numeric)) as view_quote_conversions
      ,sum(cast((select json_extract_scalar(c,'$.value') from unnest(json_extract_array(conversions,'$')) c where json_extract_scalar(c,'$.action_type')='offsite_conversion.fb_pixel_custom.purchase_insurance') as numeric)) as purchase_insurance_conversions 
      ,sum(cast((select json_extract_scalar(c,'$.value') from unnest(json_extract_array(conversion_values,'$')) c where json_extract_scalar(c,'$.action_type')='offsite_conversion.fb_pixel_custom.purchase_insurance') as numeric)) as purchase_insurance_value
      ,sum(cast((select json_extract_scalar(c,'$.value') from unnest(json_extract_array(actions,'$')) c where json_extract_scalar(c,'$.action_type')='lead') as numeric)) as lead_actions
from {{ref('stg_src_airbyte__facebook_ads_insights')}}
group by 1,2
)
select * 
from data
