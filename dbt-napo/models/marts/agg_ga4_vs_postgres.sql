{{config(materialized='table')}}


with purchase_data as(
  select created_date
  ,first_payment_charge_date
  ,quote_id
  ,payment_plan_type
  ,monthly_price
  ,annual_price
  ,policy_id
  from {{ref('dim_policy_detail')}}
  where first_payment_charge_date>= '2022-05-09'
),
analytics_data as (
SELECT _table_suffix as date
      ,event_name
      ,user_id
      ,device.web_info.hostname
      ,ecommerce.*
      ,(select value.string_value from unnest(event_params) where key='quote_id') as quote_id
      ,(select value.string_value from unnest(event_params) where key='currency') as currency
FROM `ae32-vpcservice-datawarehouse.analytics_290494422.events_*` 
where _table_suffix >= '20220509'
and event_name = 'purchase'
and device.web_info.hostname = 'www.napo.pet'
),
final as (
select a.*
      ,b.* except (quote_id)
from purchase_data a
full outer join analytics_data b
on a.quote_id = b.quote_id
)
select 
    format_date('%Y',date_trunc(parse_date('%Y%m%d',date),YEAR)) as analytics_year
    ,format_date('%m',date_trunc(parse_date('%Y%m%d',date),MONTH)) as analytics_month
    ,count(quote_id) as policies
    , count(event_name) as analytics_policies 
from final
group by 1,2
order by 1,2