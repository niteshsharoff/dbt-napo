with purchase_data as(
  select created_date
  ,quote_id
  ,payment_plan_type
  ,monthly_price
  ,annual_price
  ,policy_id
  from {{ref('dim_policy_detail')}}
  where (annual_payment_id is not null or is_subscription_active is not null)
  and cast(created_date as date) >= date('2022-05-09') and cast(created_date as date)<date_sub(current_date(), INTERVAL 1 day)
),
analytics_data as (
select * 
from (
  SELECT _table_suffix as date
        ,event_name
        ,user_id
        ,device.web_info.hostname
        ,ecommerce.*
        ,(select value.string_value from unnest(event_params) where key='quote_id') as quote_id
        ,(select value.string_value from unnest(event_params) where key='currency') as currency
        ,row_number() over(partition by (select value.string_value from unnest(event_params) where key='quote_id') order by _table_suffix asc) as row_no
  FROM {{source('ga4','events')}}
  where _table_suffix >= '20220509' and _table_suffix < format_date('%Y%m%d',current_date())
  and event_name = 'purchase'
  and device.web_info.hostname = 'www.napo.pet'
)
where row_no=1
),
final as (
select a.*
      ,b.* except (quote_id)
from purchase_data a
full outer join analytics_data b
on a.quote_id = b.quote_id
),

agg as (
select 
    'policies' as source
    ,count(quote_id) as policies
from final
    union all
select
    'analyitcs' as source
    ,count(event_name) as policies 
from final
)
select
    format_date('%Y',created_date) as year
    ,format_date('%m',created_date) as month
    ,count(policy_id) as backend_policies
    ,count(event_name) as analytics_policies
    ,round(
        safe_divide((count(event_name)-count(policy_id))*100
        ,count(policy_id)),2) as perc_diff
from final
group by 1,2
order by 1,2

