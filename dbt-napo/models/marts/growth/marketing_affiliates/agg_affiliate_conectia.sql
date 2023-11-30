select cast(timestamp_seconds(cast(date_dt as int64)) as date) as date
      ,sum(clicks) as clicks
      ,sum(impressions) as impressions
      ,sum(conversions) as conversions
      ,sum(revenue) as cost_gbp
      ,avg(epc) as cpc
      ,avg(conversion_rate) as conv_rate
from {{ref('stg_src_airbyte__conectia_daily_summary')}}
group by 1
order by 1 desc