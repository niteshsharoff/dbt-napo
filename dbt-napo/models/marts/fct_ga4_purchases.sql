SELECT _table_suffix as date
      ,event_name
      ,user_id
      ,device.web_info.hostname
      ,ecommerce.*
      ,traffic_source.*
      ,(select value.string_value from unnest(event_params) where key='quote_id') as quote_id
      ,(select value.string_value from unnest(event_params) where key='currency') as currency
      ,(select value.string_value from unnest(event_params) where key='source') as params_source
      ,(select value.string_value from unnest(event_params) where key='medium') as params_medium
      ,(select value.string_value from unnest(event_params) where key='referrer') as params_referrer
FROM {{source('ga4','events')}}
where _table_suffix >= '20220509'
and event_name = 'purchase'
and device.web_info.hostname = 'www.napo.pet'