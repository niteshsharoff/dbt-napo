{{config(
    materialized='incremental',
    partition_by={
        'field':'event_date',
        'granularity':'day',
        'data_type':'date'
    },
    cluster_by=['event_name','user_id','ga_session_id','transaction_id'],
    pre_hook=["""
        DECLARE table_exists BOOLEAN DEFAULT (SELECT COUNT(*) > 0 FROM `{{ target.project }}.{{ target.dataset }}.INFORMATION_SCHEMA.TABLES` WHERE table_name = '{{ this.table }}' );
        IF table_exists THEN EXECUTE IMMEDIATE 'DELETE FROM {{ this }} WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)'; END IF;"""
    ]
)}}

with base_ga4 as (
    select 
         parse_date('%Y%m%d',event_date) as event_date
        ,timestamp_micros(event_timestamp) as event_timestamp
        ,coalesce(max(user_id) over(partition by user_pseudo_id order by event_timestamp desc),user_pseudo_id) as user_id
    --  ,max(user_id) over(partition by user_pseudo_id order by event_timestamp desc) as user_id
        ,{{ga4_unnest('ga_session_id')}}
        ,{{ga4_unnest('ga_session_number')}}
        ,event_name
        ,device.web_info.hostname
        ,replace(split((select value.string_value from unnest(event_params) where key = 'page_location'),'?')[safe_offset(0)],'https://www.napo.pet','') as page_path
        ,split((select value.string_value from unnest(event_params) where key = 'page_location'),'?')[safe_offset(1)] as query_params_raw
        ,case
                when traffic_source.name = '(organic)' and traffic_source.medium='cpc' and traffic_source.source = 'google' then struct(
                'Paid Search' as name
                , traffic_source.medium as medium
                , traffic_source.source as source
                )
                else traffic_source 
        end as traffic_source
        ,collected_traffic_source
        ,privacy_info.analytics_storage
        ,privacy_info.ads_storage
        ,{{ga4_unnest('quote_id')}}
        ,{{ga4_unnest('policy_ids')}}
        ,{{ga4_unnest('transaction_type')}}
        ,{{ga4_unnest('price_monthly','policy_price_monthly')}}
        ,{{ga4_unnest('price_annual','policy_price_annual')}}
        ,{{ga4_unnest('currency')}}
        ,{{ga4_unnest('page_referrer')}}
        ,{{ga4_unnest('campaign','campaign')}}
    --  ,(select value.string_value from unnest(event_params) where key = 'page_location') as page_location
        ,coalesce(ecommerce.transaction_id,(select coalesce(value.string_value,cast(value.int_value as string)) from unnest(event_params) where key = 'transaction_id')) as transaction_id
    
    from {{source('ga4','events')}}
    {%-if is_incremental()%}
    where _table_suffix >= format_date('%Y%m%d',date_sub(current_date(),INTERVAL 3 DAY))
    {%-else%}
        {%if (target.dataset == 'dbt' or target.dataset =='dbt_marts')%}
            where _table_suffix >= '20230101'
        {%else%}
            where _table_suffix >= format_date('%Y%m%d',date_sub(current_date(),interval 15 DAY)) 
        {%endif%}
    {%-endif%}
    and user_pseudo_id is not null
),

features as (
select * except(campaign)
      ,first_value(if(event_name='page_view',page_path,null) ignore nulls) over(partition by user_id, ga_session_id order by event_timestamp) as landing_page_path
      ,max(if(lower(query_params_raw) like '%ttclid%',true,null)) over(partition by user_id,ga_session_id)as is_tiktok
      ,max(if(lower(query_params_raw) like '%fbclid%',true,null)) over(partition by user_id,ga_session_id) as is_facebook
      ,max(if(lower(query_params_raw) like any ('%gclid%','%wbraid%','gbraid'),true,null)) over(partition by user_id,ga_session_id) is_gads
      ,max(if(lower(query_params_raw) like '%msclkid%',true,null)) over(partition by user_id,ga_session_id) as is_bing
      ,max(if(lower(page_path) like '%inbound/%',true,null)) over(partition by user_id,ga_session_id) as is_pcw
      ,max(REGEXP_EXTRACT(page_path, r'inbound/([^/]+).*')) over(partition by user_id,ga_session_id) AS pcw_raw
      ,row_number() over(partition by user_id, ga_session_id order by event_timestamp asc) as event_no
      ,first_value(campaign ignore nulls) over(partition by user_id,ga_session_id order by event_timestamp asc) as campaign
from base_ga4
)
select 
     a.event_no
    ,a.event_date
    ,a.event_timestamp
    ,a.user_id
    ,a.ga_session_id
    ,a.ga_session_number
    ,a.event_name
    ,a.hostname
    ,a.page_path
    ,a.query_params_raw
    ,a.landing_page_path
    ,a.analytics_storage
    ,a.quote_id
    ,a.policy_ids
    ,a.transaction_id
    ,a.transaction_type
    ,a.currency
    ,a.policy_price_monthly
    ,a.policy_price_annual
    ,a.is_tiktok
    ,a.is_facebook
    ,a.is_gads
    ,a.is_bing
    ,a.is_pcw
    ,a.page_referrer
    ,lower(b.pcw_name) as pcw_name
    ,a.traffic_source
    ,a.collected_traffic_source
    ,a.campaign
    ,{{ga4_default_channel_grouping('a.traffic_source.source','a.traffic_source.medium','c.source_category','a.campaign')}} as default_channel_grouping

from features a
left join {{ref("lookup_quote_pcw_mapping")}} b
on a.pcw_raw = b.quote_code_name
left join {{ref('lookup_ga4_source_categories')}} c
on a.traffic_source.source = c.source
--order by user_id, event_timestamp asc