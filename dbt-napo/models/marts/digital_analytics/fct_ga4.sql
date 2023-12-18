{% set partitions_to_replace = ['current_date'] %}
{% for i in range(3) %}
    {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
{% endfor %}


{{config(
    materialized='incremental',
    partition_by={
        'field':'event_date',
        'granularity':'day',
        'data_type':'date'
    },
    require_partition_filter=true,
    cluster_by=['event_name','user_id','ga_session_id','transaction_id'],
    incremental_strategy='insert_overwrite',
    partitions=partitions_to_replace,
    schema='marts',
    tags=['daily','growth']
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
        ,{{ga4_unnest('page_title')}}
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
        ,{{ga4_unnest('batch_ordering_id','batch_sequence_id')}}
        ,event_params
        ,items
        ,device
        ,geo
    from {{source('ga4','events')}}
    {%-if is_incremental()%}
    where _table_suffix >= format_date('%Y%m%d',date_sub(current_date(),INTERVAL 3 DAY))
    {%-else%}
        {%if (target.dataset == 'dbt' or target.dataset =='dbt_marts')%}
            where _table_suffix >= '20230101'
        {%else%}
            where _table_suffix >= format_date('%Y%m%d',date_sub(current_date(),interval 50 DAY)) 
        {%endif%}
    {%-endif%}
    and user_pseudo_id is not null
),

features as (
select * except(campaign)
      ,first_value(if(event_name='page_view',page_path,null) ignore nulls) over(partition by user_id, ga_session_id order by event_timestamp) as landing_page_path
      ,max(if(lower(query_params_raw) like '%ttclid%',true,null)) over(partition by user_id,ga_session_id)as is_tiktok
      ,max(if(lower(query_params_raw) like '%fbclid%',true,null)) over(partition by user_id,ga_session_id) as is_facebook
      ,max(if(lower(query_params_raw) like any ('%gclid%','%wbraid%','%gbraid%'),true,null)) over(partition by user_id,ga_session_id) is_gads
      ,max(if(lower(query_params_raw) like '%msclkid%',true,null)) over(partition by user_id,ga_session_id) as is_bing
      ,max(if(lower(page_path) like '%inbound/%',true,null)) over(partition by user_id,ga_session_id) as is_pcw --TO DO: Should this be landing_page_path?
      ,max(REGEXP_EXTRACT(page_path, r'inbound/([^/]+).*')) over(partition by user_id,ga_session_id) AS pcw_raw
      ,row_number() over(partition by user_id, ga_session_id order by event_timestamp asc) as event_no
      ,first_value(campaign ignore nulls) over(partition by user_id,ga_session_id order by event_timestamp asc) as campaign
from base_ga4
),
joined as (
select 
     a.* except(pcw_raw)
    ,d.napo_page_category
    ,b.pcw_name
    ,{{ga4_default_channel_grouping('a.traffic_source.source','a.traffic_source.medium','c.source_category','a.campaign')}} as default_channel_grouping

from features a
left join {{ref("lookup_quote_pcw_mapping")}} b
on a.pcw_raw = b.quote_code_name
left join {{ref('lookup_ga4_source_categories')}} c
on a.traffic_source.source = c.source
left join {{ref('lookup_ga4_growth_page_category')}} d
on a.landing_page_path = d.page_path and a.hostname = d.domain
--order by user_id, event_timestamp asc
),
napo_attribution as (
select 
    * replace(lower(pcw_name) as pcw_name)
    ,case
        when is_pcw then 'pcw'
        when napo_page_category = 'brand_ambassador' then 'lead_generation'
        when traffic_source.source like any ('%benefitshub%','%perkbox%') then 'partnership'
        when lower(query_params_raw) like any ('%voucher_code=gohenry10%','%voucher_code=vodasummer23%') then 'partnership'        
        when landing_page_path in (select page_path from {{ref('lookup_ga4_growth_page_category')}} where napo_page_category='lead_generation') and coalesce(is_gads,is_facebook) then 'lead_generation'
        when coalesce(is_tiktok,is_bing,is_gads,is_facebook) then 'paid_marketing'
        when landing_page_path like '/join/%' then 'referral'
        when lower(default_channel_grouping) in ('organic search','direct',null) then 'direct'
        else 'direct'
    end as napo_channel
    ,case
        when traffic_source.source like '%benefitshub%' then 'benefitshub'
        when traffic_source.source like '%perkbox%' then 'perkbox'
        when lower(query_params_raw) like '%voucher_code=gohenry10%' then 'gohenry'
        when lower(query_params_raw) like '%voucher_code=vodasummer23%' then 'vodafone'
        when landing_page_path like '/blog%' then 'blog'                    --if the landing page of the session is blog
        when is_facebook then 'facebook'                                    --if the session is a facebook session (fbclid present in url)
        when is_tiktok then 'tiktok'                                        --if the session is a tiktok session (ttclid present in url)
        when is_bing then 'bing'                                            --if the session is a bing session (msclkid present in url)
        when is_gads then 'google'                                          --if the session is a google session (gclid,wbraid or gbraid present in url)
        when pcw_name is not null and is_pcw then lower(pcw_name) 
        when landing_page_path like '/join/%' then 'referral'          
        when napo_page_category = 'brand_ambassador' then 'brand_ambassador'--if the landing page of the session has a brand ambassador page
        when napo_page_category is not null then napo_page_category
        else 'organic'
    end as napo_subchannel
from joined
)
select 
     * except(napo_channel, napo_subchannel)
    ,first_value(napo_channel) over(partition by user_id,ga_session_id order by event_no asc) as napo_channel
    ,first_value(napo_subchannel) over(partition by user_id,ga_session_id order by event_no asc) as napo_subchannel 
from napo_attribution