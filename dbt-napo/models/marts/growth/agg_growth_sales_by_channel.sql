
WITH date_spine AS (
    SELECT DATE
    FROM UNNEST(GENERATE_DATE_ARRAY('2023-01-01', CURRENT_DATE())) AS DATE
),

channel_subchannels AS (
    SELECT *
    FROM UNNEST([
        STRUCT('pcw' AS channel, 'moneysupermarket' AS subchannel),
        ('pcw', 'comparethemarket'),
        ('pcw', 'gocompare'),
        ('pcw', 'confused'),
        ('paid_marketing', 'facebook'),
        ('paid_marketing', 'google'),
        ('paid_marketing', 'youtube'),
        ('paid_marketing', 'bing'),
        ('paid_marketing', 'tiktok'),
        ('paid_marketing', 'affiliate'),
        ('lead_generation', 'brand_ambassador'),
        ('lead_generation', 'facebook'),
        ('lead_generation', 'google'),
        ('lead_generation', 'bing'),
        ('direct', 'organic'),
        ('direct', 'blog'),
     --   ('direct','direct'),
        ('direct', 'commercial_page'),
        ('referral', 'referral'),
        ('partnership', 'benfitshub'),
        ('partnership', 'perkbox'),
        ('partnership', 'vodafone'),
        ('partnership', 'gohenry'),
        ('training_product', 'training_product'),
        ('pa_standalone','google'),
        ('pa_standalone','facebook'),
        ('pa_standalone','bing')
    ])
),

core as (
  SELECT a.date
      , b.channel
      , b.subchannel
  FROM date_spine a
  CROSS JOIN channel_subchannels b
),

core__quote_response_volume as (
    select 
    a.*
    ,b.total_offered_quotes as quote_response_volume
    from core a
    left join {{ref('agg_quote_requests')}} b
    on a.date = b.created_date
    and case 
        when b.quote_source !='direct' then a.channel='pcw'
        else a.channel ='direct'
    end 
    and a.subchannel = case
                        when b.quote_source = 'direct' then 'organic'
                        else b.quote_source
                       end
),

int_marketing_by_campaign as (
  --Facebook Ads
  select 
  date
  ,case 
    when napo_campaign_type='leadgen' then 'lead_generation'
    when napo_campaign_type='growth' then 'paid_marketing'
    else 'pa_standalone'
   end as channel
  ,'facebook' as subchannel
  ,sum(cost_gbp) as total_spend
  ,sum(conversions) as conversions
  ,sum(clicks) as clicks
  ,sum(view_quote_conversions) as view_quote_conversions
  ,sum(lead_actions) as lead_conversions
  ,sum(purchase_insurance_conversions) as purchase_conversions
  ,sum(purchase_insurance_value) as purchase_conv_value
  from {{ref('agg_marketing_facebook_ads_by_campaign_type')}}
  group by 1,2,3
  union all 
  --Google Ads
  select 
      date
      ,case 
      when napo_campaign_type='growth' then 'paid_marketing'
      when napo_campaign_type='leadgen' then 'lead_generation'
      else 'pa_standalone'
      end as channel
      ,case
      when is_youtube_campaign is true then 'youtube'
      else 'google'
      end as sub_channel
      ,sum(cost_gbp) as total_spend
      ,sum(conversions_all) as conversions
      ,sum(clicks) as clicks
      ,sum(view_quote_conversions) as view_quote_conversions
      ,sum(lead_conversions) as lead_conversions
      ,sum(purchase_conversions) as purchase_conversions
      ,sum(all_conversions_value) as purchase_conv_value
  from {{ref('agg_marketing_google_ads_by_campaign_type')}}
  group by 1,2,3
  union all
  --Bing Ads
  select 
    date
    ,case 
      when napo_campaign_type='growth' then 'paid_marketing'
      when napo_campaign_type='leadgen' then 'lead_generation'
      else 'pa_standalone'
    end as channel
    ,'bing' as subchannel
    ,sum(cost_gbp) as total_spend
    ,sum(all_conversions) as conversions
    ,sum(clicks) as clicks
    ,sum(view_quote_conversions) as view_quote_conversions
    ,sum(lead_conversions) as lead_conversions
    ,sum(purchase_conversions) as purchase_conversions
    ,sum(purchase_conv_revenue) as purchase_conv_value
  from {{ref('agg_marketing_bing_ads_by_campaign_type')}} 
  group by 1,2,3
),

core__paid_marketing as (
  select a.*
        ,b.total_spend
 --       ,b.conversions
        ,b.clicks
        ,b.view_quote_conversions
        ,b.lead_conversions
        ,b.purchase_conversions
        ,b.purchase_conv_value
  from core__quote_response_volume a
  left join int_marketing_by_campaign b
  on a.date = b.date
  and a.channel = b.channel
  and a.subchannel = b.subchannel 
),

int_affiliates as (
    select date
        ,'paid_marketing' as channel
        ,'affiliate' as subchannel
        ,sum(cost_gbp) as total_spend
        ,sum(conversions) as conversions
        ,sum(clicks) as clicks
    from   {{ref('agg_affiliate_conectia')}}
    group by 1,2,3
),

core__affiliate as (
  select a.* except(total_spend,clicks,purchase_conversions)
        ,case
          when a.channel = 'paid_marketing' and a.subchannel = 'affiliate' then b.total_spend
          else a.total_spend
         end as total_spend
         ,case
          when a.channel = 'paid_marketing' and a.subchannel = 'affiliate' then b.clicks
          else a.clicks
         end as clicks
         ,case 
            when a.channel = 'paid_marketing' and a.subchannel = 'affiliate' then b.conversions
            else a.purchase_conversions
          end as purchase_conversions
  from core__paid_marketing a
  left join int_affiliates b
  on a.date = b.date
  and a.channel =b.channel
  and a.subchannel = b.subchannel
),

int_referral_code_shares as (
  select *
        ,'referral' as channel
        ,'referral' as subchannel 
  from {{ref('agg_daily_referrals')}}
),

core__referrals as (
  select a.*
        ,b.unique_referrals_created as referral_code_shares
  from core__affiliate a
  left join int_referral_code_shares b
  on a.date = b.created_date
  and a.channel = b.channel
  and a.subchannel = b.subchannel
),

int_sales_volume as (
  SELECT 
    created_date
    ,case
      when quote_source = 'direct' then 'direct'
      when quote_source = 'referral' then 'referral'
      else 'pcw'
    end as channel
    ,case 
        when trim(quote_source)='direct' then 'organic'
        else trim(quote_source) 
    end as subchannel
    ,sum(total_sold_policies) as sales_volume
    ,avg(avg_monthly_price) as avg_monthly_price
    ,avg(avg_annual_price) as avg_annual_price
  FROM {{ref('agg_sold_policies_excl_renewals')}}
  group by 1,2,3
),

core__sales as (

/*
    Logic for the sales volume:
    direct/organic = actual direct sales - platform reported marketing purchases
    This column has been modified and contains marketing purchases as well as an 
*/

select a.*
        --,b.sales_volume
        ,case 
          when 
            a.channel = 'direct' 
              and a.subchannel = 'organic' 
            then sum(sales_volume) over(partition by date,a.channel,a.subchannel)-round(sum(a.purchase_conversions) over(partition by date),0)
          when 
            a.channel = 'referral' 
              or a.channel = 'pcw' 
            then b.sales_volume
          else a.purchase_conversions 
        end as sales_volume_adjusted
        ,b.avg_monthly_price
        ,b.avg_annual_price
  from core__referrals a
  left join int_sales_volume b
  on a.date = b.created_date
  and a.channel = b.channel
  and a.subchannel = b.subchannel
),

int_ga4 as (
    select 
        event_date as date
        ,napo_channel
        ,napo_subchannel
        ,count(distinct user_id) as users 
        ,countif(event_name='page_view') as total_pageviews
        ,countif(event_name='view_quote') as total_quote_views
        ,countif(event_name='generate_lead') as total_leads
        ,count(distinct if(event_name='page_view',user_id,null)) as user_pageviews
        ,count(distinct if(event_name='view_quote',user_id,null)) as user_quote_views
        ,count(distinct if(event_name='generate_lead',user_id,null)) as user_leads
        ,count(distinct if(event_name='page_view',concat(user_id,ga_session_id),null)) as session_pageviews
        ,count(distinct if(event_name='view_quote',concat(user_id,ga_session_id),null)) as session_quote_views
        ,count(distinct if(event_name='generate_lead',concat(user_id,ga_session_id),null)) as session_leads

    from {{ref('fct_ga4')}}
    group by 1,2,3
),

core__ga4 as (
    select a.*
          ,b.total_pageviews as landing_page_count
          ,b.total_quote_views as quote_landing_count
--          ,b.total_leads as lead_capture_count
          ,b.session_pageviews as landing_page_sessions
          ,b.session_quote_views as quote_landing_sessions
          ,b.user_pageviews as landing_page_users
          ,b.user_quote_views as quote_landing_users
--          ,b.session_leads as lead_capture_sessions

    from core__sales a
    left join int_ga4 b
    on a.date = b.date
    and a.channel = b.napo_channel
    and a.subchannel = b.napo_subchannel
)
select 
        date
        ,channel
        ,subchannel
        ,total_spend
        ,clicks as platform_reported_clicks
        ,view_quote_conversions as platform_reported_quote_conversions
        ,purchase_conversions as platform_reported_purchase_conversions
        ,lead_conversions as platform_reported_lead_conversions
        ,referral_code_shares
        ,landing_page_count
        ,landing_page_sessions
        ,landing_page_users
        ,quote_landing_count
        ,quote_landing_sessions
        ,quote_landing_users
        ,quote_response_volume
        ,sales_volume_adjusted
--        ,average_policy_price
from core__ga4
where date < current_date()
order by 1 desc,2,3