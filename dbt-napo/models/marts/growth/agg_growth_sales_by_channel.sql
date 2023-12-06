--Notes for Kaveh:
--> Added 'direct','direct' for unknown data like quote requests
--> Total sales I'm pulling from policy_details, did you want that or did you want sales as per ad platforms?
--> Not all paid is going into the paid_marketing and lead_generation buckets. We have some spend un-accounted for.
--> It looks like all we're pulling from marketing is the spent, did we need anything else?
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
        ('direct', 'organic'),
        ('direct', 'blog'),
        ('direct','direct'),
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
    and a.subchannel = b.quote_source
),

int_marketing_by_campaign as (
  select 
  date
  ,case 
    when napo_campaign_type='leadgen' then 'lead_generation'
    when napo_campaign_type='growth' then 'paid_marketing'
    else 'other'
   end as channel
  ,'facebook' as subchannel
  ,sum(cost_gbp) as total_spend
  from {{ref('agg_marketing_facebook_ads_by_campaign_type')}}
  group by 1,2,3
  union all 
  select 
      date
      ,case 
      when napo_campaign_type='growth' then 'paid_marketing'
      when napo_campaign_type='leadgen' then 'lead_generation'
      else 'other'
      end as channel
      ,case
      when is_youtube_campaign is true then 'youtube'
      else 'google'
      end as sub_channel
      ,sum(cost_gbp) as total_spend
  from {{ref('agg_marketing_google_ads_by_campaign_type')}}
  group by 1,2,3
),

core__paid_marketing as (
  select a.*
        ,b.total_spend
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
from   {{ref('agg_affiliate_conectia')}}
group by 1,2,3
),

core__affiliate as (
  select a.* except(total_spend)
        ,case
          when a.channel = 'paid_marketing' and a.subchannel = 'affiliate' then b.total_spend
          else a.total_spend
         end as total_spend
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
    ,trim(quote_source) as subchannel
    ,sum(total_sold_policies) as sales_volume
    ,avg(avg_monthly_price) as avg_monthly_price
    ,avg(avg_annual_price) as avg_annual_price
FROM {{ref('agg_sold_policies_excl_renewals')}}
group by 1,2,3
),
core__sales as (
  select a.*
        ,b.sales_volume
        ,b.avg_monthly_price
        ,b.avg_annual_price
  from core__referrals a
  left join int_sales_volume b
  on a.date = b.created_date
  and a.channel = b.channel
  and a.subchannel = b.subchannel
)

select * 
from core__sales
order by date desc