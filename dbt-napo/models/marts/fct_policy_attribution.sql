with policies as (
select 
      policy_id
      ,cast(cast(created_date as timestamp) as date) as created_date
      ,quote_id
      ,reference_number
      ,annual_payment_id
      ,active_subscription_setup
      ,cast(cast(last_subscription_created_date as timestamp) as date) as last_subscription_created_date
      ,policy_start_date
      ,policy_cancel_date
      ,payment_plan_type
      ,quote_source_reference
      ,quote_source
      ,cast(cast (first_payment_charge_date as timestamp) as date) as first_payment_charge_date
      ,user
from {{ref('dim_policy_detail')}}
where (annual_payment_id is not null or is_subscription_active is not null)
),
ga4_purchases as (
select * except(row_no)
from (
  select *
        ,row_number() over(partition by transaction_id order by date asc) as row_no  
  from {{ref('fct_ga4_purchases')}}
  --where transaction_id = 'a20110be-cb1c-48f8-97ec-53ec52f9f0d3'
)
where row_no = 1
),
joined as (
select 
       a.policy_id
      ,a.created_date
      ,a.payment_plan_type
      ,a.quote_source as backend_quote_source
      ,b.medium as ga4_medium
      ,case
        when b.source = 'moneysupermarket.com' then 'moneysupermarket'
        when b.source='pet.gocompare.com' then 'gocompare'
        when b.source in ('l.facebook.com','m.facebook.com','lm.facebook.com') then 'facebook'
        when b.source in ('instagram.com','linktr.ee') then 'instagram'
        when b.source = 'uk.search.yahoo.com' then 'yahoo'
       -- when b.source in ('duckduckgo','ecosia.org') then b.source 
        else b.source
      end as ga4_source
      ,b.name as ga4_campaign_name
       ,a.quote_id as backend_quote_id
      ,b.quote_id as ga_quote_id
from policies a
left join ga4_purchases b
on b.transaction_id = a.quote_id 
order by a.created_date desc
),
rolled_up as (
select 
    format_date('%Y-%m',created_date) as month
    ,case
      when (backend_quote_source in ('direct') AND ga4_medium in ('company_profile','display')) then ga4_source
      when (backend_quote_source in ('direct') AND ga4_medium in ('referral','organic') and ga4_source in ('duckduckgo','ecosia.org','yahoo','bing')) then 'non-google search'
      when (backend_quote_source in ('direct') AND ga4_medium = 'referral' and ga4_source in ('gocompare','moneysupermarket')) then 'direct via pcw'
      when (backend_quote_source in ('direct') AND ga4_medium = 'referral' and ga4_source in ('instagram','facebook')) then 'social organic'
      when (backend_quote_source in ('direct') AND ga4_medium = 'cpc') then concat(ga4_source,' ads')
      when (backend_quote_source in ('direct') AND ga4_medium = 'email') then 'napo email'
      when (backend_quote_source in ('direct') AND ga4_medium = 'organic') then concat(ga4_source,' organic')
      --when (backend_quote_source in ('direct') AND ga4_medium = 'organic' and ga4_source like '%organic%') then ga4_source 
      when (backend_quote_source in ('direct') AND ga4_medium = 'referral_code') then 'direct'
      when (backend_quote_source in ('direct') AND ga4_medium not in ('(none)','direct','(direct)') AND ga4_medium is not null) then ga4_medium
      else backend_quote_source
    end as rolled_up_source
    ,backend_quote_source
    ,ga4_medium
    ,ga4_source
    ,count(policy_id) as policy_count
from joined
group by 1,2,3,4,5
order by 1 desc,6 desc
),
final as (
select 
    month
  ,rolled_up_source as marketing_channel_attribution
  ,sum(policy_count) as total
from rolled_up
  group by 1,2
  order by 3 desc
)
select *
,round(safe_divide(sum(total),sum(total) over(partition by month)),3) as percentage 
from final
group by 1,2,3
order by month desc ,total desc