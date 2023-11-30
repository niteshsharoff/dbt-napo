with fb as (
    select *
    from {{ref('agg_marketing_facebook_ads')}}
),

bing as (
    select *
    from {{ref("agg_marketing_bing_ads")}}
),

gads as (
    select *
    from {{ref("agg_marketing_google_ads")}}
),

date_spine as (
select dt as date
from unnest(GENERATE_DATE_ARRAY('2023-01-01',date_sub(current_date(),interval 1 day))) dt
),

joined as (
    select 
         a.date
        ,sum(b.impressions)+sum(c.impressions)+sum(d.impressions) as impressions
        ,sum(b.clicks)+sum(c.clicks)+sum(d.clicks) as clicks
        ,round(sum(b.conversions)+sum(c.conversions)+sum(d.conversions),3) as conversions
        ,round(sum(b.cost_gbp)+sum(c.cost_gbp)+sum(d.cost_gbp),3) as cost_gbp
    from date_spine a
    left join fb b   on a.date = b.date
    left join bing c on a.date = c.date
    left join gads d on a.date = d.date
    group by 1 
--    order by 1 desc
),
features as (
    select * 
          ,round(safe_divide(clicks,impressions)*100,3) as ctr
          ,round(safe_divide(cost_gbp,conversions),2) as cost_per_conversion
          ,round(safe_divide(cost_gbp,clicks),2) as cost_per_click
          ,round(safe_divide(cost_gbp,safe_divide(impressions,1000)),2) as cpm
    from joined
)
select *
from features