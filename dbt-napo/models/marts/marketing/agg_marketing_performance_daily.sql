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
from unnest(GENERATE_DATE_ARRAY('2023-10-01',date_sub(current_date(),interval 1 day))) dt
),

joined as (
    select 
         a.date
        ,sum(b.impressions,c.impressions,d.impressions) as impressions
        ,sum(b.clicks,c.clicks,d.clicks) as clicks
        ,sum(b.conversions,c.conversions,d.conversions) as conversions
        ,sum(b.cost,c.cost,d.cost) as cost
    from date_spine a
    left join fb b   on a.date = b.date
    left join bing c on a.date = c.date
    left join gads d on a.date = d.date
    group by 1 
    order by 1 desc
)
    select * 
            --cpm
            --cpc
            --
    from joined
