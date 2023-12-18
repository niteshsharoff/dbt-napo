{{
    config(
        materialized="table",
        partition_by={"field": "date", "data_type": "date", "granularity": "day"},
        tags=["daily", "growth"],
    )
}}

with
    daily_bing as (
        select
            a.timeperiod as date,
            a.campaignid,
            a.campaignname,
            sum(a.impressions) as impressions,
            sum(a.clicks) as clicks,
            sum(a.allconversions) as all_conversions,
            sum(a.revenue) as all_conv_value,
            sum(a.spend) as cost_gbp
        from {{ ref("stg_src_airbyte__bing_campaign_performance_report_daily") }} a
        group by 1, 2, 3
    ),
    goals_bing as (select * from {{ ref("int_bing_goals_and_funnels_daily") }}),
    campaign_level_bing as (
        select
            a.date,
            a.campaignid,
            a.campaignname,
            a.impressions,
            a.clicks,
            a.all_conversions,
            a.all_conv_value,
            a.cost_gbp,
            b.view_quote_conversions as view_quote_conversions,
            b.view_quote_conversions_qualified as view_quote_conversions_qualified,
            b.lead_conversions as lead_conversions,
            b.lead_conversions_qualified as lead_conversions_qualified,
            b.purchase_conversions as purchase_conversions,
            b.purchase_conversions_qualified as purchase_conversions_qualified,
            b.purchase_conversion_revenue as purchase_conv_revenue

        from daily_bing a
        left join
            goals_bing b
            on a.date = b.date
            and cast(a.campaignid as numeric) = cast(b.campaignid as numeric)
    )

select
    date,
    {{ marketing_campaign_classification("CampaignName") }} as napo_campaign_type,
    {{ agg("impressions") }},
    {{ agg("clicks") }},
    {{ agg("all_conversions") }},
    {{ agg("all_conv_value") }},
    {{ agg("cost_gbp") }},
    {{ agg("view_quote_conversions") }},
    {{ agg("view_quote_conversions_qualified") }},
    {{ agg("lead_conversions") }},
    {{ agg("lead_conversions_qualified") }},
    {{ agg("purchase_conversions") }},
    {{ agg("purchase_conversions_qualified") }},
    {{ agg("purchase_conv_revenue") }}
from campaign_level_bing
group by 1, 2
