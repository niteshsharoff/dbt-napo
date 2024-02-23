{{ config(tags=["daily", "growth"]) }}

with
    raw as (
        select
            cast(timeperiod as date) as timeperiod,
            campaignid,
            campaignname,
            goal,
            allconversionsqualified,
            allconversions,
            allrevenue,
            row_number() over (
                partition by concat(timeperiod, campaignid, coalesce(goal, '-'))
            ) as row_no
        from {{ ref("stg_src_airbyte__bing_goals_and_funnels_report_request") }}
        qualify row_no = 1
    )
select
    timeperiod as date,
    campaignid,
    sum(
        case
            when goal = 'generate_lead' then cast(allconversions as numeric) else null
        end
    ) as lead_conversions,
    sum(
        case when goal = 'view_quote' then cast(allconversions as numeric) else null end
    ) as view_quote_conversions,
    sum(
        case when goal = 'purchase' then cast(allconversions as numeric) else null end
    ) as purchase_conversions,
    sum(
        case
            when goal = 'generate_lead'
            then cast(allconversionsqualified as numeric)
            else null
        end
    ) as lead_conversions_qualified,
    sum(
        case
            when goal = 'view_quote'
            then cast(allconversionsqualified as numeric)
            else null
        end
    ) as view_quote_conversions_qualified,
    sum(
        case
            when goal = 'purchase'
            then cast(allconversionsqualified as numeric)
            else null
        end
    ) as purchase_conversions_qualified,
    sum(
        case when goal = 'purchase' then cast(allrevenue as numeric) else null end
    ) as purchase_conversion_revenue

from raw
group by 1, 2
