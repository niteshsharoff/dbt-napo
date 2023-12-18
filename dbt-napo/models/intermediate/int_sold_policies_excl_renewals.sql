{{
    config(
        partition_by={
            "field": "created_date",
            "data_type": "date",
            "granularity": "day",
        },
        cluster_by=["quote_source"],
        tags=["daily", "growth"],
    )
}}

with
    data as (
        select
            created_date,
            quote_source,
            count(policy_id) as total_sold_policies,
            round(avg(monthly_price), 3) as avg_monthly_price,
            round(avg(annual_price), 3) as avg_annual_price
        from {{ ref("dim_policy_detail") }}
        where
            (is_subscription_active is not null or annual_payment_id is not null)
            and quote_source != 'renewal'
        group by 1, 2
    )
select *
from data
