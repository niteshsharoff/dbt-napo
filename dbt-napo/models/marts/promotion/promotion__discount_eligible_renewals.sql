{{ config(schema="marts") }}
with
    snapshot_details as (
        select parse_date('%Y-%m-%d', '{{run_started_at.date()}}') as snapshot_date
    ),
    policy_status as (
        select
            customer.uuid as customer_uuid,
            case
                when policy.cancel_date is not null and policy.sold_at is not null
                then 'cancelled'
                when policy.sold_at is null
                then 'not_purchased'
                when snapshot_date < policy.start_date
                then 'not_started'
                when snapshot_date >= policy.end_date
                then 'lapsed'
                else 'active'
            end as status
        from {{ ref("int_policy_history") }}, snapshot_details
        where
            timestamp(snapshot_date) >= row_effective_from
            and timestamp(snapshot_date) < row_effective_to
    ),
    customer_active_policy_count as (
        select customer_uuid, count(*) as n_active_policies
        from policy_status
        where status in ('active', 'not_started')
        group by customer_uuid
    ),
    pending_renewal as (
        select
            customer.email,
            renewal.uuid as renewal_uuid,
            old_policy.end_date as renewal_date,
            not old_policy.renewal_approved as opted_out,
            annual_retail_price as year_2_annual_retail_price,
            voucher_code as year_2_voucher_code,
            old_policy.annual_price as year_1_annual_retail_price,
            annual_retail_price
            / old_policy.annual_price as year_1_to_2_retail_price_ratio,
            array_length(old_policy_claims) > 0 as year_1_has_claims,
            n_active_policies as customer_n_active_policies
        from `dbt_tim.int_renewal_snapshot`, snapshot_details
        left join
            customer_active_policy_count
            on customer.uuid = customer_active_policy_count.customer_uuid
        where
            old_policy.end_date > snapshot_details.snapshot_date
            and old_policy.cancel_date is null
    )
select
    *,
    case
        when
            customer_n_active_policies = 1
            and opted_out
            and year_2_voucher_code is null
            and not year_1_has_claims
            and year_1_to_2_retail_price_ratio >= 1.45
        then 15
        when
            customer_n_active_policies = 1
            and opted_out
            and year_2_voucher_code is null
            and not year_1_has_claims
            and year_1_to_2_retail_price_ratio >= 1.3
        then 10
        when
            customer_n_active_policies = 1
            and opted_out
            and year_2_voucher_code is null
            and not year_1_has_claims
            and year_1_to_2_retail_price_ratio >= 1.2
        then 5
    end as eligible_discount_percent
from pending_renewal
