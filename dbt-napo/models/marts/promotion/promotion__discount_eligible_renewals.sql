with
    snapshot_details as (
        select parse_date('%Y-%m-%d', '{{run_started_at.date()}}') as snapshot_date
    ),
    pending_renewal as (
        select
            renewal.uuid as renewal_uuid,
            old_policy.end_date as renewal_date,
            not old_policy.renewal_approved as opted_out,
            annual_retail_price as year_2_annual_retail_price,
            voucher_code as year_2_voucher_code,
            old_policy.annual_price as year_1_annual_retail_price,
            annual_retail_price
            / old_policy.annual_price as year_1_to_2_retail_price_ratio,
            array_length(old_policy_claims) > 0 as year_1_has_claims,
        from `dbt_tim.int_renewal_snapshot`, snapshot_details
        where old_policy.end_date > snapshot_details.snapshot_date
    )
select
    *,
    case
        when
            opted_out
            and year_2_voucher_code is null
            and not year_1_has_claims
            and year_1_to_2_retail_price_ratio >= 1.45
        then 15
        when
            opted_out
            and year_2_voucher_code is null
            and not year_1_has_claims
            and year_1_to_2_retail_price_ratio >= 1.3
        then 10
        when
            opted_out
            and year_2_voucher_code is null
            and not year_1_has_claims
            and year_1_to_2_retail_price_ratio >= 1.2
        then 5
    end as eligible_discount_percent
from pending_renewal
