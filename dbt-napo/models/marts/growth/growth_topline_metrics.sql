{{ config(schema="marts") }}

with
    overall as (
        select distinct
            pet.pet_id,
            policy.policy_id,
            policy.start_date as policy_start_date,
            policy.end_date,
            policy.cancel_date,
            policy.current_policy_year,
            policy.created_date,
            policy.annual_price,
            policy.quote_source,
            policy.cancel_reason as policy_cancel_reason,
            coalesce(gross_premium_ipt_inc, policy.annual_price) as annual_uw_price,
            policy.change_at,
            rank() over (
                partition by pet.pet_id, policy.current_policy_year
                order by policy.change_at desc
            ) as ranking
        from {{ ref("int_policy_history") }} a
        left join
            {{ ref("cgice_cumulative_premium_bdx") }} b
            on a.policy.policy_id = b.policy_id
        where
            (policy.sold_at is not null or policy.annual_payment_id is not null)
            and transaction_type in ('New Policy', 'Renewal')
    )
select
    pet_id,
    format_date('%Y-%m', policy_start_date) as cohort_started,
    format_date('%Y-%m', created_date) as cohort_created,
    format_date('%Y-%m', cancel_date) as cohort_cancellation,
    policy_start_date,
    created_date,
    cancel_date,
    current_policy_year,
    policy_cancel_reason,
    end_date,
    case
        when date_diff(cancel_date, policy_start_date, day) < 0
        then 0
        else date_diff(cancel_date, policy_start_date, day)
    end as days_to_cancellation,
    quote_source,
    annual_price,
    annual_uw_price
from overall
where ranking = 1
