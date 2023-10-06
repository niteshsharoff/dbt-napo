{{ config(schema="marts") }}

with
    snapshot_details as (
        select
            date_sub(
                parse_date('%Y-%m-%d', '{{run_started_at.date()}}'), interval 1 day
            ) as snapshot_date,
            timestamp(
                date_sub(
                    parse_date('%Y-%m-%d', '{{run_started_at.date()}}'), interval 1 day
                )
            ) as snapshot_at
    ),
    benefit_code_usage as (
        select
            policy.customer.uuid as customer_uuid,
            policy.customer.email as customer_email,
            policy.quote.quote_id as quote_uuid,
            policy.policy.start_date as policy_start_date,
            case
                when policy.policy.sold_at is null
                then "not_purchased"
                when policy.policy.cancelled_at is not null
                then 'cancelled'
                when policy.policy.start_date > snapshot_details.snapshot_date
                then 'not_started'
                else 'active'
            end as policy_status,
            policy.policy.policy_id as policy_id,
            policy.customer.first_name as customer_first_name,
            policy.pet.name as policy_pet_name,
            napobenefitcode.code as promotion_code,
        from raw.quotewithbenefit, snapshot_details
        inner join
            (
                select *
                from raw.napobenefitcode, snapshot_details
                where run_date = snapshot_details.snapshot_date
            ) napobenefitcode
            on quotewithbenefit.benefit_id = napobenefitcode.id
        inner join
            {{ ref("int_policy_history") }} as policy
            on policy.quote.quote_id = quotewithbenefit.quote_id
            and policy.row_effective_to > snapshot_details.snapshot_at
            and policy.row_effective_from <= snapshot_details.snapshot_at
    )
select
    promotion_code,
    customer_uuid,
    customer_email,
    quote_uuid,
    min(policy_start_date) as quote_start_date,
    countif(policy_status in ('active', 'not_started')) > 0 as has_active_policies,
    false as customer_is_in_arrears,
    customer_first_name,
    array_agg(policy_pet_name) as customer_pet_names
from benefit_code_usage
group by promotion_code, customer_uuid, customer_email, quote_uuid, customer_first_name
