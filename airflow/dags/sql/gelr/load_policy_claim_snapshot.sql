insert into `{{ table_name }}` (
  policy_status
  , premium_price_ipt_inc
  , earned_premium_ipt_inc
  , invoice_claims_amount
  , paid_claims_amount
  , incurred_claims_amount
  , original_policy_id
  , policy_year
  , quote
  , policy
  , pet
  , customer
  , claims
  , snapshot_date
)
with
    constants as (select parse_date('%Y-%m-%d', '{{ run_date }}') as run_date),
    extended_policy_history as (
        select
            case
                when policy.sold_at is null
                then 'not_purchased'
                when policy.sold_at is not null and policy.cancel_date is not null
                then 'cancelled'
                when policy.start_date > constants.run_date
                then 'not_started'
                when constants.run_date >= policy.end_date
                then 'lapsed'
                else 'active'
            end as policy_status,
            constants,
            least(
                case
                    when policy.cancel_date is null
                    then date_diff(constants.run_date, policy.start_date, day)
                    else date_diff(policy.cancel_date, policy.start_date, day)
                end,
                365
            ) as policy_duration,
            quote,
            policy,
            customer,
            pet,
            premium_price as premium_price_ipt_inc,
            original_policy_id,
            policy_year,
            effective_from,
            effective_to,
            effective_from = max(effective_from) over (
                partition by policy.policy_uuid order by effective_from desc
            ) as _is_latest
        from `dbt_marts.gelr_policy_history` history, constants
        where date(effective_from) < constants.run_date
    ),
    policy_snapshot as (
        select
            policy_status,
            premium_price_ipt_inc,
            greatest(
                case
                    -- NTU
                    when
                        (policy_status = 'cancelled' and policy_duration < 14)
                        or (policy_status = 'not_purchased')
                        or (policy_status = 'not_started')
                    then 0.0
                    -- Pro-rated cancellation
                    when policy_status = 'cancelled'
                    then
                        premium_price_ipt_inc
                        * (date_diff(policy.cancel_date, policy.start_date, day) / 365)
                    -- Earned premium
                    else premium_price_ipt_inc * (policy_duration / 365)
                end,
                0.0
            ) as earned_premium_ipt_inc,
            original_policy_id,
            policy_year,
            quote,
            policy,
            pet,
            customer
        from extended_policy_history
        where _is_latest = true
    ),
    extended_claim_history as (
        select * except(run_date)
            , effective_from = max(effective_from) over (partition by claim_reference order by effective_from desc) as _is_latest
        from `dbt.dim_claim`, constants
        where effective_from <= constants.run_date
    ),
    claim_snapshot as (
        select * except (_is_latest)
        from extended_claim_history
        where _is_latest = true
    ),
    policy_claim_snapshot as (
        select p.policy.reference_number, array_agg(c) as claims
        from policy_snapshot p
        join claim_snapshot c on p.policy.reference_number = c.policy_number
        group by p.policy.reference_number
    ),
    final as (
        select p.*, c.claims
        from policy_snapshot p
        left join policy_claim_snapshot c on p.policy.reference_number = c.reference_number
    )
select policy_status
  , premium_price_ipt_inc
  , earned_premium_ipt_inc
  , coalesce((select sum(claim.invoice_amount) from unnest(claims) as claim), 0.0) as invoice_claims_amount
  , coalesce((select sum(claim.paid_amount) from unnest(claims) as claim), 0.0) as paid_claims_amount
  , coalesce((select sum(claim.incurred_amount) from unnest(claims) as claim), 0.0) as incurred_claims_amount
  , original_policy_id
  , policy_year
  , quote
  , policy
  , pet
  , customer
  , claims
  , run_date as snapshot_date
from final, constants
