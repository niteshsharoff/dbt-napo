{{ config(pre_hook=["{{declare_underwriter_udfs()}}"]) }}
with
    snapshot_details as (
        select parse_timestamp('%Y-%m-%d', '{{run_started_at.date()}}') as snapshot_at
    )
select
    policy.*,
    snapshot_details.*,
    {{ target.schema }}.calculate_policy_exposure(
        policy_start_date, extract(date from snapshot_at)
    ) as policy_exposure,
    {{ target.schema }}.calculate_policy_development_month(
        policy_start_date, policy_end_date, extract(date from snapshot_at)
    ) as policy_development_month,
    {{ target.schema }}.calculate_gross_earned_premium(
        policy_annual_premium_price,
        policy_start_date,
        policy_cancel_date,
        extract(date from snapshot_at)
    ) as policy_gross_earned_premium,
    policy_claim.* except (policy_id),
    policy_paid_claim.* except (policy_id)
from {{ ref("int_underwriter__policy_history") }} as policy, snapshot_details
left join
    (
        select
            policy_id,
            coalesce(sum(claim_incurred_amount), 0) as policy_incurred_amount,
            sum(
                if(
                    claim_cover_type = 'vet_fee_cover'
                    and claim_cover_sub_type = 'Accident',
                    claim_paid_amount,
                    0
                )
            ) as policy_vet_fee_accident_paid_amount,
            sum(
                if(
                    claim_cover_type = 'vet_fee_cover'
                    and claim_cover_sub_type = 'Illness',
                    claim_paid_amount,
                    0
                )
            ) as policy_vet_fee_illness_paid_amount
        from {{ ref("int_underwriter__claim_snapshot") }}
        group by policy_id
    ) as policy_claim
    on policy_claim.policy_id = policy.policy_id
left join
    (
        select
            policy_id,
            countif(
                claim_cover_type = 'vet_fee_cover' and claim_cover_sub_type = 'Accident'
            ) as policy_n_accepted_vet_fee_accident_claims,
            countif(
                claim_cover_type = 'vet_fee_cover' and claim_cover_sub_type = 'Illness'
            ) as policy_n_accepted_vet_fee_illness_claims,
            count(
                distinct(
                    if(
                        claim_cover_type = 'vet_fee_cover'
                        and claim_cover_sub_type = 'Accident',
                        claim_master_claim_id,
                        null
                    )
                )
            ) as policy_n_accepted_vet_fee_accident_master_claims,
            count(
                distinct(
                    if(
                        claim_cover_type = 'vet_fee_cover'
                        and claim_cover_sub_type = 'Illness',
                        claim_master_claim_id,
                        null
                    )
                )
            ) as policy_n_accepted_vet_fee_illness_master_claims
        from {{ ref("int_underwriter__claim_snapshot") }}
        where claim_status = 'accepted'
        group by policy_id
    ) as policy_paid_claim
    on policy_paid_claim.policy_id = policy.policy_id
where effective_from <= snapshot_at and snapshot_at < effective_to
