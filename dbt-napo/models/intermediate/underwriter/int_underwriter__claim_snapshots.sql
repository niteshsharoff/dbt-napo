with
    claim as (
        select
            claim.id as claim_id,
            claim.policy_id as claim_policy_id,
            claim.status as claim_status,
            claim.master_claim_id as claim_master_claim_id,
            claim.date_received as claim_received_date,
            claim.onset_date as claim_incident_date,
            claim.invoice_amount as claim_invoice_amount,
            claim.is_continuation as claim_is_continuation,
            {{ target.schema }}.calculate_claim_excess_amount(
                claim.is_continuation, policy.product_excess, claim.invoice_amount
            ) as claim_excess_amount,
            0 as claim_recovery_amount,
            claim.cover_type as claim_cover_type,
            claim.cover_sub_type as claim_cover_sub_type,
            claim.paid_amount as claim_paid_amount,
            claim.first_invoice_date as claim_first_invoice_date,
            claim.decline_reason as claim_decline_reason,
            claim.source as claim_source,
            policy.* except (
                product_vet_fee_cover,
                product_complementary_treatment_cover,
                product_dental_cover,
                product_emergency_boarding_cover,
                product_third_person_liability_cover,
                product_pet_death_cover,
                product_travel_cover,
                product_missing_pet_cover,
                product_behavioural_treatment_cover
            ),
            case
                when claim.cover_type = "vet_fee_cover"
                then product_vet_fee_cover
                when claim.cover_type = "complementary_treatment_cover"
                then product_complementary_treatment_cover
                when claim.cover_type = "dental_cover"
                then product_dental_cover
                when claim.cover_type = "emergency_boarding_cover"
                then product_emergency_boarding_cover
                when claim.cover_type = "third_person_liability_cover"
                then product_third_person_liability_cover
                when claim.cover_type = "pet_death_cover"
                then product_pet_death_cover
                when claim.cover_type = "travel_cover"
                then product_travel_cover
                when claim.cover_type = "missing_pet_cover"
                then product_missing_pet_cover
                when claim.cover_type = "behavioural_treatment_cover"
                then product_behavioural_treatment_cover
            end as product_cover_for_type,
            case
                when claim.cover_type = "vet_fee_cover"
                then paid.vet_fee_paid_amount
                when claim.cover_type = "complementary_treatment_cover"
                then paid.complementary_treatment_paid_amount
                when claim.cover_type = "dental_cover"
                then paid.dental_paid_amount
                when claim.cover_type = "emergency_boarding_cover"
                then paid.emergency_boarding_paid_amount
                when claim.cover_type = "third_person_liability_cover"
                then third_person_liability_paid_amount
                when claim.cover_type = "pet_death_cover"
                then paid.pet_death_paid_amount
                when claim.cover_type = "travel_cover"
                then paid.travel_paid_amount
                when claim.cover_type = "missing_pet_cover"
                then paid.missing_pet_paid_amount
                when claim.cover_type = "behavioural_treatment_cover"
                then paid.behavioural_treatment_paid_amount
            end as policy_paid_amount_for_type,
            claim.snapshot_date as snapshot_date,
        from {{ ref("int_claim_snapshots") }} as claim
        left join
            {{ ref("int_underwriter__policy_history") }} as policy
            on claim.policy_id = policy.policy_id
            and timestamp(snapshot_date) >= policy.effective_from
            and timestamp(snapshot_date) < policy.effective_to
        left join
            {{ ref("int_policy_paid_claim_snapshots") }} as paid
            on claim.policy_id = paid.policy_id
            and claim.snapshot_date = paid.snapshot_date
        where claim.policy_id is not null
    )
select
    *,
    {{ target.schema }}.calculate_claim_reserve_amount(
        claim_invoice_amount,
        claim_status,
        claim_is_continuation,
        product_excess,
        policy_co_pay_percent,
        product_cover_for_type,
        policy_paid_amount_for_type
    ) as claim_reserve_amount,
    {{ target.schema }}.calculate_claim_incurred_amount(
        {{ target.schema }}.calculate_claim_reserve_amount(
            claim_invoice_amount,
            claim_status,
            claim_is_continuation,
            product_excess,
            policy_co_pay_percent,
            product_cover_for_type,
            policy_paid_amount_for_type
        ),
        claim_paid_amount,
        claim_recovery_amount
    ) as claim_incurred_amount
from claim
