{{ config(severity="warn") }}
/*
  GIVEN
    a snapshot of all claims

  WHEN
    we sum up amount paid out in claims per policy by cover type and snaphsot date
    and we pivot + join amount paid out per policy, cover type and snapshot date to each claim
    and the claim_status is 'accepted'

  THEN
    the reserve amount + total amount paid out per cover type should not exceed the product cover amount for each claim
*/
with
    cumulative_paid_amount_by_cover_snapshot as (
        select
            policy_uuid,
            claim_source,
            claim_cover_type,
            sum(claim_paid_amount) paid_amount,
            snapshot_date
        from {{ ref("int_underwriter__claim_snapshot") }}
        where snapshot_date = parse_date('%Y-%m-%d', '{{run_started_at.date()}}')
        group by policy_uuid, claim_source, claim_cover_type, snapshot_date
    ),
    pivot_paid_amount_to_policy_grain as (
        select *
        from
            cumulative_paid_amount_by_cover_snapshot pivot (
                sum(paid_amount) for claim_cover_type in (
                    'vet_fee_cover' as used_vet_fee_cover,
                    'complementary_treatment_cover'
                    as used_complementary_treatment_cover,
                    'dental_cover' as used_dental_cover,
                    'behavioural_treatment_cover' as used_behavioural_treatment_cover,
                    'emergency_boarding_cover' as used_emergency_boarding_cover,
                    'third_person_liability_cover' as used_third_person_liability_cover,
                    'pet_death_cover' as used_pet_death_cover,
                    'travel_cover' as used_travel_cover,
                    'missing_pet_cover' as used_missing_pet_cover
                )
            )
    ),
    final as (
        select
            c.policy_reference_number,
            c.claim_master_claim_id,
            c.claim_id,
            c.claim_status,
            c.claim_source,
            c.claim_cover_type,
            c.claim_reserve_amount,
            c.claim_paid_amount,
            policy.product_vet_fee_cover,
            policy.product_complementary_treatment_cover,
            policy.product_dental_cover,
            policy.product_emergency_boarding_cover,
            policy.product_third_person_liability_cover,
            policy.product_pet_death_cover,
            policy.product_travel_cover,
            policy.product_missing_pet_cover,
            policy.product_behavioural_treatment_cover,
            p.used_vet_fee_cover,
            p.used_complementary_treatment_cover,
            p.used_dental_cover,
            p.used_behavioural_treatment_cover,
            p.used_emergency_boarding_cover,
            p.used_third_person_liability_cover,
            p.used_pet_death_cover,
            p.used_travel_cover,
            p.used_missing_pet_cover,
            c.snapshot_date
        from {{ ref("int_underwriter__claim_snapshot") }} c
        join
            pivot_paid_amount_to_policy_grain p
            on c.policy_uuid = p.policy_uuid
            and c.snapshot_date = p.snapshot_date
            and c.claim_source = p.claim_source
        join
            {{ ref("int_underwriter__policy_snapshot") }} policy
            on policy.policy_uuid = c.policy_uuid
        where
            (
                claim_cover_type = 'vet_fee_cover'
                and claim_reserve_amount + p.used_vet_fee_cover
                > policy.product_vet_fee_cover
            )
            or (
                claim_cover_type = 'complementary_treatment_cover'
                and claim_reserve_amount + p.used_complementary_treatment_cover
                > policy.product_complementary_treatment_cover
            )
            or (
                claim_cover_type = 'dental_cover'
                and claim_reserve_amount + p.used_dental_cover
                > policy.product_dental_cover
            )
            or (
                claim_cover_type = 'behavioural_treatment_cover'
                and claim_reserve_amount + p.used_behavioural_treatment_cover
                > cast(policy.product_behavioural_treatment_cover as float64)
            )
            or (
                claim_cover_type = 'emergency_boarding_cover'
                and claim_reserve_amount + p.used_emergency_boarding_cover
                > policy.product_emergency_boarding_cover
            )
            or (
                claim_cover_type = 'third_person_liability_cover'
                and claim_reserve_amount + p.used_third_person_liability_cover
                > policy.product_third_person_liability_cover
            )
            or (
                claim_cover_type = 'pet_death_cover'
                and claim_reserve_amount + p.used_pet_death_cover
                > policy.product_pet_death_cover
            )
            or (
                claim_cover_type = 'travel_cover'
                and claim_reserve_amount + p.used_travel_cover
                > policy.product_travel_cover
            )
            or (
                claim_cover_type = 'missing_pet_cover'
                and claim_reserve_amount + p.used_missing_pet_cover
                > policy.product_missing_pet_cover
            )
    )
select *
from final
where claim_status = 'accepted'
