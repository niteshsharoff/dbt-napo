select
    policy_id,
    snapshot_date,
    coalesce(vet_fee_paid_amount, 0) as vet_fee_paid_amount,
    coalesce(
        complementary_treatment_paid_amount, 0
    ) as complementary_treatment_paid_amount,
    coalesce(dental_paid_amount, 0) as dental_paid_amount,
    coalesce(emergency_boarding_paid_amount, 0) as emergency_boarding_paid_amount,
    coalesce(behavioural_treatment_paid_amount, 0) as behavioural_treatment_paid_amount,
    coalesce(
        third_person_liability_paid_amount, 0
    ) as third_person_liability_paid_amount,
    coalesce(pet_death_paid_amount, 0) as pet_death_paid_amount,
    coalesce(travel_paid_amount, 0) as travel_paid_amount,
    coalesce(missing_pet_paid_amount, 0) as missing_pet_paid_amount
from
    (
        select
            policy_id,
            status,
            snapshot_date,
            cover_type,
            coalesce(paid_amount, 0) as paid_amount
        from {{ ref("int_claim_snapshots") }}
        where status in ('accepted')
    ) pivot (
        sum(paid_amount) for cover_type in (
            "vet_fee_cover" as vet_fee_paid_amount,
            "complementary_treatment_cover" as complementary_treatment_paid_amount,
            "dental_cover" as dental_paid_amount,
            "emergency_boarding_cover" as emergency_boarding_paid_amount,
            "third_person_liability_cover" as third_person_liability_paid_amount,
            "pet_death_cover" as pet_death_paid_amount,
            "travel_cover" as travel_paid_amount,
            "missing_pet_cover" as missing_pet_paid_amount,
            "behavioural_treatment_cover" as behavioural_treatment_paid_amount
        )
    )
