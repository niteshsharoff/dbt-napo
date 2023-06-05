SELECT
  policy_id,
  snapshot_date,
  COALESCE(vet_fee_paid_amount, 0) AS vet_fee_paid_amount,
  COALESCE(complementary_treatment_paid_amount, 0) AS complementary_treatment_paid_amount,
  COALESCE(dental_paid_amount, 0) AS dental_paid_amount,
  COALESCE(emergency_boarding_paid_amount, 0) AS emergency_boarding_paid_amount,
  COALESCE(behavioural_treatment_paid_amount, 0) AS behavioural_treatment_paid_amount,
  COALESCE(third_person_liability_paid_amount, 0) AS third_person_liability_paid_amount,
  COALESCE(pet_death_paid_amount, 0) AS pet_death_paid_amount,
  COALESCE(travel_paid_amount, 0) as travel_paid_amount,
  COALESCE(missing_pet_paid_amount, 0) AS missing_pet_paid_amount
FROM (
  SELECT
    policy_id,
    snapshot_date,
    cover_type,
    COALESCE(paid_amount, 0) AS paid_amount
  FROM
    {{ref("int_claim_snapshots")}}
) PIVOT(
  SUM(paid_amount) FOR cover_type IN (
    "vet_fee_cover" AS vet_fee_paid_amount,
    "complementary_treatment_cover" AS complementary_treatment_paid_amount,
    "dental_cover" AS dental_paid_amount,
    "emergency_boarding_cover" AS emergency_boarding_paid_amount,
    "third_person_liability_cover" AS third_person_liability_paid_amount,
    "pet_death_cover" AS pet_death_paid_amount,
    "travel_cover" AS travel_paid_amount,
    "missing_pet_cover" AS missing_pet_paid_amount,
    "behavioural_treatment_cover" AS behavioural_treatment_paid_amount
  )
)