WITH claim AS (
  SELECT
    claim.id AS claim_id,
    claim.policy_id AS claim_policy_id,
    claim.status AS claim_status,
    claim.master_claim_id AS claim_master_claim_id,
    claim.date_received AS claim_received_date,
    claim.onset_date AS claim_incident_date,
    claim.invoice_amount AS claim_invoice_amount,
    claim.is_continuation AS claim_is_continuation,
    {{target.schema}}.calculate_claim_excess_amount (
      claim.is_continuation,
      policy.product_excess,
      claim.invoice_amount
    ) AS claim_excess_amount,
    0 AS claim_recovery_amount,
    claim.cover_type AS claim_cover_type,
    claim.cover_sub_type AS claim_cover_sub_type,
    claim.paid_amount AS claim_paid_amount,
    claim.first_invoice_date AS claim_first_invoice_date,
    claim.decline_reason AS claim_decline_reason,
    claim.source AS claim_source,
    policy.* EXCEPT (
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
    CASE
      WHEN claim.cover_type = "vet_fee_cover" THEN product_vet_fee_cover
      WHEN claim.cover_type = "complementary_treatment_cover" THEN product_complementary_treatment_cover
      WHEN claim.cover_type = "dental_cover" THEN product_dental_cover
      WHEN claim.cover_type = "emergency_boarding_cover" THEN product_emergency_boarding_cover
      WHEN claim.cover_type = "third_person_liability_cover" THEN product_third_person_liability_cover
      WHEN claim.cover_type = "pet_death_cover" THEN product_pet_death_cover
      WHEN claim.cover_type = "travel_cover" THEN product_travel_cover
      WHEN claim.cover_type = "missing_pet_cover" THEN product_missing_pet_cover
      WHEN claim.cover_type = "behavioural_treatment_cover" THEN product_behavioural_treatment_cover
    END AS product_cover_for_type,
    CASE
      WHEN claim.cover_type = "vet_fee_cover" THEN paid.vet_fee_paid_amount
      WHEN claim.cover_type = "complementary_treatment_cover" THEN paid.complementary_treatment_paid_amount
      WHEN claim.cover_type = "dental_cover" THEN paid.dental_paid_amount
      WHEN claim.cover_type = "emergency_boarding_cover" THEN paid.emergency_boarding_paid_amount
      WHEN claim.cover_type = "third_person_liability_cover" THEN third_person_liability_paid_amount
      WHEN claim.cover_type = "pet_death_cover" THEN paid.pet_death_paid_amount
      WHEN claim.cover_type = "travel_cover" THEN paid.travel_paid_amount
      WHEN claim.cover_type = "missing_pet_cover" THEN paid.missing_pet_paid_amount
      WHEN claim.cover_type = "behavioural_treatment_cover" THEN paid.behavioural_treatment_paid_amount
    END AS policy_paid_amount_for_type,
    claim.snapshot_date AS snapshot_date,
  FROM
    {{ ref("int_claim_snapshots") }} AS claim
  LEFT JOIN
      {{ ref("int_underwriter__policy_history") }} AS policy
  ON
      claim.policy_id = policy.policy_id
  AND
      TIMESTAMP(snapshot_date) >= policy.effective_from
  AND
      TIMESTAMP(snapshot_date) < policy.effective_to
  LEFT JOIN
      {{ ref("int_policy_paid_claim_snapshots") }} AS paid
  ON
      claim.policy_id = paid.policy_id
  AND
      claim.snapshot_date = paid.snapshot_date
  WHERE
    claim.policy_id IS NOT NULL
)
SELECT
  *,
  {{target.schema}}.calculate_claim_reserve_amount (
    claim_invoice_amount,
    claim_status,
    claim_is_continuation,
    product_excess,
    policy_co_pay_percent,
    product_cover_for_type,
    policy_paid_amount_for_type
  ) AS claim_reserve_amount,
  {{target.schema}}.calculate_claim_incurred_amount(
    {{target.schema}}.calculate_claim_reserve_amount (
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
  ) AS claim_incurred_amount
FROM
  claim