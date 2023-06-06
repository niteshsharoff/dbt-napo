{{ config(
    materialized='table',
    pre_hook=[
      "{{declare_underwriter_udfs()}}"
    ]
) }}
WITH
  snapshot_details AS (
    SELECT
      TIMESTAMP(DATETIME(2023, 5, 1, 0, 0, 0)) AS snapshot_at
  )
SELECT
  policy.*,
  snapshot_details.*,
  {{target.schema}}.calculate_policy_exposure(
    policy_start_date,
    EXTRACT(DATE FROM snapshot_at)
  ) AS policy_exposure,
  {{target.schema}}.calculate_policy_development_month(
    policy_start_date,
    policy_end_date,
    EXTRACT(DATE FROM snapshot_at)
  ) AS policy_development_month,
  {{target.schema}}.calculate_gross_earned_premium(
    policy_annual_retail_price,
    policy_start_date,
    policy_cancel_date, 
    EXTRACT(DATE FROM snapshot_at)
  ) AS policy_gross_earned_premium,
  policy_claim.* EXCEPT (policy_id)
FROM
  {{ ref ("int_underwriter__policy_history") }} AS policy,
  snapshot_details
LEFT JOIN (
  SELECT
    policy_id,
    COALESCE(SUM(claim_incurred_amount), 0) AS policy_incurred_amount,
    COUNTIF(claim_cover_type = 'vet_fee_cover' AND claim_cover_sub_type = 'Accident') AS policy_n_vet_fee_accident_claims,
    COUNTIF(claim_cover_type = 'vet_fee_cover' AND claim_cover_sub_type = 'Illness') AS policy_n_vet_fee_illness_claims,
    SUM(IF(claim_cover_type = 'vet_fee_cover' AND claim_cover_sub_type = 'Accident', claim_paid_amount, 0)) AS policy_vet_fee_accident_paid_amount,
    SUM(IF(claim_cover_type = 'vet_fee_cover' AND claim_cover_sub_type = 'Illness', claim_paid_amount, 0)) AS policy_vet_fee_illness_paid_amount
  FROM
    {{ ref("int_underwriter__claim_snapshot") }}
  GROUP BY
    policy_id
) AS policy_claim ON
  policy_claim.policy_id = policy.policy_id
WHERE
  effective_from <= snapshot_at
  AND snapshot_at < effective_to