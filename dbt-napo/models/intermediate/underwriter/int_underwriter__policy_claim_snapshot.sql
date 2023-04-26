SELECT
  claim.id AS claim_id,
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
  {{target.schema}}.calculate_claim_reserve_amount (
    claim.invoice_amount,
    claim.status,
    claim.is_continuation,
    policy.product_excess,
    policy.policy_co_pay_percent
  ) AS claim_reserve_amount,
  claim.type AS claim_cover_type,
  claim.sub_type AS claim_sub_type,
  claim.paid_amount AS claim_paid_amount,
  claim.first_invoice_date AS claim_first_invoice_date,
  claim.decline_reason AS claim_decline_reason,
  claim.source AS claim_source,
  policy.*
FROM
  {{ ref ("int_claim_snapshot") }} AS claim
  LEFT JOIN {{ ref ("int_underwriter__policy_snapshot") }} AS policy ON policy.policy_id = claim.policy_id
    and claim.snapshot_date = cast(policy.snapshot_at as date)
