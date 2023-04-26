SELECT
  id,
  CAST(policy_id AS INT64) AS policy_id,
  master_claim_id,
  EXTRACT(DATE FROM TIMESTAMP_MILLIS(CAST(date_received AS INT64))) AS date_received,
  EXTRACT(DATE FROM TIMESTAMP_MILLIS(CAST(onset_date AS INT64))) AS onset_date,
  type,
  sub_type,
  CAST(paid_amount AS FLOAT64) AS paid_amount,
  EXTRACT(DATE FROM TIMESTAMP_MILLIS(CAST(first_invoice_date AS INT64))) AS first_invoice_date,
  decline_reason,
  CAST(invoice_amount AS FLOAT64) AS invoice_amount,
  source,
  snapshot_date,
  CASE
    WHEN status IN ('accepted', 'accepted claim') THEN 'accepted'
    WHEN status IN ('declined', 'declined claim') THEN 'declined'
    ELSE status
  END AS status,
  CASE
    WHEN is_continuation = 'Yes' THEN TRUE
    WHEN is_continuation = 'No' THEN FALSE
    ELSE NULL
  END AS is_continuation,
FROM
  raw.claims_snapshot AS claim
WHERE
  snapshot_date = PARSE_DATE('%Y-%m-%d', '{{run_started_at.date()}}')
