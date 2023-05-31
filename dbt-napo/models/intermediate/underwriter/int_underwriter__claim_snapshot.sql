WITH
  snapshot_details AS (
    SELECT
      PARSE_DATE('%Y-%m-%d', '{{run_started_at.date()}}') AS snapshot_date
  )
SELECT
  claim.*
FROM
  {{ref('int_underwriter__claim_snapshots')}} AS claim,
  snapshot_details
WHERE
  claim.snapshot_date = snapshot_details.snapshot_date