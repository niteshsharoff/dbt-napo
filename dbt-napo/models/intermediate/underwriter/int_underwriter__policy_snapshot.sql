WITH
  snapshot_details AS (
    SELECT
      PARSE_TIMESTAMP('%Y-%m-%d', '{{run_started_at.date()}}') AS snapshot_at
  )
SELECT
  *
FROM
  {{ref("int_underwriter__policy_history")}},
  snapshot_details
WHERE
  effective_from <= snapshot_at
  AND snapshot_at < effective_to
