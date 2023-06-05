{{ config(materialized='table') }}

SELECT
  *
FROM
  {{ref('int_underwriter__claim_snapshots')}}
WHERE
  snapshot_date = DATE(2023, 5, 9)
AND
  claim_received_date < DATE(2023, 5, 1)