/*
  GIVEN
    a snapshot of all policies

  WHEN
    we have a policy record

  THEN
    we expect the policy's annual premium price to be positive
*/
select
    policy_reference_number,
    policy_annual_premium_price,
    policy_annual_retail_price,
    snapshot_at
from {{ ref("int_underwriter__policy_snapshot") }}
where
    cast(snapshot_at as date) = parse_date('%Y-%m-%d', '{{run_started_at.date()}}')
    and (policy_annual_premium_price < 0 or policy_annual_retail_price < 0)
