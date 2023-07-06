/*
  GIVEN
    a snapshot of all claims and policies

  WHEN
    a claim is received

  THEN
    the claim should have a policy associated with it
    and the received date should be between the policy start and end dates
*/
select
    policy_reference_number,
    claim_master_claim_id,
    claim_id,
    claim_received_date,
    policy_start_date,
    policy_end_date,
    snapshot_date
from {{ ref("int_underwriter__claim_snapshot") }} c
where
    snapshot_date = parse_date('%Y-%m-%d', '{{run_started_at.date()}}')
    and (
        policy_uuid is null
        or (
            claim_received_date < policy_start_date
            and claim_received_date > policy_end_date
        )
    )
