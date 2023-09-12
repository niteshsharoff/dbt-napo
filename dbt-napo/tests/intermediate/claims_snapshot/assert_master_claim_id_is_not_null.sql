{{ config(severity="warn") }}
/*
  GIVEN
    a snapshot of all claims

  WHEN
    a continuation claim is created

  THEN
    the claim should always have a master claim id
*/
select policy_reference_number, claim_master_claim_id, claim_id, snapshot_date
from{{ ref("int_underwriter__claim_snapshot") }}
where
    snapshot_date = parse_date('%Y-%m-%d', '{{run_started_at.date()}}')
    and claim_master_claim_id is null
