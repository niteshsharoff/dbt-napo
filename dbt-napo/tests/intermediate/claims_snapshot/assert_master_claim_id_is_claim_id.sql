/* 
  GIVEN 
    a snapshot of all claims
    
  WHEN 
    a master claim is created
  
  THEN 
    we expect the master claim ID to also be a claim ID
*/
with distinct_claim_ids as (
  select distinct(claim_id) as claim_id
  from {{ ref("int_underwriter__policy_claim_snapshot") }}
  where cast(snapshot_at as date) = parse_date('%Y-%m-%d', '{{run_started_at.date()}}')
)
select policy_reference_number
  , claim_master_claim_id
  , claim_id
  , snapshot_at
from {{ ref("int_underwriter__policy_claim_snapshot") }}
where cast(snapshot_at as date) = parse_date('%Y-%m-%d', '{{run_started_at.date()}}')
  and trim(claim_master_claim_id) not in (select trim(claim_id) from distinct_claim_ids)
