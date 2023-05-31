/* 
  GIVEN 
    a snapshot of all claims
    
  WHEN 
    a claim is declined
  
  THEN 
    the decline reason should not be null
*/
select policy_reference_number
  , claim_master_claim_id
  , claim_id
  , claim_status
  , claim_decline_reason
  , snapshot_date
from {{ ref("int_underwriter__claim_snapshot") }}
where snapshot_date = parse_date('%Y-%m-%d', '{{run_started_at.date()}}')
  and claim_status = 'declined'
  and claim_decline_reason is null