/* 
  GIVEN
    a snapshot of all claims
    
  WHEN
    a claim is closed
  
  THEN 
    the reserve amount of the claim should be 0
*/
select *
from (
  select policy_reference_number
    , claim_master_claim_id
    , claim_id
    , claim_status
    , claim_reserve_amount
    , case
        when claim_status = 'accepted' or claim_status = 'declined' then true
        else false
    end as is_closed
    , snapshot_at
  from {{ ref("int_underwriter__policy_claim_snapshot") }}
  where cast(snapshot_at as date) = parse_date('%Y-%m-%d', '{{run_started_at.date()}}')
)
where is_closed = true
  and claim_reserve_amount != 0