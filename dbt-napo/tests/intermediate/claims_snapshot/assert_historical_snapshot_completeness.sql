/* 
  GIVEN 
    consecutive daily snapshots of all claims
    
  WHEN 
    a claim is created in any past snapshots
  
  THEN 
    the claim id should be present in all snapshots following the first snapshot it's reported in
*/
select *
  , date_diff(snapshot_at, prev_snapshot_at, day) as diff
from (
  select policy_reference_number
    , claim_id
    , cast(snapshot_at as date) as snapshot_at
    , min(cast(snapshot_at as date)) over(partition by claim_id) as first_snapshot_at
    , lag(cast(snapshot_at as date)) over(partition by claim_id order by snapshot_at) as prev_snapshot_at
  from {{ ref("int_underwriter__policy_claim_snapshot") }}
  order by snapshot_at
)
where date_diff(snapshot_at, prev_snapshot_at, day) != 1
  and snapshot_at != first_snapshot_at