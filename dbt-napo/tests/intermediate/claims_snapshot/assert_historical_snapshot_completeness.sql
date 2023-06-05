/* 
  GIVEN 
    consecutive daily snapshots of all claims
    
  WHEN 
    a claim is created in any past snapshots
  
  THEN 
    the claim id should be present in all snapshots following the first snapshot it's reported in
*/
select *
  , date_diff(snapshot_date, prev_snapshot_date, day) as diff
from (
  select policy_reference_number
    , claim_id
    , snapshot_date
    , min(snapshot_date) over(partition by claim_id) as first_snapshot_date
    , lag(snapshot_date) over(partition by claim_id order by snapshot_date) as prev_snapshot_date
  from {{ ref("int_underwriter__claim_snapshot") }}
  order by snapshot_date
)
where date_diff(snapshot_date, prev_snapshot_date, day) != 1
  and snapshot_date != first_snapshot_date