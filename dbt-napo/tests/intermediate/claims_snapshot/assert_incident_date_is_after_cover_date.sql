/* 
  GIVEN 
    a snapshot of all claims
    
  WHEN 
    a claim_sub_type is 'Illness' or 'Accident' and not null
    and the claim_status is 'accepted'
  
  THEN 
    the claim incident date should occur after the policy's cover start date
*/
select *
from (
  select policy_reference_number
    , claim_master_claim_id
    , claim_id
    , claim_status
    , claim_sub_type
    , claim_incident_date
    , policy_start_date
    , policy_illness_cover_start_date
    , policy_accident_cover_start_date
    , policy_is_renewal
    , case 
        when claim_sub_type = 'Illness' then claim_incident_date >= policy_illness_cover_start_date
        when claim_sub_type = 'Accident' then claim_incident_date >= policy_accident_cover_start_date
        else false
    end as is_covered
    , snapshot_at
  from {{ ref("int_underwriter__policy_claim_snapshot") }}
  where cast(snapshot_at as date) = parse_date('%Y-%m-%d', '{{run_started_at.date()}}')
)
where is_covered is false
  and claim_status = 'accepted'
  and policy_is_renewal = false