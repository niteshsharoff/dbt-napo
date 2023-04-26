/* 
  GIVEN 
    a snapshot of all policies
    
  WHEN 
    a policy is created
  
  THEN 
    the customer's age should be greater than 18 years old
*/
select policy_reference_number
  , policy_start_date
  , customer_date_of_birth
  , date_diff(policy_start_date, customer_date_of_birth, year) as age
  , snapshot_at
from {{ ref("int_underwriter__policy_snapshot") }}
where cast(snapshot_at as date) = parse_date('%Y-%m-%d', '{{run_started_at.date()}}')
  and date_diff(policy_start_date, customer_date_of_birth, year) < 18
