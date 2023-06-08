/* 
  GIVEN 
    a table of policy transactions
    
  WHEN 
    we sum up the premium difference for each policy
  
  THEN 
    we expect the cumulative premium position to be >= 0
*/
select policy.reference_number
  , sum(underwriter.premium_difference) as cumulative_premium_position
from {{ref('fct_policy_transaction')}}
group by policy.reference_number
having cumulative_premium_position < 0.0