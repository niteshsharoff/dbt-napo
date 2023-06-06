/* 
  GIVEN 
    a table of policy transactions
    
  WHEN 
    we have any transaction type
  
  THEN 
    they should all be attributable to a new policy or renewal
*/
select policy.reference_number
from {{ref('int_underwriter__policy_transaction')}}
where policy.reference_number not in (
  select distinct(policy.reference_number) as reference_number
  from {{ref('int_underwriter__policy_transaction')}}
  where transaction_type = 'New Policy' or transaction_type = 'Renewal'
)