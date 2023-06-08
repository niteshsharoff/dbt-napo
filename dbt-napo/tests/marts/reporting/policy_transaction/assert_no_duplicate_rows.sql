/* 
  GIVEN 
    a table of policy transactions
    
  WHEN 
    we get transaction types and timestamps associated to a policy
  
  THEN 
    there should be no duplicate records
*/
select transaction_at
  , transaction_type
  , policy.reference_number
  , count(*) as row_count
from {{ref('fct_policy_transaction')}}
group by transaction_at
  , transaction_type
  , policy.reference_number
having row_count > 1