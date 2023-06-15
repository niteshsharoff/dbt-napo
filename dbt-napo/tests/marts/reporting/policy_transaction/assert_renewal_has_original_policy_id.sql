/* 
  GIVEN 
    a table of policy transactions
    
  WHEN 
    we have a renewal transaction
  
  THEN 
    we expect the original_quote_source to be populated
*/
select transaction_at
  , transaction_type
  , policy.reference_number
  , policy.quote_source
  , policy.original_quote_source
from {{ref('fct_policy_transaction')}}
where transaction_type = 'Renewal' and policy.original_quote_source is null