/* 
  GIVEN 
    a table of policy transactions
    
  WHEN 
    we get transaction types and timestamps associated to a policy
  
  THEN 
    there should be no duplicate records
*/
-- select transaction_date
--   , transaction_type
--   , policy_reference_number
--   , count(*) as row_count
-- from {{ref('policy_transaction_fixtures')}}
-- group by transaction_date
--   , transaction_type
--   , policy_reference_number
-- having row_count > 1