/* 
  GIVEN 
    a list of transactions
    
  WHEN 
    we have an MTA record following a cancellation
  
  THEN 
    the record's transaction type should be 'Cancel'
*/
-- select *
-- from (
--   select policy_reference_number
--     , transaction_date
--     , transaction_type
--     , lag(transaction_type) over(partition by policy_reference_number order by transaction_date) as prev_transaction_type
--   from {{ref('policy_transaction_fixtures')}}
-- )
-- where transaction_type = 'MTA' and prev_transaction_type = 'Cancel'