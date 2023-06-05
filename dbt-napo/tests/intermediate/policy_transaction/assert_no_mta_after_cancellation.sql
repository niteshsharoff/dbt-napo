/* 
  GIVEN 
    a list of transactions
    
  WHEN 
    we have an MTA record following a cancellation
  
  THEN 
    the record's transaction type should be 'Cancellation' or 'Cancellation MTA'
*/
select *
from (
  select policy.reference_number
    , transaction_at
    , transaction_type
    , lag(transaction_type) over(partition by policy.reference_number order by transaction_at) as prev_transaction_type
  from {{ref('int_underwriter__policy_transaction')}}
)
where transaction_type = 'MTA' 
  and (
    prev_transaction_type = 'NTU'
    or prev_transaction_type = 'Cancellation'
    or prev_transaction_type = 'Cancellation MTA'
  )